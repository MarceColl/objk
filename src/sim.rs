//! Deterministic simulation (turmoil). The real server runs over a simulated network with
//! producers and consumers as clients, while the driver crashes the server and partitions
//! the network. Time is virtual and every run replays exactly from its seed:
//!   OBJK_SIM_SEED=<n>   rerun one seed
//!   OBJK_SIM_RUNS=<n>   number of seeds to run (default 4)
//!   OBJK_SIM_FIRST=<n>  first seed (default 0)
//!
//! Clients log what they observed; afterwards the surviving object store is reopened and
//! checked against those observations (see `check`).

use super::*;
use chrono::{DateTime, TimeDelta, Utc};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Deserialize;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::throttle::{ThrottleConfig, ThrottledStore};
use slatedb_common::clock::SystemClockTicker;
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

const PARTITIONS: u32 = 2;
const PRODUCERS: usize = 4;
const CONSUMERS: usize = 3;
/// Producers and faults stop here; consumers then drain until `END`.
const FAULTS_UNTIL: Duration = Duration::from_secs(90);
const END: Duration = Duration::from_secs(200);
const LATENCY: Duration = Duration::from_millis(50);
const TOPIC: &str = "t";
const GROUP: &str = "g";

/// Virtual time shared by all hosts, so slateDB and claims see one clock across restarts.
#[derive(Debug)]
struct SimClock;

impl SystemClock for SimClock {
    fn now(&self) -> DateTime<Utc> {
        DateTime::UNIX_EPOCH + TimeDelta::from_std(turmoil::sim_elapsed().unwrap_or_default()).unwrap()
    }

    fn sleep<'a>(&'a self, d: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(tokio::time::sleep(d))
    }

    fn ticker<'a>(&'a self, d: Duration) -> SystemClockTicker<'a> {
        SystemClockTicker::new(self, d)
    }
}

fn now() -> u64 {
    turmoil::sim_elapsed().unwrap().as_millis() as u64
}

struct SimListener(turmoil::net::TcpListener);

impl axum::serve::Listener for SimListener {
    type Io = turmoil::net::TcpStream;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            if let Ok(c) = self.0.accept().await {
                return c;
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.0.local_addr()
    }
}

/// One request on a fresh connection. `None` means the outcome is unknown (network fault or timeout).
async fn http(method: &str, path: &str, member: &str, json: Option<String>) -> Option<(u16, Bytes)> {
    let go = async {
        let io = TokioIo::new(turmoil::net::TcpStream::connect(("objk", 3000)).await.ok()?);
        let (mut tx, conn) = hyper::client::conn::http1::handshake(io).await.ok()?;
        tokio::spawn(conn);
        let req = hyper::Request::builder()
            .method(method)
            .uri(path)
            .header(header::HOST, "objk")
            .header(header::ACCEPT, "application/json")
            .header(header::CONTENT_TYPE, "application/json")
            .header(MEMBER, member)
            .body(Full::new(Bytes::from(json.unwrap_or_default())))
            .unwrap();
        let res = tx.send_request(req).await.ok()?;
        let status = res.status().as_u16();
        Some((status, res.into_body().collect().await.ok()?.to_bytes()))
    };
    tokio::time::timeout(Duration::from_secs(2), go).await.ok().flatten()
}

/// `http` that only cares about successful responses.
async fn ok(method: &str, path: &str, member: &str, json: Option<String>) -> Option<Bytes> {
    http(method, path, member, json).await.filter(|(s, _)| *s == 200).map(|(_, b)| b)
}

fn num(b: &Bytes) -> u64 {
    std::str::from_utf8(b).unwrap().parse().unwrap()
}

/// A request by `member` on partition `p`, sent and answered at these virtual times (ms).
#[derive(Debug, Clone)]
struct Span {
    member: String,
    p: u32,
    sent: u64,
    recv: u64,
}

/// Everything clients observed.
#[derive(Default, Debug)]
struct Log {
    /// Produce acked: (p, offset, value).
    acked: Vec<(u32, u64, String)>,
    /// Message handed to a consumer: (p, offset, value).
    processed: Vec<(u32, u64, String)>,
    claims: Vec<Span>,
    /// Release attempts, whatever their outcome.
    releases: Vec<Span>,
    commits: Vec<(Span, u64)>,
    /// Committed-offset reads.
    reads: Vec<(Span, u64)>,
    bounces: usize,
}

type Shared = Rc<RefCell<Log>>;

fn rng(seed: u64, who: u64) -> StdRng {
    StdRng::seed_from_u64(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ who)
}

async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await
}

async fn producer(id: usize, seed: u64, log: Shared) -> turmoil::Result {
    let mut rng = rng(seed, 100 + id as u64);
    let mut n = 0;
    while turmoil::sim_elapsed().unwrap() < FAULTS_UNTIL {
        let p = rng.random_range(0..PARTITIONS);
        let msgs: Vec<String> = (0..rng.random_range(1..=3)).map(|_| (n += 1, format!("p{id}-{n}")).1).collect();
        if let Some(b) = ok("POST", &format!("/t/{TOPIC}/{p}"), "", Some(serde_json::to_string(&msgs)?)).await {
            let first = num(&b);
            log.borrow_mut().acked.extend(msgs.into_iter().zip(first..).map(|(m, off)| (p, off, m)));
        }
        sleep_ms(rng.random_range(10..300)).await;
    }
    Ok(())
}

#[derive(Deserialize)]
struct MsgIn {
    offset: u64,
    value: String,
}

/// The README consumer loop, plus faults of its own: sometimes it vanishes without releasing
/// (a crash) and comes back as a new member; sometimes it releases everything (a deploy);
/// sometimes it stalls past the claim TTL mid-batch (a GC pause) and then commits anyway;
/// sometimes it re-sends a stale commit (a late retry).
async fn consumer(id: usize, seed: u64, log: Shared) -> turmoil::Result {
    let mut rng = rng(seed, 200 + id as u64);
    let mut life = 0;
    while turmoil::sim_elapsed().unwrap() < END {
        let member = format!("c{id}-{life}");
        let faulty = turmoil::sim_elapsed().unwrap() < FAULTS_UNTIL;
        if faulty && rng.random_bool(0.02) {
            sleep_ms(rng.random_range(1_000..40_000)).await;
            life += 1;
            continue;
        }
        if faulty && rng.random_bool(0.02) {
            for p in 0..PARTITIONS {
                let sent = now();
                http("DELETE", &format!("/g/{GROUP}/{TOPIC}/{p}/claim"), &member, None).await;
                log.borrow_mut().releases.push(Span { member: member.clone(), p, sent, recv: now() });
            }
        }
        for p in 0..PARTITIONS {
            let span = |sent| Span { member: member.clone(), p, sent, recv: now() };
            let sent = now();
            if ok("POST", &format!("/g/{GROUP}/{TOPIC}/{p}/claim"), &member, None).await.is_none() {
                continue;
            }
            log.borrow_mut().claims.push(span(sent));
            let sent = now();
            let Some(b) = ok("GET", &format!("/g/{GROUP}/{TOPIC}/{p}"), &member, None).await else { continue };
            let from = num(&b);
            log.borrow_mut().reads.push((span(sent), from));
            let Some(b) = ok("GET", &format!("/t/{TOPIC}/{p}/{from}"), &member, None).await else { continue };
            let msgs: Vec<MsgIn> = serde_json::from_slice(&b)?;
            let Some(next) = msgs.last().map(|m| m.offset + 1) else { continue };
            log.borrow_mut().processed.extend(msgs.into_iter().map(|m| (p, m.offset, m.value)));
            if faulty && rng.random_bool(0.03) {
                sleep_ms(CLAIM_TTL_MS + rng.random_range(0..15_000)).await;
            }
            let sent = now();
            if ok("PUT", &format!("/g/{GROUP}/{TOPIC}/{p}/{next}"), &member, None).await.is_some() {
                log.borrow_mut().commits.push((span(sent), next));
                // A late retry of the previous commit, landing after this newer one.
                if faulty && rng.random_bool(0.1) {
                    http("PUT", &format!("/g/{GROUP}/{TOPIC}/{p}/{from}"), &member, None).await;
                }
            }
        }
        sleep_ms(rng.random_range(50..500)).await;
    }
    Ok(())
}

/// Flush memtables to L0 SSTs (and so compact) constantly instead of every 64MB, so reads go
/// through the object store and crashes land mid-flush and mid-compaction.
fn tiny_memtables() -> Settings {
    Settings { l0_sst_size_bytes: 4 * 1024, ..bounded() }
}

#[derive(Clone, Copy)]
enum Link {
    Cut,
    Held,
    Slow,
}

fn run(seed: u64) -> (Log, Arc<dyn ObjectStore>) {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(END + Duration::from_secs(60))
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(LATENCY)
        .rng_seed(seed)
        .build();

    // Object storage outlives the server, like S3 would. Latency makes requests suspend
    // mid-operation, so concurrent ones actually interleave.
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ms = Duration::from_millis;
    let config = ThrottleConfig {
        wait_get_per_call: ms(5),
        wait_put_per_call: ms(15),
        wait_list_per_call: ms(5),
        wait_delete_per_call: ms(5),
        ..Default::default()
    };
    let os: Arc<dyn ObjectStore> = Arc::new(ThrottledStore::new(mem.clone(), config));
    let opens = Rc::new(Cell::new(0u64));
    let (host_os, host_opens) = (os.clone(), opens.clone());
    sim.host("objk", move || {
        let os = host_os.clone();
        host_opens.set(host_opens.get() + 1);
        // slateDB derives object ids from its seed, so each open needs a fresh one.
        let db_seed = seed.wrapping_mul(1_000_003).wrapping_add(host_opens.get());
        async move {
            let store = Arc::new(Store::open("objk", os, Arc::new(SimClock), Some(db_seed), tiny_memtables()).await?);
            let listener = turmoil::net::TcpListener::bind("0.0.0.0:3000").await?;
            axum::serve(SimListener(listener), router(store)).await?;
            Ok(())
        }
    });

    let log: Shared = Default::default();
    let mut clients = Vec::new();
    for i in 0..PRODUCERS {
        clients.push(format!("producer-{i}"));
        sim.client(clients.last().unwrap().as_str(), producer(i, seed, log.clone()));
    }
    for i in 0..CONSUMERS {
        clients.push(format!("consumer-{i}"));
        sim.client(clients.last().unwrap().as_str(), consumer(i, seed, log.clone()));
    }

    // Faults, at a rate per 1ms tick. A link is cut (messages dropped), held (delayed until
    // released) or slow (up to 3s per message, past the client's 2s timeout).
    let mut rng = rng(seed, 0);
    let mut links: Vec<(&str, Link)> = Vec::new();
    let heal = |sim: &mut turmoil::Sim, (c, l): (&str, Link)| match l {
        Link::Cut => sim.repair("objk", c),
        Link::Held => sim.release("objk", c),
        Link::Slow => sim.set_link_max_message_latency("objk", c, LATENCY),
    };
    while !sim.step().unwrap_or_else(|e| panic!("seed {seed}: host failed: {e}")) {
        if sim.elapsed() >= FAULTS_UNTIL {
            links.drain(..).for_each(|l| heal(&mut sim, l));
            continue;
        }
        if rng.random_bool(1.0 / 1_500.0) {
            sim.bounce("objk");
            log.borrow_mut().bounces += 1;
        }
        if rng.random_bool(1.0 / 3_000.0) {
            let c = clients[rng.random_range(0..clients.len())].as_str();
            let l = [Link::Cut, Link::Held, Link::Slow][rng.random_range(0..3)];
            match l {
                Link::Cut => sim.partition("objk", c),
                Link::Held => sim.hold("objk", c),
                Link::Slow => sim.set_link_max_message_latency("objk", c, Duration::from_secs(3)),
            }
            links.push((c, l));
        }
        // Mean ~4s: longer than the client timeout, so held requests land after the client moved on.
        if !links.is_empty() && rng.random_bool(1.0 / 4_000.0) {
            let l = links.remove(rng.random_range(0..links.len()));
            heal(&mut sim, l);
        }
    }
    drop(sim);
    (Rc::try_unwrap(log).unwrap().into_inner(), mem)
}

/// Reopens the surviving store and checks it against what clients observed.
fn check(seed: u64, log: &Log, os: Arc<dyn ObjectStore>) {
    let fail = |what: String| -> ! { panic!("seed {seed}: {what}") };
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let (parts, committed) = rt.block_on(async {
        let s = Store::open("objk", os, Arc::new(DefaultSystemClock::new()), None, bounded()).await.unwrap();
        let mut parts = Vec::new();
        let mut committed = Vec::new();
        for p in 0..PARTITIONS {
            parts.push(s.fetch(TOPIC, p, 0, usize::MAX).await.unwrap());
            committed.push(s.committed(GROUP, TOPIC, p).await.unwrap());
        }
        (parts, committed)
    });
    let at = |p: u32, off: u64| parts[p as usize].get(off as usize).map(|(_, v)| v.as_ref());

    // Offsets in a partition are exactly 0, 1, 2, ...
    for (p, msgs) in parts.iter().enumerate() {
        if let Some((i, (off, _))) = msgs.iter().enumerate().find(|(i, (off, _))| *off != *i as u64) {
            fail(format!("partition {p} has offset {off} at position {i}"));
        }
    }
    // No lost acks, no offset reuse: every acked message is where its ack said.
    for (p, off, v) in &log.acked {
        if at(*p, *off) != Some(v.as_bytes()) {
            fail(format!("acked {v:?} at {p}/{off}, store has {:?}", at(*p, *off)));
        }
    }
    // Consumers only ever saw what the log holds.
    for (p, off, v) in &log.processed {
        if at(*p, *off) != Some(v.as_bytes()) {
            fail(format!("consumer saw {v:?} at {p}/{off}, store has {:?}", at(*p, *off)));
        }
    }
    // At-least-once: once faults stop and consumers drain, every acked message was processed.
    let seen: HashSet<(u32, u64)> = log.processed.iter().map(|(p, off, _)| (*p, *off)).collect();
    if let Some((p, off, v)) = log.acked.iter().find(|(p, off, _)| !seen.contains(&(*p, *off))) {
        fail(format!("acked {v:?} at {p}/{off} was never processed"));
    }
    // Acked commits are durable.
    for (k, off) in &log.commits {
        if committed[k.p as usize] < *off {
            fail(format!("commit {off} on {} acked, store has {}", k.p, committed[k.p as usize]));
        }
    }
    // Committed offsets never go back: anything that finished before a read started (another
    // read, or an acked commit) saw no more than it.
    let done = log.reads.iter().chain(&log.commits);
    for (a, x) in done {
        if let Some((b, y)) = log.reads.iter().find(|(b, y)| b.p == a.p && a.recv < b.sent && x > y) {
            fail(format!("committed on {} was {x} by {}, then read {y} at {}", a.p, a.recv, b.sent));
        }
    }
    // Claims are exclusive: no one else commits while a claim is provably live and unreleased.
    for c in &log.claims {
        for (k, _) in &log.commits {
            let live = c.recv <= k.sent && k.recv < c.sent + CLAIM_TTL_MS;
            let released = log.releases.iter().any(|r| r.member == c.member && r.p == c.p && r.sent >= c.sent && r.sent <= k.recv);
            if k.p == c.p && k.member != c.member && live && !released {
                fail(format!("{} committed on {} while {} held it: {c:?} {k:?}", k.member, k.p, c.member));
            }
        }
    }
}

#[test]
fn simulation() {
    let seeds: Vec<u64> = match std::env::var("OBJK_SIM_SEED") {
        Ok(s) => vec![s.parse().unwrap()],
        Err(_) => {
            let env = |k, d| std::env::var(k).map_or(d, |n| n.parse().unwrap());
            let first = env("OBJK_SIM_FIRST", 0);
            (first..first + env("OBJK_SIM_RUNS", 4)).collect()
        }
    };
    for seed in seeds {
        let (log, os) = run(seed);
        // Same seed, same fingerprint: that's what makes a failing seed replayable.
        let mut h = std::hash::DefaultHasher::new();
        std::hash::Hash::hash(&format!("{log:?}"), &mut h);
        eprintln!(
            "seed {seed}: {} acked, {} processed, {} commits, {} bounces, fingerprint {:016x}",
            log.acked.len(),
            log.processed.len(),
            log.commits.len(),
            log.bounces,
            std::hash::Hasher::finish(&h)
        );
        check(seed, &log, os);
    }
}
