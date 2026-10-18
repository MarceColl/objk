use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use metrics::{counter, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
use serde::Serialize;
use slatedb::bytes::{Buf, Bytes};
use slatedb::config::{DurabilityLevel, ScanOptions, Settings};
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::db_cache::{DbCache, SplitCache};
use slatedb::object_store::ObjectStore;
use slatedb::{Db, Error, IterationOrder, WriteBatch};
use slatedb_common::clock::{DefaultSystemClock, SystemClock};
use std::future::ready;
use std::io::IsTerminal;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::Mutex;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

// Key layout. Names are '/'-terminated when followed by more; ints are fixed-width BE
// (no separator needed) so byte order == numeric order.
//   m/{topic}/{p}{off}        -> payload
//   g/{group}/{topic}/{p}     -> u64 committed
//   c/{group}/{topic}/{p}     -> u64 claim expiry (unix ms) | member

/// Names are embedded in keys, so '/' would make `foo` a prefix of `foo/bar`.
fn name(s: &str) -> Result<(), QErr> {
    ensure(!s.is_empty() && !s.contains('/'), QErr::Bad("bad name"))
}

fn ensure(ok: bool, e: QErr) -> Result<(), QErr> {
    ok.then_some(()).ok_or(e)
}

fn partition_key(kind: &[u8], topic: &str, p: u32) -> Vec<u8> {
    [kind, topic.as_bytes(), b"/", &p.to_be_bytes()].concat()
}

fn msg_key(topic: &str, p: u32, off: u64) -> Vec<u8> {
    [partition_key(b"m/", topic, p), off.to_be_bytes().to_vec()].concat()
}

fn msg_off(key: &[u8]) -> u64 {
    u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap())
}

fn group_key(kind: &[u8], group: &str, topic: &str, p: u32) -> Vec<u8> {
    [kind, group.as_bytes(), b"/", &partition_key(b"", topic, p)].concat()
}

/// Splits `{name}/rest` (keys only ever hold valid names).
fn split_name(k: &[u8]) -> (&str, &[u8]) {
    let i = k.iter().position(|&b| b == b'/').unwrap();
    (std::str::from_utf8(&k[..i]).unwrap(), &k[i + 1..])
}

/// Parses `{topic}/{p}...` back out of a key with its kind prefix stripped.
fn split_partition(k: &[u8]) -> (&str, u32, &[u8]) {
    let (topic, rest) = split_name(k);
    (topic, u32::from_be_bytes(rest[..4].try_into().unwrap()), &rest[4..])
}

/// slateDB's defaults assume a machine of its own: up to 1 GiB of unflushed writes, a 640 MiB
/// cache and four concurrent compactions, which OOM-killed a 512 MiB pod under batch load.
/// These (with the cache in `Store::open`) keep objk around 0.65 GiB peak under burst writes
/// (1.1 GiB with the defaults); when unflushed writes hit the cap, producers wait.
fn bounded() -> Settings {
    let mut settings = Settings { max_unflushed_bytes: 128 << 20, ..Default::default() };
    if let Some(c) = settings.compactor_options.as_mut() {
        c.max_concurrent_compactions = 1;
        if let Some(w) = c.worker.as_mut() {
            w.max_concurrent_compactions = 1;
        }
    }
    settings
}

/// How long a claim lives without being renewed.
const CLAIM_TTL_MS: u64 = 30_000;

// Metrics. Lag isn't exported: Grafana derives it as
//   objk_head_offset - on(topic, partition) group_right objk_committed_offset
fn labels(topic: &str, p: u32) -> [(&'static str, String); 2] {
    [("topic", topic.to_string()), ("partition", p.to_string())]
}

#[derive(Serialize, Debug, PartialEq)]
struct Partition {
    topic: String,
    partition: u32,
    head: u64,
}

#[derive(Serialize, Debug, PartialEq)]
struct Group {
    group: String,
    topic: String,
    partition: u32,
    committed: u64,
    holder: Option<String>,
}

#[derive(Debug)]
enum QErr {
    Db(Error),
    /// Client error, returned as 400.
    Bad(&'static str),
    /// Partition claimed by another member, returned as 409.
    Taken,
}

impl From<Error> for QErr {
    fn from(e: Error) -> Self {
        QErr::Db(e)
    }
}

struct Store {
    db: Db,
    /// Serializes read-modify-writes so offsets are assigned and written in order.
    write: Mutex<()>,
    /// The same clock slateDB runs on; the simulation swaps in virtual time.
    clock: Arc<dyn SystemClock>,
}

impl Store {
    /// `seed` makes slateDB's randomness (object ids, ...) deterministic; only the simulation sets it,
    /// and must vary it per open.
    async fn open(
        path: &str,
        os: Arc<dyn ObjectStore>,
        clock: Arc<dyn SystemClock>,
        seed: Option<u64>,
        settings: Settings,
    ) -> Result<Self, Error> {
        let cache = |mib: u64| Some(Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions { max_capacity: mib << 20, ..Default::default() })) as Arc<dyn DbCache>);
        let cache = SplitCache::new().with_block_cache(cache(128)).with_meta_cache(cache(32)).build();
        let b = Db::builder(path, os).with_system_clock(clock.clone()).with_settings(settings).with_db_cache(Arc::new(cache), 0);
        let db = match seed { Some(seed) => b.with_seed(seed), None => b }.build().await?;
        Ok(Store { db, write: Mutex::new(()), clock })
    }

    fn now(&self) -> u64 {
        self.clock.now().timestamp_millis() as u64
    }

    /// Appends `msgs` atomically at contiguous offsets and returns the first one.
    /// Topics and partitions are implicit: they exist once written to.
    async fn produce(&self, topic: &str, p: u32, msgs: &[Bytes]) -> Result<u64, QErr> {
        name(topic)?;
        ensure(!msgs.is_empty(), QErr::Bad("empty batch"))?;
        let l = labels(topic, p);
        let (first, handle) = {
            let _g = self.write.lock().await;
            let first = self.head(topic, p).await?;
            let mut batch = WriteBatch::new();
            for (off, m) in (first..).zip(msgs) {
                batch.put(msg_key(topic, p, off), m);
            }
            let handle = self.db.write(batch).await?;
            gauge!("objk_head_offset", &l).set((first + msgs.len() as u64) as f64);
            (first, handle)
        };
        handle.await_durable().await?;
        counter!("objk_produced_messages_total", &l).increment(msgs.len() as u64);
        counter!("objk_produced_bytes_total", &l).increment(msgs.iter().map(|m| m.len() as u64).sum());
        Ok(first)
    }

    /// Only durable messages: anything else could vanish on crash and its offset be reused.
    async fn fetch(&self, topic: &str, p: u32, from: u64, max: usize) -> Result<Vec<(u64, Bytes)>, QErr> {
        name(topic)?;
        let opts = ScanOptions {
            durability_filter: DurabilityLevel::Remote,
            read_ahead_bytes: 256 * 1024,
            cache_blocks: true,
            max_fetch_tasks: 4,
            ..Default::default()
        };
        let mut it = self.db.scan_with_options(msg_key(topic, p, from)..=msg_key(topic, p, u64::MAX), &opts).await?;
        let mut out = Vec::new();
        while out.len() < max {
            let Some(kv) = it.next().await? else { break };
            out.push((msg_off(&kv.key), kv.value));
        }
        counter!("objk_fetched_messages_total", &labels(topic, p)).increment(out.len() as u64);
        Ok(out)
    }

    /// `off` is the next offset to read. Only the claim holder may commit, and it's monotonic,
    /// so a stale consumer can't move it back.
    async fn commit(&self, group: &str, topic: &str, p: u32, member: &str, off: u64, now: u64) -> Result<(), QErr> {
        let handle = {
            let _g = self.write.lock().await;
            ensure(self.holder(group, topic, p, now).await?.as_deref() == Some(member), QErr::Taken)?;
            let off = off.max(self.committed(group, topic, p).await?);
            let l = [("group", group.to_string()), ("topic", topic.to_string()), ("partition", p.to_string())];
            gauge!("objk_committed_offset", &l).set(off as f64);
            self.db.put(group_key(b"g/", group, topic, p), off.to_be_bytes()).await?
        };
        Ok(handle.await_durable().await?)
    }

    async fn committed(&self, group: &str, topic: &str, p: u32) -> Result<u64, QErr> {
        name(group)?;
        name(topic)?;
        let v = self.db.get(group_key(b"g/", group, topic, p)).await?;
        Ok(v.map_or(0, |v| u64::from_be_bytes(v[..].try_into().unwrap())))
    }

    /// Takes the claim if free, expired or already ours, and (re)sets it to expire in CLAIM_TTL_MS.
    /// Holders renew by calling it again.
    async fn claim(&self, group: &str, topic: &str, p: u32, member: &str, now: u64) -> Result<(), QErr> {
        ensure(!member.is_empty(), QErr::Bad("missing member"))?;
        let handle = {
            let _g = self.write.lock().await;
            ensure(self.holder(group, topic, p, now).await?.is_none_or(|h| h == member), QErr::Taken)?;
            let v = [&(now + CLAIM_TTL_MS).to_be_bytes()[..], member.as_bytes()].concat();
            self.db.put(group_key(b"c/", group, topic, p), v).await?
        };
        Ok(handle.await_durable().await?)
    }

    /// Drops the claim if we hold it; otherwise a no-op.
    async fn release(&self, group: &str, topic: &str, p: u32, member: &str, now: u64) -> Result<(), QErr> {
        let _g = self.write.lock().await;
        if self.holder(group, topic, p, now).await?.as_deref() == Some(member) {
            self.db.delete(group_key(b"c/", group, topic, p)).await?.await_durable().await?;
        }
        Ok(())
    }

    /// Member holding an unexpired claim, if any.
    async fn holder(&self, group: &str, topic: &str, p: u32, now: u64) -> Result<Option<String>, QErr> {
        name(group)?;
        name(topic)?;
        let v = self.db.get(group_key(b"c/", group, topic, p)).await?;
        let live = v.filter(|v| u64::from_be_bytes(v[..8].try_into().unwrap()) > now);
        Ok(live.map(|v| String::from_utf8_lossy(&v[8..]).into_owned()))
    }

    /// Every partition with its head, by skip-scan over the message keys: one seek per
    /// partition, nothing extra to keep in sync. `m0` bounds the scan: '0' sorts right after '/'.
    async fn partitions(&self) -> Result<Vec<Partition>, QErr> {
        let mut out = Vec::new();
        let mut from = b"m/".to_vec();
        loop {
            let Some(kv) = self.db.scan(from..b"m0".to_vec()).await?.next().await? else { break };
            let (topic, p, _) = split_partition(&kv.key[2..]);
            out.push(Partition { topic: topic.into(), partition: p, head: self.head(topic, p).await? });
            from = match p.checked_add(1) {
                Some(next) => partition_key(b"m/", topic, next),
                None => [b"m/", topic.as_bytes(), b"0"].concat(),
            };
        }
        Ok(out)
    }

    /// Every committed group offset, with the live claim holder. Groups that only ever
    /// claimed have nothing committed and aren't listed.
    async fn groups(&self, now: u64) -> Result<Vec<Group>, QErr> {
        let mut it = self.db.scan(b"g/".to_vec()..b"g0".to_vec()).await?;
        let mut out = Vec::new();
        while let Some(kv) = it.next().await? {
            let (group, rest) = split_name(&kv.key[2..]);
            let (topic, p, _) = split_partition(rest);
            let committed = u64::from_be_bytes(kv.value[..].try_into().unwrap());
            let holder = self.holder(group, topic, p, now).await?;
            out.push(Group { group: group.into(), topic: topic.into(), partition: p, committed, holder });
        }
        Ok(out)
    }

    /// Next offset to be written. Derived from the last message key, so no counter to keep in sync.
    async fn head(&self, topic: &str, p: u32) -> Result<u64, QErr> {
        name(topic)?;
        let opts = ScanOptions { order: IterationOrder::Descending, ..Default::default() };
        let mut it = self.db.scan_with_options(msg_key(topic, p, 0)..=msg_key(topic, p, u64::MAX), &opts).await?;
        Ok(it.next().await?.map_or(0, |kv| msg_off(&kv.key) + 1))
    }
}

// HTTP. Offsets are plain-text numbers. Formats are picked by header:
//   produce Content-Type: application/json  ["msg", ...] (UTF-8 strings)
//                         anything else     (u32 len | bytes)*
//   fetch   Accept:       application/json  [{"offset": n, "value": "msg"}, ...]
//                         anything else     (u64 off | u32 len | bytes)*

const FETCH_MAX: usize = 1000;

type S = State<Arc<Store>>;

impl IntoResponse for QErr {
    fn into_response(self) -> Response {
        match self {
            QErr::Bad(why) => (StatusCode::BAD_REQUEST, why).into_response(),
            QErr::Taken => (StatusCode::CONFLICT, "claimed by another member").into_response(),
            QErr::Db(e) => {
                error!(%e, "db");
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    }
}

fn header<'a>(h: &'a HeaderMap, k: impl header::AsHeaderName) -> &'a str {
    h.get(k).and_then(|v| v.to_str().ok()).unwrap_or("")
}

fn unframe(mut b: Bytes) -> Result<Vec<Bytes>, QErr> {
    let mut out = Vec::new();
    while b.has_remaining() {
        let n = b.try_get_u32().map_err(|_| QErr::Bad("bad batch framing"))? as usize;
        ensure(b.len() >= n, QErr::Bad("bad batch framing"))?;
        out.push(b.split_to(n));
    }
    Ok(out)
}

async fn produce(State(s): S, Path((t, p)): Path<(String, u32)>, h: HeaderMap, body: Bytes) -> Result<String, QErr> {
    let ct = header(&h, header::CONTENT_TYPE);
    let msgs = if ct.starts_with("application/json") {
        let v: Vec<String> = serde_json::from_slice(&body).map_err(|_| QErr::Bad("expected JSON array of strings"))?;
        v.into_iter().map(Bytes::from).collect()
    } else {
        unframe(body)?
    };
    Ok(s.produce(&t, p, &msgs).await?.to_string())
}

#[derive(Serialize)]
struct Msg {
    offset: u64,
    value: String,
}

async fn fetch(State(s): S, Path((t, p, from)): Path<(String, u32, u64)>, h: HeaderMap) -> Result<Response, QErr> {
    let msgs = s.fetch(&t, p, from, FETCH_MAX).await?;
    if !header(&h, header::ACCEPT).contains("application/json") {
        let bin = msgs.iter().flat_map(|(o, b)| [&o.to_be_bytes()[..], &(b.len() as u32).to_be_bytes(), b].concat());
        return Ok(bin.collect::<Vec<u8>>().into_response());
    }
    let json = msgs.into_iter().map(|(offset, b)| {
        let value = String::from_utf8(b.into()).map_err(|_| QErr::Bad("non-UTF-8 message, fetch as binary"))?;
        Ok(Msg { offset, value })
    });
    Ok(Json(json.collect::<Result<Vec<_>, QErr>>()?).into_response())
}

async fn head(State(s): S, Path((t, p)): Path<(String, u32)>) -> Result<String, QErr> {
    Ok(s.head(&t, p).await?.to_string())
}

// Claims and commits identify the consumer by this header.
const MEMBER: &str = "x-objk-member";

async fn commit(State(s): S, Path((g, t, p, off)): Path<(String, String, u32, u64)>, h: HeaderMap) -> Result<(), QErr> {
    s.commit(&g, &t, p, header(&h, MEMBER), off, s.now()).await
}

async fn claim(State(s): S, Path((g, t, p)): Path<(String, String, u32)>, h: HeaderMap) -> Result<(), QErr> {
    s.claim(&g, &t, p, header(&h, MEMBER), s.now()).await
}

async fn release(State(s): S, Path((g, t, p)): Path<(String, String, u32)>, h: HeaderMap) -> Result<(), QErr> {
    s.release(&g, &t, p, header(&h, MEMBER), s.now()).await
}

async fn committed(State(s): S, Path((g, t, p)): Path<(String, String, u32)>) -> Result<String, QErr> {
    Ok(s.committed(&g, &t, p).await?.to_string())
}

async fn partitions(State(s): S) -> Result<Json<Vec<Partition>>, QErr> {
    Ok(Json(s.partitions().await?))
}

async fn groups(State(s): S) -> Result<Json<Vec<Group>>, QErr> {
    Ok(Json(s.groups(s.now()).await?))
}

fn router(s: Arc<Store>) -> Router {
    Router::new()
        // Read-only browser over the JSON listings and the binary fetch.
        .route("/", get(|| ready(Html(include_str!("ui.html")))))
        .route("/t", get(partitions))
        .route("/g", get(groups))
        .route("/t/{t}/{p}", post(produce).get(head))
        .route("/t/{t}/{p}/{from}", get(fetch))
        .route("/g/{g}/{t}/{p}", get(committed))
        .route("/g/{g}/{t}/{p}/claim", post(claim).delete(release))
        .route("/g/{g}/{t}/{p}/{off}", axum::routing::put(commit))
        .with_state(s)
}

/// Served on :3000. Config is env only:
///   OBJK_STORE  object store root URL, no path (default `file:///`), e.g. `s3://bucket`
///   OBJK_PATH   DB location inside the store (default `data`)
///   AWS_*       store options, passed through by slateDB (AWS_ENDPOINT, AWS_ALLOW_HTTP, ...)
///   RUST_LOG    log filter (default `info`)
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(filter).with_ansi(std::io::stdout().is_terminal()).init();
    let env = |k, d: &str| std::env::var(k).unwrap_or(d.into());
    let (url, path) = (env("OBJK_STORE", "file:///"), env("OBJK_PATH", "data"));
    let clock = Arc::new(DefaultSystemClock::new());
    let store = Arc::new(Store::open(&path, Db::resolve_object_store(&url)?, clock, None, bounded()).await?);
    let metrics = PrometheusBuilder::new().install_recorder()?;
    let app = router(store.clone()).route("/metrics", get(move || ready(metrics.render())));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    info!(store = url, path, addr = %listener.local_addr()?, "listening");
    axum::serve(listener, app).with_graceful_shutdown(shutdown()).await?;
    info!("draining done, closing db");
    store.db.close().await?;
    info!("closed");
    Ok(())
}

/// SIGINT or SIGTERM. Also makes `docker stop` work: as PID 1 we'd otherwise ignore SIGTERM.
async fn shutdown() {
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
    info!("shutdown signal, draining requests");
}

#[cfg(test)]
mod sim;

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    async fn store() -> Store {
        Store::open("t", Arc::new(InMemory::new()), Arc::new(DefaultSystemClock::new()), None, bounded()).await.unwrap()
    }

    fn msgs(ms: &[&'static str]) -> Vec<Bytes> {
        ms.iter().map(|m| Bytes::from(*m)).collect()
    }

    #[tokio::test]
    async fn produce_fetch() {
        let s = store().await;
        assert!(matches!(s.produce("a/b", 0, &msgs(&["x"])).await, Err(QErr::Bad(_))));
        assert!(matches!(s.fetch("", 0, 0, 1).await, Err(QErr::Bad(_))));
        assert!(s.fetch("nope", 0, 0, 10).await.unwrap().is_empty());
        assert_eq!(s.head("a", 0).await.unwrap(), 0);

        assert_eq!(s.produce("a", 0, &msgs(&["x"])).await.unwrap(), 0);
        assert_eq!(s.produce("a", 0, &msgs(&["y", "z"])).await.unwrap(), 1);
        assert!(matches!(s.produce("a", 0, &[]).await, Err(QErr::Bad(_))));
        assert_eq!(s.produce("a", 1, &msgs(&["other"])).await.unwrap(), 0);
        assert_eq!(s.head("a", 0).await.unwrap(), 3);

        let got = s.fetch("a", 0, 1, 10).await.unwrap();
        assert_eq!(got, vec![(1, Bytes::from("y")), (2, Bytes::from("z"))]);
        assert_eq!(s.fetch("a", 0, 0, 1).await.unwrap().len(), 1);
        assert!(s.fetch("a", 0, 3, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn listings() {
        let s = store().await;
        assert!(s.partitions().await.unwrap().is_empty());
        assert!(s.groups(0).await.unwrap().is_empty());

        // "a" is a byte prefix of "ab", and partition u32::MAX is the skip-scan's edge.
        for (t, p, n) in [("ab", 0, 1), ("a", 1, 2), ("a", 0, 3), ("a", u32::MAX, 1), ("b", 7, 1)] {
            s.produce(t, p, &msgs(&["x"; 3][..n])).await.unwrap();
        }
        let got: Vec<_> = s.partitions().await.unwrap().into_iter().map(|p| (p.topic, p.partition, p.head)).collect();
        let want = [("a", 0, 3), ("a", 1, 2), ("a", u32::MAX, 1), ("ab", 0, 1), ("b", 7, 1)];
        assert_eq!(got, want.map(|(t, p, h)| (t.to_string(), p, h)));

        s.claim("g", "a", 1, "m", 0).await.unwrap();
        s.commit("g", "a", 1, "m", 2, 0).await.unwrap();
        s.claim("only-claims", "a", 0, "m", 0).await.unwrap();
        let g = |holder: Option<&str>| Group {
            group: "g".into(),
            topic: "a".into(),
            partition: 1,
            committed: 2,
            holder: holder.map(Into::into),
        };
        assert_eq!(s.groups(0).await.unwrap(), vec![g(Some("m"))]);
        assert_eq!(s.groups(CLAIM_TTL_MS).await.unwrap(), vec![g(None)]);
    }

    #[test]
    fn unframe_batch() {
        let b = Bytes::from([&2u32.to_be_bytes()[..], b"hi", &0u32.to_be_bytes()].concat());
        assert_eq!(unframe(b).unwrap(), msgs(&["hi", ""]));
        assert!(unframe(Bytes::new()).unwrap().is_empty());
        assert!(matches!(unframe(Bytes::from_static(&[0, 0])), Err(QErr::Bad(_))));
        assert!(matches!(unframe(Bytes::from_static(&[0, 0, 0, 5, b'x'])), Err(QErr::Bad(_))));
    }

    #[tokio::test]
    async fn commit_monotonic() {
        let s = store().await;
        s.claim("g", "a", 0, "m", 0).await.unwrap();
        assert_eq!(s.committed("g", "a", 0).await.unwrap(), 0);
        s.commit("g", "a", 0, "m", 5, 0).await.unwrap();
        s.commit("g", "a", 0, "m", 3, 0).await.unwrap();
        assert_eq!(s.committed("g", "a", 0).await.unwrap(), 5);
        assert_eq!(s.committed("h", "a", 0).await.unwrap(), 0);
        assert!(matches!(s.commit("g/x", "a", 0, "m", 1, 0).await, Err(QErr::Bad(_))));
    }

    #[tokio::test]
    async fn claims() {
        let s = store().await;
        let ttl = CLAIM_TTL_MS;
        assert!(matches!(s.claim("g", "a", 0, "", 0).await, Err(QErr::Bad(_))));
        assert!(matches!(s.commit("g", "a", 0, "m", 1, 0).await, Err(QErr::Taken)));

        s.claim("g", "a", 0, "m", 0).await.unwrap();
        s.claim("g", "a", 0, "m", 0).await.unwrap();
        s.claim("g", "a", 1, "n", 0).await.unwrap();
        s.claim("h", "a", 0, "n", 0).await.unwrap();
        assert!(matches!(s.claim("g", "a", 0, "n", ttl - 1).await, Err(QErr::Taken)));
        assert!(matches!(s.commit("g", "a", 0, "n", 1, 0).await, Err(QErr::Taken)));

        s.claim("g", "a", 0, "m", ttl - 1).await.unwrap();
        assert!(matches!(s.claim("g", "a", 0, "n", ttl).await, Err(QErr::Taken)));
        s.claim("g", "a", 0, "n", 2 * ttl).await.unwrap();
        assert!(matches!(s.commit("g", "a", 0, "m", 1, 2 * ttl).await, Err(QErr::Taken)));

        s.release("g", "a", 0, "m", 2 * ttl).await.unwrap();
        assert!(matches!(s.claim("g", "a", 0, "m", 2 * ttl).await, Err(QErr::Taken)));
        s.release("g", "a", 0, "n", 2 * ttl).await.unwrap();
        s.claim("g", "a", 0, "m", 2 * ttl).await.unwrap();
    }
}
