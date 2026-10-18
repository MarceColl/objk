use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use metrics::{counter, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
use serde::Serialize;
use slatedb::bytes::{Buf, Bytes};
use slatedb::config::{DurabilityLevel, ScanOptions};
use slatedb::{Db, Error, IterationOrder, WriteBatch};
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

/// Names are embedded in keys, so '/' would make `foo` a prefix of `foo/bar`.
fn name(s: &str) -> Result<(), QErr> {
    ensure(!s.is_empty() && !s.contains('/'), "bad name")
}

fn ensure(ok: bool, why: &'static str) -> Result<(), QErr> {
    ok.then_some(()).ok_or(QErr::Bad(why))
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

fn group_key(group: &str, topic: &str, p: u32) -> Vec<u8> {
    [b"g/", group.as_bytes(), b"/", &partition_key(b"", topic, p)].concat()
}

// Metrics. Lag isn't exported: Grafana derives it as
//   objk_head_offset - on(topic, partition) group_right objk_committed_offset
fn labels(topic: &str, p: u32) -> [(&'static str, String); 2] {
    [("topic", topic.to_string()), ("partition", p.to_string())]
}

#[derive(Debug)]
enum QErr {
    Db(Error),
    /// Client error, returned as 400.
    Bad(&'static str),
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
}

impl Store {
    fn new(db: Db) -> Self {
        Store { db, write: Mutex::new(()) }
    }

    /// Appends `msgs` atomically at contiguous offsets and returns the first one.
    /// Topics and partitions are implicit: they exist once written to.
    async fn produce(&self, topic: &str, p: u32, msgs: &[Bytes]) -> Result<u64, QErr> {
        name(topic)?;
        ensure(!msgs.is_empty(), "empty batch")?;
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
        let opts = ScanOptions { durability_filter: DurabilityLevel::Remote, ..Default::default() };
        let mut it = self.db.scan_with_options(msg_key(topic, p, from)..=msg_key(topic, p, u64::MAX), &opts).await?;
        let mut out = Vec::new();
        while out.len() < max {
            let Some(kv) = it.next().await? else { break };
            out.push((msg_off(&kv.key), kv.value));
        }
        counter!("objk_fetched_messages_total", &labels(topic, p)).increment(out.len() as u64);
        Ok(out)
    }

    /// `off` is the next offset to read. Monotonic: a stale consumer can't move it back.
    async fn commit(&self, group: &str, topic: &str, p: u32, off: u64) -> Result<(), QErr> {
        let handle = {
            let _g = self.write.lock().await;
            let off = off.max(self.committed(group, topic, p).await?);
            let l = [("group", group.to_string()), ("topic", topic.to_string()), ("partition", p.to_string())];
            gauge!("objk_committed_offset", &l).set(off as f64);
            self.db.put(group_key(group, topic, p), off.to_be_bytes()).await?
        };
        Ok(handle.await_durable().await?)
    }

    async fn committed(&self, group: &str, topic: &str, p: u32) -> Result<u64, QErr> {
        name(group)?;
        name(topic)?;
        let v = self.db.get(group_key(group, topic, p)).await?;
        Ok(v.map_or(0, |v| u64::from_be_bytes(v[..].try_into().unwrap())))
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
//   produce Content-Type: application/json         ["msg", ...] (UTF-8 strings)
//                         application/x-objk-batch (u32 len | bytes)*
//                         anything else            the whole body is one message
//   fetch   Accept:       application/json         [{"offset": n, "value": "msg"}, ...]
//                         anything else            (u64 off | u32 len | bytes)*

const FETCH_MAX: usize = 1000;

type S = State<Arc<Store>>;

impl IntoResponse for QErr {
    fn into_response(self) -> Response {
        match self {
            QErr::Bad(why) => (StatusCode::BAD_REQUEST, why).into_response(),
            QErr::Db(e) => {
                error!(%e, "db");
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    }
}

fn header<'a>(h: &'a HeaderMap, k: header::HeaderName) -> &'a str {
    h.get(k).and_then(|v| v.to_str().ok()).unwrap_or("")
}

fn unframe(mut b: Bytes) -> Result<Vec<Bytes>, QErr> {
    let mut out = Vec::new();
    while b.has_remaining() {
        let n = b.try_get_u32().map_err(|_| QErr::Bad("bad batch framing"))? as usize;
        ensure(b.len() >= n, "bad batch framing")?;
        out.push(b.split_to(n));
    }
    Ok(out)
}

async fn produce(State(s): S, Path((t, p)): Path<(String, u32)>, h: HeaderMap, body: Bytes) -> Result<String, QErr> {
    let ct = header(&h, header::CONTENT_TYPE);
    let msgs = if ct.starts_with("application/json") {
        let v: Vec<String> = serde_json::from_slice(&body).map_err(|_| QErr::Bad("expected JSON array of strings"))?;
        v.into_iter().map(Bytes::from).collect()
    } else if ct.starts_with("application/x-objk-batch") {
        unframe(body)?
    } else {
        vec![body]
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

async fn commit(State(s): S, Path((g, t, p, off)): Path<(String, String, u32, u64)>) -> Result<(), QErr> {
    s.commit(&g, &t, p, off).await
}

async fn committed(State(s): S, Path((g, t, p)): Path<(String, String, u32)>) -> Result<String, QErr> {
    Ok(s.committed(&g, &t, p).await?.to_string())
}

fn router(s: Arc<Store>) -> Router {
    Router::new()
        .route("/t/{t}/{p}", post(produce).get(head))
        .route("/t/{t}/{p}/{from}", get(fetch))
        .route("/g/{g}/{t}/{p}", get(committed))
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
    let db = Db::open(path.clone(), Db::resolve_object_store(&url)?).await?;
    let store = Arc::new(Store::new(db));
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
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    async fn store() -> Store {
        Store::new(Db::open("t", Arc::new(InMemory::new())).await.unwrap())
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
        assert_eq!(s.committed("g", "a", 0).await.unwrap(), 0);
        s.commit("g", "a", 0, 5).await.unwrap();
        s.commit("g", "a", 0, 3).await.unwrap();
        assert_eq!(s.committed("g", "a", 0).await.unwrap(), 5);
        assert_eq!(s.committed("h", "a", 0).await.unwrap(), 0);
        assert!(matches!(s.commit("g/x", "a", 0, 1).await, Err(QErr::Bad(_))));
    }
}
