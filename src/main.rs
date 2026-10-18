use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use slatedb::bytes::Bytes;
use slatedb::config::{DurabilityLevel, ScanOptions};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::{Db, Error, IterationOrder};
use std::sync::Arc;
use tokio::sync::Mutex;

// Key layout. Names are '/'-terminated when followed by more; ints are fixed-width BE
// (no separator needed) so byte order == numeric order.
//   m/{topic}/{p}{off}        -> payload
//   g/{group}/{topic}/{p}     -> u64 committed

/// Names are embedded in keys, so '/' would make `foo` a prefix of `foo/bar`.
fn name(s: &str) -> Result<(), QErr> {
    (!s.is_empty() && !s.contains('/')).then_some(()).ok_or(QErr::BadName)
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

#[derive(Debug)]
enum QErr {
    Db(Error),
    BadName,
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

    /// Topics and partitions are implicit: they exist once written to.
    async fn produce(&self, topic: &str, p: u32, bytes: &[u8]) -> Result<u64, QErr> {
        name(topic)?;
        let (off, handle) = {
            let _g = self.write.lock().await;
            let off = self.last(topic, p).await?.map_or(0, |o| o + 1);
            (off, self.db.put(msg_key(topic, p, off), bytes).await?)
        };
        handle.await_durable().await?;
        Ok(off)
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
        Ok(out)
    }

    /// `off` is the next offset to read. Monotonic: a stale consumer can't move it back.
    async fn commit(&self, group: &str, topic: &str, p: u32, off: u64) -> Result<(), QErr> {
        let handle = {
            let _g = self.write.lock().await;
            let off = off.max(self.committed(group, topic, p).await?);
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

    /// Head is derived from the last message key, so no counter to keep in sync.
    async fn last(&self, topic: &str, p: u32) -> Result<Option<u64>, QErr> {
        let opts = ScanOptions { order: IterationOrder::Descending, ..Default::default() };
        let mut it = self.db.scan_with_options(msg_key(topic, p, 0)..=msg_key(topic, p, u64::MAX), &opts).await?;
        Ok(it.next().await?.map(|kv| msg_off(&kv.key)))
    }
}

// HTTP. Offsets are plain-text numbers; fetch returns (u64 off | u32 len | bytes)*.

const FETCH_MAX: usize = 1000;

type S = State<Arc<Store>>;

impl IntoResponse for QErr {
    fn into_response(self) -> Response {
        match self {
            QErr::BadName => (StatusCode::BAD_REQUEST, "bad name").into_response(),
            QErr::Db(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        }
    }
}

async fn produce(State(s): S, Path((t, p)): Path<(String, u32)>, body: Bytes) -> Result<String, QErr> {
    Ok(s.produce(&t, p, &body).await?.to_string())
}

async fn fetch(State(s): S, Path((t, p, from)): Path<(String, u32, u64)>) -> Result<Vec<u8>, QErr> {
    let msgs = s.fetch(&t, p, from, FETCH_MAX).await?;
    Ok(msgs.iter().flat_map(|(o, b)| [&o.to_be_bytes()[..], &(b.len() as u32).to_be_bytes(), b].concat()).collect())
}

async fn commit(State(s): S, Path((g, t, p, off)): Path<(String, String, u32, u64)>) -> Result<(), QErr> {
    s.commit(&g, &t, p, off).await
}

async fn committed(State(s): S, Path((g, t, p)): Path<(String, String, u32)>) -> Result<String, QErr> {
    Ok(s.committed(&g, &t, p).await?.to_string())
}

fn router(s: Arc<Store>) -> Router {
    Router::new()
        .route("/t/{t}/{p}", post(produce))
        .route("/t/{t}/{p}/{from}", get(fetch))
        .route("/g/{g}/{t}/{p}", get(committed))
        .route("/g/{g}/{t}/{p}/{off}", axum::routing::put(commit))
        .with_state(s)
}

/// `objk [dir]`: data lives under `dir` (default `data`), served on :3000.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args().nth(1).unwrap_or("data".into());
    std::fs::create_dir_all(&dir)?;
    let db = Db::open("objk", Arc::new(LocalFileSystem::new_with_prefix(dir)?)).await?;
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, router(Arc::new(Store::new(db)))).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    async fn store() -> Store {
        Store::new(Db::open("t", Arc::new(InMemory::new())).await.unwrap())
    }

    #[tokio::test]
    async fn produce_fetch() {
        let s = store().await;
        assert!(matches!(s.produce("a/b", 0, b"x").await, Err(QErr::BadName)));
        assert!(matches!(s.fetch("", 0, 0, 1).await, Err(QErr::BadName)));
        assert!(s.fetch("nope", 0, 0, 10).await.unwrap().is_empty());

        for (i, m) in [b"x", b"y", b"z"].iter().enumerate() {
            assert_eq!(s.produce("a", 0, *m).await.unwrap(), i as u64);
        }
        assert_eq!(s.produce("a", 1, b"other").await.unwrap(), 0);

        let got = s.fetch("a", 0, 1, 10).await.unwrap();
        assert_eq!(got, vec![(1, Bytes::from("y")), (2, Bytes::from("z"))]);
        assert_eq!(s.fetch("a", 0, 0, 1).await.unwrap().len(), 1);
        assert!(s.fetch("a", 0, 3, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn commit_monotonic() {
        let s = store().await;
        assert_eq!(s.committed("g", "a", 0).await.unwrap(), 0);
        s.commit("g", "a", 0, 5).await.unwrap();
        s.commit("g", "a", 0, 3).await.unwrap();
        assert_eq!(s.committed("g", "a", 0).await.unwrap(), 5);
        assert_eq!(s.committed("h", "a", 0).await.unwrap(), 0);
        assert!(matches!(s.commit("g/x", "a", 0, 1).await, Err(QErr::BadName)));
    }
}
