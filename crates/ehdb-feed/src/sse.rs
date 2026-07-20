//! **L1 T3 — the gateway/SPA live feed over Server-Sent Events.**
//!
//! Streams the change-feed to browser/SPA clients as `text/event-stream`, the
//! same one-hop delivery path as the worker feed — this is what the gateway's
//! SSE `ConnectionHub` carries to the SPA. SSE is a natural fit: its built-in
//! **`id:` / `Last-Event-ID`** reconnect maps **exactly** onto the L0
//! [`ChangeFeed`] cursor — each event's `id` is the record's sort key, and a
//! browser `EventSource` that drops and reconnects sends `Last-Event-ID`, from
//! which the server resumes with zero missed / zero duplicate events.
//!
//! **T3 posture:** shadow — an additive live feed alongside the existing
//! gateway path; NATS stays authoritative. Wire shape:
//!
//! ```text
//! GET /feed?shard=0&cursor=0        (or Last-Event-ID: <sort_key> on reconnect)
//! -> Content-Type: text/event-stream
//!    id: 1
//!    data: {"global_sequence":1,...}
//!
//!    id: 2
//!    data: {...}
//! ```
//!
//! Cursor precedence: `Last-Event-ID` header (a reconnect) > `cursor` query
//! param > `0` (from the beginning).

use std::io;
use std::sync::{Arc, Mutex};

use ehdb_l0::{ChangeFeed, Dataset, L0Engine};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::{io_err, FeedWriter};

/// Parse `(shard, cursor)` from an SSE request head.
pub(crate) fn parse_request(head: &str) -> (u32, u64) {
    let mut shard = 0u32;
    let mut cursor = 0u64;
    let mut last_event_id: Option<u64> = None;

    let mut lines = head.split("\r\n");
    if let Some(req_line) = lines.next() {
        // e.g. `GET /feed?shard=0&cursor=5 HTTP/1.1`
        if let Some(path) = req_line.split_whitespace().nth(1) {
            if let Some(query) = path.split('?').nth(1) {
                for kv in query.split('&') {
                    let mut it = kv.splitn(2, '=');
                    match (it.next(), it.next()) {
                        (Some("shard"), Some(v)) => shard = v.parse().unwrap_or(0),
                        (Some("cursor"), Some(v)) => cursor = v.parse().unwrap_or(0),
                        _ => {}
                    }
                }
            }
        }
    }
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("last-event-id") {
                last_event_id = value.trim().parse().ok();
            }
        }
    }
    // A reconnect's Last-Event-ID wins over the initial query cursor.
    (shard, last_event_id.unwrap_or(cursor))
}

async fn read_head(sock: &mut tokio::net::TcpStream) -> io::Result<String> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        let n = sock.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() > 8192 {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Serve the SSE live feed on `listener` off `writer`. Each connection follows
/// its requested shard from its cursor, streaming events until the client
/// disconnects. Runs until the listener errors; spawn it as a task.
pub async fn serve_sse<D>(listener: TcpListener, writer: Arc<FeedWriter<D>>) -> io::Result<()>
where
    D: Dataset,
    D::Record: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    loop {
        let (mut sock, _peer) = listener.accept().await?;
        sock.set_nodelay(true)?;
        let (engine, rx) = writer.subscriber_handle();
        tokio::spawn(async move {
            let head = match read_head(&mut sock).await {
                Ok(h) => h,
                Err(_) => return,
            };
            let (shard, cursor) = parse_request(&head);
            let _ = stream_events::<D>(engine, rx, sock, shard, cursor).await;
        });
    }
}

async fn stream_events<D>(
    engine: Arc<Mutex<L0Engine<D>>>,
    mut rx: watch::Receiver<u64>,
    mut sock: tokio::net::TcpStream,
    shard: u32,
    cursor: u64,
) -> io::Result<()>
where
    D: Dataset,
    D::Record: Serialize + DeserializeOwned + Clone,
{
    sock.write_all(
        b"HTTP/1.1 200 OK\r\n\
          Content-Type: text/event-stream\r\n\
          Cache-Control: no-cache\r\n\
          Connection: keep-alive\r\n\r\n",
    )
    .await?;
    sock.flush().await?;

    let mut feed = ChangeFeed::new(shard, cursor);
    loop {
        let batch = {
            let engine = engine.lock().unwrap();
            feed.poll(&engine).map_err(io_err)?
        };
        if !batch.is_empty() {
            let mut frame = String::new();
            for rec in &batch {
                let id = D::sort_key(rec);
                let data = serde_json::to_string(rec).map_err(io_err)?;
                // JSON is single-line, so one `data:` line per event.
                frame.push_str(&format!("id: {id}\ndata: {data}\n\n"));
            }
            sock.write_all(frame.as_bytes()).await?;
            sock.flush().await?;
            continue;
        }
        if rx.changed().await.is_err() {
            return Ok(()); // writer dropped
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_request;

    #[test]
    fn query_params_and_last_event_id() {
        let (s, c) = parse_request("GET /feed?shard=3&cursor=42 HTTP/1.1\r\nHost: x\r\n\r\n");
        assert_eq!((s, c), (3, 42));

        // Last-Event-ID (browser reconnect) overrides the query cursor.
        let (s, c) = parse_request(
            "GET /feed?shard=2&cursor=10 HTTP/1.1\r\nLast-Event-ID: 99\r\nHost: x\r\n\r\n",
        );
        assert_eq!((s, c), (2, 99));

        // Defaults.
        let (s, c) = parse_request("GET /feed HTTP/1.1\r\n\r\n");
        assert_eq!((s, c), (0, 0));
    }
}
