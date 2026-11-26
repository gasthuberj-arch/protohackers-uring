//! Adversarial compliance and stress test suite for Protohackers #0 (Smoke Test).
//!
//! # Running
//!
//! By default, running the test target executes all 6 adversarial scenarios
//! against the built-in `std::net::TcpListener` reference echo server so the
//! harness self-verifies while `p0-smoke` remains a `todo!()` stub:
//!
//! ```sh
//! cargo test -p harness --test p0_smoke
//! ```
//!
//! Once `crates/p0-smoke` is implemented, build it and run the `#[ignore]`d
//! binary test suite against it:
//!
//! ```sh
//! cargo build -p p0-smoke && cargo test -p harness --test p0_smoke -- --ignored
//! ```
//!
//! To test any alternate binary that implements the `--bind <addr>` /
//! `LISTENING <actual-addr>` readiness contract, set `PROTOHACKERS_BIN`:
//!
//! ```sh
//! PROTOHACKERS_BIN=/path/to/bin cargo test -p harness --test p0_smoke -- --ignored
//! ```

use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Explicit read/write/connect timeout applied to every client `TcpStream` so a
/// broken server fails promptly instead of hanging indefinitely.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

/// Connects a client `TcpStream` to `addr` with explicit read and write
/// timeouts configured.
fn connect(addr: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&addr, CLIENT_TIMEOUT)
        .unwrap_or_else(|err| panic!("failed to connect to {addr}: {err}"));
    stream
        .set_read_timeout(Some(CLIENT_TIMEOUT))
        .expect("failed to set client read timeout");
    stream
        .set_write_timeout(Some(CLIENT_TIMEOUT))
        .expect("failed to set client write timeout");
    stream
}

/// Deterministic `std`-only PRNG (xorshift64) for generating reproducible binary
/// payloads without external dependencies.
struct Xorshift64 {
    state: u64,
}

impl Xorshift64 {
    fn new(seed: u64) -> Self {
        assert!(seed != 0, "xorshift64 seed must be non-zero");
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn fill_bytes(&mut self, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            let word = self.next_u64().to_le_bytes();
            let take = (len - out.len()).min(word.len());
            out.extend_from_slice(&word[..take]);
        }
        out
    }
}

/// Asserts byte-for-byte equality with concise diagnostics on large buffers.
fn assert_bytes_eq(received: &[u8], expected: &[u8], context: &str) {
    assert_eq!(
        received.len(),
        expected.len(),
        "{context}: length mismatch (received {} bytes, expected {} bytes)",
        received.len(),
        expected.len()
    );
    if let Some((offset, (&got, &want))) = received
        .iter()
        .zip(expected.iter())
        .enumerate()
        .find(|(_, (a, b))| a != b)
    {
        panic!(
            "{context}: first byte mismatch at offset {offset} (got 0x{got:02x}, expected 0x{want:02x})"
        );
    }
}

/// Scenario 1: Basic UTF-8 echo + TCP half-close + sequential connection reuse.
fn basic_utf8_and_half_close(addr: SocketAddr) {
    // First connection: write multiple UTF-8 chunks without waiting for echo,
    // immediately half-close, then drain to clean EOF.
    {
        let mut stream = connect(addr);
        let chunks = [
            "hello protohackers\n",
            "Grüße aus io_uring 🦀\n",
            "trailing chunk without newline — αβγδ",
        ];
        let mut expected = Vec::new();
        for chunk in chunks {
            let bytes = chunk.as_bytes();
            stream
                .write_all(bytes)
                .expect("conn 1: failed to write UTF-8 chunk");
            expected.extend_from_slice(bytes);
        }

        stream
            .shutdown(Shutdown::Write)
            .expect("conn 1: shutdown(Shutdown::Write) failed");

        let mut received = Vec::new();
        let n = stream
            .read_to_end(&mut received)
            .expect("conn 1: expected clean EOF without RST");
        assert_eq!(n, expected.len(), "conn 1: unexpected EOF byte count");
        assert_bytes_eq(&received, &expected, "conn 1 UTF-8 echo");
    }

    // Second sequential connection to verify the server continues accepting and
    // echoing cleanly after a prior half-closed connection completes.
    {
        let mut stream = connect(addr);
        let payload = "second sequential connection: 日本語テスト 🚀\n".as_bytes();
        stream
            .write_all(payload)
            .expect("conn 2: failed to write UTF-8 payload");
        stream
            .shutdown(Shutdown::Write)
            .expect("conn 2: shutdown(Shutdown::Write) failed");

        let mut received = Vec::new();
        let n = stream
            .read_to_end(&mut received)
            .expect("conn 2: expected clean EOF without RST");
        assert_eq!(n, payload.len(), "conn 2: unexpected EOF byte count");
        assert_bytes_eq(&received, payload, "conn 2 UTF-8 echo");
    }
}

/// Scenario 2: Arbitrary raw binary bytes (`0x00..=0xFF`, invalid UTF-8,
/// embedded NULs, `\r\n`, lone `\r`, and lone `\n`).
fn raw_binary_bytes(addr: SocketAddr) {
    let mut payload = Vec::with_capacity(4096);

    // Explicit invalid UTF-8 byte sequences.
    payload.extend_from_slice(&[0xFF, 0xFE, 0x80, 0xC0, 0xAF]);
    // Embedded NULs, CRLF, lone CR, lone LF, and mixed binary framing.
    payload.extend_from_slice(b"\x00\r\n\x00\r\x00\n\x00");

    // All 256 byte values repeated 8 times.
    for _ in 0..8 {
        payload.extend(0x00u8..=0xFFu8);
    }

    // Trailing invalid UTF-8 + NUL with no terminating newline to expose
    // accidental line-buffered or UTF-8-decoding servers.
    payload.extend_from_slice(&[0xC0, 0xAF, 0x00, 0xFE, 0xFF, b'\r', 0x00, 0x80]);

    let mut stream = connect(addr);
    stream
        .write_all(&payload)
        .expect("failed to write raw binary payload");
    stream
        .shutdown(Shutdown::Write)
        .expect("shutdown(Shutdown::Write) failed");

    let mut received = Vec::with_capacity(payload.len());
    let n = stream
        .read_to_end(&mut received)
        .expect("expected clean EOF on raw binary echo");
    assert_eq!(n, payload.len(), "raw binary echo byte count mismatch");
    assert_bytes_eq(&received, &payload, "raw binary echo");
}

/// Scenario 3: Large 1 MiB payload with concurrent writer/reader and paced
/// initial reads to force partial reads/writes and TCP backpressure.
fn large_payload_backpressure(addr: SocketAddr) {
    const PAYLOAD_LEN: usize = 1_048_576; // 1 MiB
    const PACED_BYTES: usize = 128 * 1024; // Initial ~128 KiB paced
    const PACED_CHUNK: usize = 4 * 1024; // 4 KiB chunks

    let payload = Xorshift64::new(0x5EED_C0DE_1000_0001).fill_bytes(PAYLOAD_LEN);
    let mut reader = connect(addr);
    let mut writer = reader
        .try_clone()
        .expect("failed to clone client TcpStream");

    let received = thread::scope(|scope| {
        let writer_handle = scope.spawn(|| {
            writer
                .write_all(&payload)
                .expect("writer failed to send full 1 MiB payload");
            writer
                .shutdown(Shutdown::Write)
                .expect("writer failed to half-close after 1 MiB payload");
        });

        let mut received = Vec::with_capacity(PAYLOAD_LEN);
        let mut chunk = [0u8; PACED_CHUNK];
        let mut sleep_bucket = 0usize;

        while received.len() < PACED_BYTES {
            let remaining = PACED_BYTES - received.len();
            let to_read = remaining.min(PACED_CHUNK);
            match reader.read(&mut chunk[..to_read]) {
                Ok(0) => break,
                Ok(n) => {
                    received.extend_from_slice(&chunk[..n]);
                    let new_bucket = received.len() / PACED_CHUNK;
                    if new_bucket > sleep_bucket {
                        sleep_bucket = new_bucket;
                        thread::sleep(Duration::from_millis(2));
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => {
                    panic!("paced reader failed after {} bytes: {err}", received.len());
                }
            }
        }

        reader
            .read_to_end(&mut received)
            .expect("reader failed draining remaining 1 MiB payload to clean EOF");

        writer_handle.join().expect("1 MiB writer thread panicked");

        received
    });

    assert_bytes_eq(&received, &payload, "1 MiB backpressure echo");
}

/// Scenario 4: Fragmented 1-byte trickle writes with `TCP_NODELAY`.
fn fragmented_one_byte_trickle(addr: SocketAddr) {
    const PAYLOAD_LEN: usize = 128;
    const TRICKLE_SLEEP_BYTES: usize = 64;

    let payload = Xorshift64::new(0x0102_0304_0506_0708).fill_bytes(PAYLOAD_LEN);
    let mut stream = connect(addr);
    stream
        .set_nodelay(true)
        .expect("failed to enable TCP_NODELAY");

    for (idx, &byte) in payload.iter().enumerate() {
        stream
            .write_all(&[byte])
            .unwrap_or_else(|err| panic!("failed writing byte {idx}: {err}"));
        if idx < TRICKLE_SLEEP_BYTES {
            thread::sleep(Duration::from_millis(1));
        }
    }

    stream
        .shutdown(Shutdown::Write)
        .expect("shutdown(Shutdown::Write) failed after trickle writes");

    let mut received = Vec::with_capacity(PAYLOAD_LEN);
    let n = stream
        .read_to_end(&mut received)
        .expect("expected clean EOF after 1-byte trickle");
    assert_eq!(n, PAYLOAD_LEN, "trickle echo byte count mismatch");
    assert_bytes_eq(&received, &payload, "1-byte trickle echo");
}

/// Synchronization helper for Scenario 5 so stalled clients stay open while
/// active clients complete, without risking an indefinite hang if a thread
/// panics.
struct StallState {
    ready_count: usize,
    released: bool,
    aborted: bool,
}

struct StallCoordinator {
    state: Mutex<StallState>,
    cvar: Condvar,
}

impl StallCoordinator {
    fn new() -> Self {
        Self {
            state: Mutex::new(StallState {
                ready_count: 0,
                released: false,
                aborted: false,
            }),
            cvar: Condvar::new(),
        }
    }

    fn mark_ready(&self) {
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.ready_count += 1;
        self.cvar.notify_all();
    }

    fn mark_aborted(&self) {
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.aborted = true;
        guard.released = true;
        self.cvar.notify_all();
    }

    fn release_all(&self) {
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.released = true;
        self.cvar.notify_all();
    }

    fn wait_until_ready(&self, expected: usize, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while guard.ready_count < expected && !guard.aborted {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out waiting for {expected} stalled clients to connect and send prefix (ready: {})",
                guard.ready_count
            );
            let (next_guard, _) = self
                .cvar
                .wait_timeout(guard, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard = next_guard;
        }
        assert!(
            !guard.aborted,
            "stalled client failed before entering stalled hold state"
        );
    }

    fn wait_for_release(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !guard.released {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "stalled client timed out waiting for active clients to finish"
            );
            let (next_guard, _) = self
                .cvar
                .wait_timeout(guard, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard = next_guard;
        }
    }
}

struct AbortOnDrop<'a> {
    coord: &'a StallCoordinator,
    disarmed: bool,
}

impl<'a> AbortOnDrop<'a> {
    fn new(coord: &'a StallCoordinator) -> Self {
        Self {
            coord,
            disarmed: false,
        }
    }

    fn disarm(&mut self) {
        self.disarmed = true;
    }
}

impl Drop for AbortOnDrop<'_> {
    fn drop(&mut self) {
        if !self.disarmed {
            self.coord.mark_aborted();
        }
    }
}

struct ReleaseOnDrop<'a>(&'a StallCoordinator);

impl Drop for ReleaseOnDrop<'_> {
    fn drop(&mut self) {
        self.0.release_all();
    }
}

/// Scenario 5: 24 concurrent clients (2 stalled + 22 active) verifying no
/// head-of-line blocking and no cross-connection buffer aliasing.
fn concurrent_clients_no_hol_blocking(addr: SocketAddr) {
    const STALLED_CLIENTS: usize = 2;
    const ACTIVE_CLIENTS: usize = 22;
    const CHUNKS_PER_ACTIVE: usize = 8;
    const CHUNK_SIZE: usize = 4 * 1024;

    let coord = Arc::new(StallCoordinator::new());

    // Spawn the 2 stalled clients first and wait until both have connected and
    // sent their non-empty prefix without half-closing.
    let mut stalled_handles = Vec::with_capacity(STALLED_CLIENTS);
    for stalled_id in 0..STALLED_CLIENTS {
        let coord = Arc::clone(&coord);
        stalled_handles.push(thread::spawn(move || {
            let mut abort_guard = AbortOnDrop::new(&coord);
            let mut stream = connect(addr);

            let prefix = format!("stalled-client-{stalled_id}:prefix:keep-open\n").into_bytes();
            let suffix =
                Xorshift64::new(0x57A1_1ED0_0000_0001 + stalled_id as u64).fill_bytes(2048);

            stream.write_all(&prefix).unwrap_or_else(|err| {
                panic!("stalled client {stalled_id}: prefix write failed: {err}")
            });

            coord.mark_ready();
            abort_guard.disarm();

            // Hold socket open without calling shutdown(Shutdown::Write) until
            // all 22 active clients complete and verify.
            coord.wait_for_release(CLIENT_TIMEOUT);

            stream.write_all(&suffix).unwrap_or_else(|err| {
                panic!("stalled client {stalled_id}: suffix write failed: {err}")
            });
            stream.shutdown(Shutdown::Write).unwrap_or_else(|err| {
                panic!("stalled client {stalled_id}: shutdown(Shutdown::Write) failed: {err}")
            });

            let mut expected = Vec::with_capacity(prefix.len() + suffix.len());
            expected.extend_from_slice(&prefix);
            expected.extend_from_slice(&suffix);

            let mut received = Vec::with_capacity(expected.len());
            stream.read_to_end(&mut received).unwrap_or_else(|err| {
                panic!("stalled client {stalled_id}: read_to_end failed: {err}")
            });
            assert_bytes_eq(
                &received,
                &expected,
                &format!("stalled client {stalled_id} echo"),
            );
        }));
    }

    // Ensure stalled clients are definitely released even if active clients fail.
    let release_guard = ReleaseOnDrop(&coord);
    coord.wait_until_ready(STALLED_CLIENTS, CLIENT_TIMEOUT);

    // Spawn 22 active clients concurrently while the 2 stalled clients remain open.
    let mut active_handles = Vec::with_capacity(ACTIVE_CLIENTS);
    for client_id in 0..ACTIVE_CLIENTS {
        active_handles.push(thread::spawn(move || {
            let mut rng = Xorshift64::new(0xAC71_CE00_0000_0001 + (client_id as u64) * 0x9E37_79B9);
            let mut expected = Vec::with_capacity(CHUNKS_PER_ACTIVE * CHUNK_SIZE);
            let mut chunks = Vec::with_capacity(CHUNKS_PER_ACTIVE);

            for chunk_idx in 0..CHUNKS_PER_ACTIVE {
                let mut chunk = rng.fill_bytes(CHUNK_SIZE);
                // Stamp unique client/chunk markers at start and end so any
                // cross-connection slab/buffer aliasing is unmistakable.
                chunk[0] = client_id as u8;
                chunk[1] = chunk_idx as u8;
                chunk[CHUNK_SIZE - 2] = client_id as u8;
                chunk[CHUNK_SIZE - 1] = chunk_idx as u8;
                expected.extend_from_slice(&chunk);
                chunks.push(chunk);
            }

            let mut stream = connect(addr);
            let mut received = Vec::with_capacity(expected.len());
            let mut read_buf = vec![0u8; CHUNK_SIZE];

            // Interleave writes and reads for the first 7 chunks so servers
            // that erroneously buffer until EOF are caught immediately.
            for (chunk_idx, chunk) in chunks.iter().enumerate().take(CHUNKS_PER_ACTIVE - 1) {
                stream.write_all(chunk).unwrap_or_else(|err| {
                    panic!("active client {client_id} chunk {chunk_idx}: write failed: {err}")
                });
                stream.read_exact(&mut read_buf).unwrap_or_else(|err| {
                    panic!("active client {client_id} chunk {chunk_idx}: read_exact failed: {err}")
                });
                received.extend_from_slice(&read_buf);
            }

            // Write the final chunk, half-close, and drain cleanly to EOF.
            let last_chunk = &chunks[CHUNKS_PER_ACTIVE - 1];
            stream.write_all(last_chunk).unwrap_or_else(|err| {
                panic!("active client {client_id} final chunk: write failed: {err}")
            });
            stream.shutdown(Shutdown::Write).unwrap_or_else(|err| {
                panic!("active client {client_id}: shutdown(Shutdown::Write) failed: {err}")
            });
            stream.read_to_end(&mut received).unwrap_or_else(|err| {
                panic!("active client {client_id}: read_to_end failed: {err}")
            });

            assert_bytes_eq(
                &received,
                &expected,
                &format!("active client {client_id} echo"),
            );
        }));
    }

    let mut failures = Vec::new();
    for (idx, handle) in active_handles.into_iter().enumerate() {
        if let Err(err) = handle.join() {
            failures.push(format!("active client {idx} panicked: {err:?}"));
        }
    }

    // Release stalled clients now that all active clients have finished.
    drop(release_guard);

    for (idx, handle) in stalled_handles.into_iter().enumerate() {
        if let Err(err) = handle.join() {
            failures.push(format!("stalled client {idx} panicked: {err:?}"));
        }
    }

    assert!(
        failures.is_empty(),
        "concurrent clients scenario failed:\n{}",
        failures.join("\n")
    );
}

/// Scenario 6: Immediate 0-byte EOF, abrupt client drops (both mid-write and
/// immediate post-connect), and post-chaos survival check.
fn zero_byte_eof_and_abrupt_drops(addr: SocketAddr) {
    // 1. 10 connections that immediately half-close with 0 bytes sent.
    for i in 0..10 {
        let mut stream = connect(addr);
        stream.shutdown(Shutdown::Write).unwrap_or_else(|err| {
            panic!("0-byte EOF conn {i}: shutdown(Shutdown::Write) failed: {err}")
        });
        let mut received = Vec::new();
        let n = stream.read_to_end(&mut received).unwrap_or_else(|err| {
            panic!("0-byte EOF conn {i}: expected clean Ok(0) EOF without RST: {err}")
        });
        assert_eq!(n, 0, "0-byte EOF conn {i}: expected 0 bytes, got {n}");
        assert!(received.is_empty());
    }

    // 2. 20 abrupt drops:
    //    - 10 send 32 KiB and drop immediately without reading (inducing
    //      EPIPE / ECONNRESET on server writes).
    //    - 10 drop immediately right after connect().
    let drop_payload = Xorshift64::new(0xAB04_7D00_1234_5678).fill_bytes(32 * 1024);
    for i in 0..10 {
        let mut stream = connect(addr);
        stream
            .write_all(&drop_payload)
            .unwrap_or_else(|err| panic!("abrupt drop writer {i}: write_all failed: {err}"));
        drop(stream);
    }

    for _ in 0..10 {
        let stream = connect(addr);
        drop(stream);
    }

    // 3. Post-chaos survival check: fresh small connections plus a 64 KiB echo.
    for i in 0..4 {
        let mut stream = connect(addr);
        let payload = format!("post-chaos-health-check-{i}\n").into_bytes();
        stream
            .write_all(&payload)
            .unwrap_or_else(|err| panic!("post-chaos small conn {i}: write failed: {err}"));
        stream.shutdown(Shutdown::Write).unwrap_or_else(|err| {
            panic!("post-chaos small conn {i}: shutdown(Shutdown::Write) failed: {err}")
        });
        let mut received = Vec::new();
        stream
            .read_to_end(&mut received)
            .unwrap_or_else(|err| panic!("post-chaos small conn {i}: read_to_end failed: {err}"));
        assert_bytes_eq(
            &received,
            &payload,
            &format!("post-chaos small conn {i} echo"),
        );
    }

    // 64 KiB concurrent echo survival check.
    let survival_payload = Xorshift64::new(0x5076_17A1_6400_0001).fill_bytes(64 * 1024);
    let mut reader = connect(addr);
    let mut writer = reader
        .try_clone()
        .expect("post-chaos 64 KiB: failed to clone client TcpStream");

    let received = thread::scope(|scope| {
        let writer_handle = scope.spawn(|| {
            writer
                .write_all(&survival_payload)
                .expect("post-chaos 64 KiB: write_all failed");
            writer
                .shutdown(Shutdown::Write)
                .expect("post-chaos 64 KiB: shutdown(Shutdown::Write) failed");
        });

        let mut received = Vec::with_capacity(survival_payload.len());
        reader
            .read_to_end(&mut received)
            .expect("post-chaos 64 KiB: expected clean EOF");

        writer_handle
            .join()
            .expect("post-chaos 64 KiB: writer thread panicked");

        received
    });

    assert_bytes_eq(&received, &survival_payload, "post-chaos 64 KiB echo");
}

#[cfg(test)]
mod reference_server {
    use std::io::{self, Read, Write};
    use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;

    /// Minimal thread-per-connection `std::net` reference echo server used to
    /// self-verify that the adversarial test suite is deterministic and
    /// deadlock-free while `p0-smoke` remains a `todo!()` stub.
    struct ReferenceEchoServer {
        addr: SocketAddr,
        shutdown: Arc<AtomicBool>,
        accept_thread: Option<thread::JoinHandle<()>>,
    }

    impl ReferenceEchoServer {
        fn start() -> Self {
            let listener =
                TcpListener::bind("127.0.0.1:0").expect("failed to bind reference echo server");
            let addr = listener
                .local_addr()
                .expect("failed to query reference echo server addr");
            let shutdown = Arc::new(AtomicBool::new(false));
            let shutdown_flag = Arc::clone(&shutdown);

            let accept_thread = thread::spawn(move || {
                let mut workers = Vec::new();

                loop {
                    match listener.accept() {
                        Ok((stream, _peer)) => {
                            if shutdown_flag.load(Ordering::Acquire) {
                                drop(stream);
                                break;
                            }
                            workers.push(thread::spawn(move || handle_connection(stream)));
                        }
                        Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                        Err(_) => break,
                    }
                }

                drop(listener);
                for handle in workers {
                    let _ = handle.join();
                }
            });

            Self {
                addr,
                shutdown,
                accept_thread: Some(accept_thread),
            }
        }

        fn addr(&self) -> SocketAddr {
            self.addr
        }
    }

    impl Drop for ReferenceEchoServer {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::Release);
            let _ = TcpStream::connect(self.addr);
            if let Some(handle) = self.accept_thread.take() {
                let _ = handle.join();
            }
        }
    }

    fn handle_connection(mut stream: TcpStream) {
        let mut buf = [0u8; 8192];
        loop {
            match stream.read(&mut buf) {
                Ok(0) => {
                    let _ = stream.shutdown(Shutdown::Write);
                    break;
                }
                Ok(n) => {
                    if stream.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
    }

    #[test]
    fn basic_utf8_and_half_close() {
        let server = ReferenceEchoServer::start();
        super::basic_utf8_and_half_close(server.addr());
    }

    #[test]
    fn raw_binary_bytes() {
        let server = ReferenceEchoServer::start();
        super::raw_binary_bytes(server.addr());
    }

    #[test]
    fn large_payload_backpressure() {
        let server = ReferenceEchoServer::start();
        super::large_payload_backpressure(server.addr());
    }

    #[test]
    fn fragmented_one_byte_trickle() {
        let server = ReferenceEchoServer::start();
        super::fragmented_one_byte_trickle(server.addr());
    }

    #[test]
    fn concurrent_clients_no_hol_blocking() {
        let server = ReferenceEchoServer::start();
        super::concurrent_clients_no_hol_blocking(server.addr());
    }

    #[test]
    fn zero_byte_eof_and_abrupt_drops() {
        let server = ReferenceEchoServer::start();
        super::zero_byte_eof_and_abrupt_drops(server.addr());
    }
}

mod p0_smoke_bin {
    use harness::TestServer;

    #[test]
    #[ignore = "requires human implementation of p0-smoke; run with: cargo build -p p0-smoke && cargo test -p harness --test p0_smoke -- --ignored"]
    fn basic_utf8_and_half_close() {
        let server = TestServer::spawn_workspace_bin("p0-smoke")
            .expect("failed to spawn p0-smoke workspace binary");
        super::basic_utf8_and_half_close(server.addr());
    }

    #[test]
    #[ignore = "requires human implementation of p0-smoke; run with: cargo build -p p0-smoke && cargo test -p harness --test p0_smoke -- --ignored"]
    fn raw_binary_bytes() {
        let server = TestServer::spawn_workspace_bin("p0-smoke")
            .expect("failed to spawn p0-smoke workspace binary");
        super::raw_binary_bytes(server.addr());
    }

    #[test]
    #[ignore = "requires human implementation of p0-smoke; run with: cargo build -p p0-smoke && cargo test -p harness --test p0_smoke -- --ignored"]
    fn large_payload_backpressure() {
        let server = TestServer::spawn_workspace_bin("p0-smoke")
            .expect("failed to spawn p0-smoke workspace binary");
        super::large_payload_backpressure(server.addr());
    }

    #[test]
    #[ignore = "requires human implementation of p0-smoke; run with: cargo build -p p0-smoke && cargo test -p harness --test p0_smoke -- --ignored"]
    fn fragmented_one_byte_trickle() {
        let server = TestServer::spawn_workspace_bin("p0-smoke")
            .expect("failed to spawn p0-smoke workspace binary");
        super::fragmented_one_byte_trickle(server.addr());
    }

    #[test]
    #[ignore = "requires human implementation of p0-smoke; run with: cargo build -p p0-smoke && cargo test -p harness --test p0_smoke -- --ignored"]
    fn concurrent_clients_no_hol_blocking() {
        let server = TestServer::spawn_workspace_bin("p0-smoke")
            .expect("failed to spawn p0-smoke workspace binary");
        super::concurrent_clients_no_hol_blocking(server.addr());
    }

    #[test]
    #[ignore = "requires human implementation of p0-smoke; run with: cargo build -p p0-smoke && cargo test -p harness --test p0_smoke -- --ignored"]
    fn zero_byte_eof_and_abrupt_drops() {
        let server = TestServer::spawn_workspace_bin("p0-smoke")
            .expect("failed to spawn p0-smoke workspace binary");
        super::zero_byte_eof_and_abrupt_drops(server.addr());
    }
}
