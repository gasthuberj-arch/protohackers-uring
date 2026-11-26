//! Protohackers Problem 0: Smoke Test (TCP Echo Server) skeleton.
//!
//! All protocol handling and `io_uring` reactor orchestration intentionally
//! remain `todo!()` for human implementation.

use std::io;
use std::net::SocketAddr;
use std::process::ExitCode;

use uring_core::{ConnId, Handler, Reactor, ReactorConfig};

const USAGE: &str = "Usage: p0-smoke [--bind <addr> | --bind=<addr>]";

/// Protocol handler for Protohackers #0 (Smoke Test).
///
/// Echoes every byte received back to the peer verbatim, preserving TCP
/// half-close semantics (flushing all queued outbound bytes before shutting
/// down the write half after `on_peer_eof`).
struct SmokeHandler;

impl Handler for SmokeHandler {
    #[allow(unused_variables)]
    fn on_accept(&mut self, conn_id: ConnId, reactor: &mut Reactor) {
        todo!("handle newly accepted connection for Protohackers #0")
    }

    #[allow(unused_variables)]
    fn on_read(&mut self, conn_id: ConnId, data: &[u8], reactor: &mut Reactor) {
        todo!("echo received bytes back via reactor.queue_send(conn_id, data)")
    }

    #[allow(unused_variables)]
    fn on_peer_eof(&mut self, conn_id: ConnId, reactor: &mut Reactor) {
        todo!("handle TCP half-close: flush remaining outbound bytes then queue_shutdown_write / queue_close")
    }

    #[allow(unused_variables)]
    fn on_write_complete(&mut self, conn_id: ConnId, bytes_written: usize, reactor: &mut Reactor) {
        todo!("handle completed outbound write and continue shutdown if peer already sent EOF")
    }

    #[allow(unused_variables)]
    fn on_close(&mut self, conn_id: ConnId) {
        todo!("clean up any per-connection protocol state after connection closes")
    }
}

/// Starts the Protohackers #0 server bound to `bind_addr`.
///
/// # Stdout Readiness Contract
///
/// Immediately after [`Reactor::bind_and_listen`] succeeds, this function MUST
/// print `LISTENING <actual-addr>\n` to `stdout` and flush `stdout` before
/// entering [`Reactor::run`]. The integration harness (`crates/harness`) relies
/// on this line to discover the OS-assigned ephemeral port.
#[allow(unused_variables)]
fn run(bind_addr: SocketAddr) -> io::Result<()> {
    let _ = (Reactor::new, ReactorConfig::default, SmokeHandler);
    todo!(
        "initialize Reactor, bind_and_listen(bind_addr), print and flush 'LISTENING {{actual_addr}}', and drive SmokeHandler"
    )
}

fn parse_bind_addr(mut args: impl Iterator<Item = String>) -> Result<SocketAddr, String> {
    let mut bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));

    while let Some(arg) = args.next() {
        if arg == "--bind" {
            let value = args
                .next()
                .ok_or_else(|| "missing value for --bind".to_string())?;
            bind_addr = value
                .parse::<SocketAddr>()
                .map_err(|err| format!("invalid socket address '{value}': {err}"))?;
        } else if let Some(value) = arg.strip_prefix("--bind=") {
            bind_addr = value
                .parse::<SocketAddr>()
                .map_err(|err| format!("invalid socket address '{value}': {err}"))?;
        } else {
            return Err(format!("unrecognized argument '{arg}'"));
        }
    }

    Ok(bind_addr)
}

fn main() -> ExitCode {
    let bind_addr = match parse_bind_addr(std::env::args().skip(1)) {
        Ok(addr) => addr,
        Err(err) => {
            eprintln!("error: {err}\n{USAGE}");
            return ExitCode::from(2);
        }
    };

    if let Err(err) = run(bind_addr) {
        eprintln!("fatal: {err}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
