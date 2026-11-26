//! Per-connection state, `user_data` encoding, and buffer-ownership invariants.

use std::os::fd::RawFd;

/// Stable slab token identifying a single connection in the reactor's
/// `Slab<Connection>`.
///
/// The wrapped `usize` is the index returned by `slab::Slab::insert` and is
/// packed into SQE `user_data` via [`UserData`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnId(pub usize);

/// Explicit TCP connection lifecycle state machine.
///
/// # TCP Half-Close Semantics
///
/// TCP is full-duplex: each direction closes independently. When a `Recv` CQE
/// completes with `0` bytes, the peer has sent a `FIN` and will not send any
/// more data, transitioning the connection to [`ConnState::PeerEof`].
///
/// **Critical invariant:** Receiving `0` bytes does **not** mean the connection
/// can be torn down immediately or that queued outbound bytes in `write_buf`
/// can be discarded. The peer may still be reading our response (e.g., an echo
/// server flushing buffered bytes after the client shuts down its write half).
/// Only after outbound data is drained and our write half is shut down (or an
/// unrecoverable error occurs) should the connection transition through
/// [`ConnState::ShuttingDown`] and [`ConnState::Closing`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnState {
    /// Both read and write halves are open.
    Open,
    /// Peer sent `FIN` (`Recv` returned `0`); no further reads will be armed,
    /// but queued outbound writes must still be flushed before closing.
    PeerEof,
    /// Outbound writes have drained and a `ShutdownWrite` (`SHUT_WR`) SQE is
    /// in flight or completed; waiting to issue or finish `Close`.
    ShuttingDown,
    /// `Close` has been queued; waiting for all remaining `in_flight` CQEs to
    /// complete before removing the slot from the slab.
    Closing,
}

/// Discriminant identifying which `io_uring` operation an SQE/CQE represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    /// Listening socket `accept` (single-shot or multishot).
    Accept,
    /// Connection `recv` into `Connection::read_buf`.
    Recv,
    /// Connection `send` from `Connection::write_buf`.
    Send,
    /// Half-close outbound direction via `shutdown(fd, SHUT_WR)`.
    ShutdownWrite,
    /// Explicit asynchronous `close(fd)` via `opcode::Close`.
    Close,
}

/// 64-bit tag stored in `SQE::user_data` and echoed back in `CQE::user_data`.
///
/// Packs a [`ConnId`] and an [`OpKind`] into a single `u64` (for example,
/// placing the `OpKind` discriminant in the low 3–8 bits and shifting the slab
/// index into the upper bits, or vice versa, using a reserved sentinel for the
/// listener socket on [`OpKind::Accept`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserData(pub u64);

impl UserData {
    /// Bit-packs `(conn, op)` into a 64-bit SQE `user_data` value.
    ///
    /// Ensure the chosen bit layout cannot collide across any valid slab index
    /// (up to `ReactorConfig::max_connections`) and all [`OpKind`] variants.
    #[allow(unused_variables)]
    pub fn encode(conn: ConnId, op: OpKind) -> Self {
        todo!("bit-pack ConnId and OpKind into a u64 UserData tag")
    }

    /// Unpacks a CQE `user_data` value back into its `(ConnId, OpKind)` pair.
    ///
    /// Must be the exact inverse of [`UserData::encode`].
    pub fn decode(self) -> (ConnId, OpKind) {
        todo!("unpack UserData u64 back into (ConnId, OpKind)")
    }
}

/// Per-connection kernel-owned buffers, file descriptor, and in-flight SQE
/// accounting.
///
/// # Safety & Ownership Invariants
///
/// 1. **`RawFd` + explicit `opcode::Close` (not `OwnedFd`):**
///    Dropping an `OwnedFd` closes the file descriptor synchronously. If any
///    SQE referencing that FD is still in flight in the kernel, the kernel may
///    either operate on a recycled FD number assigned to a newly accepted
///    connection or race with teardown. Storing a `RawFd` and closing it
///    explicitly via `opcode::Close` (only after transitioning to
///    [`ConnState::Closing`]) keeps FD lifetime under strict reactor control.
///
/// 2. **Pinned, stable buffer addresses during in-flight I/O:**
///    When `opcode::Recv` is submitted with `read_buf.as_mut_ptr()` or
///    `opcode::Send` is submitted with `write_buf.as_ptr()`, the kernel retains
///    those raw pointers until the matching CQE is reaped.
///    - `read_buf` is a fixed-capacity `Box<[u8]>` so its heap allocation never
///      moves even if `Connection` is moved, as long as `read_buf` is neither
///      dropped nor replaced while a `Recv` SQE is in flight.
///    - `write_buf` is a `Vec<u8>`: **never** call `push`/`extend`/`reserve`
///      (which may reallocate and free the backing buffer!) while a `Send` SQE
///      holds a pointer into `write_buf`. Either keep a separate staging queue
///      for newly queued bytes while a send is in flight, or use a fixed/swapped
///      send buffer.
///
/// 3. **`in_flight` reference counting before slab removal:**
///    Increment `in_flight` whenever an SQE is pushed for this connection, and
///    decrement it when each corresponding CQE is reaped (including `ECANCELED`
///    and `Close` completions). Only remove the `Connection` from
///    `Slab<Connection>` once `state == ConnState::Closing` **and**
///    `in_flight == 0`. Removing earlier would drop `read_buf`/`write_buf`
///    while the kernel still holds pointers to them (use-after-free).
pub struct Connection {
    #[allow(dead_code)]
    fd: RawFd,
    #[allow(dead_code)]
    state: ConnState,
    #[allow(dead_code)]
    read_buf: Box<[u8]>,
    #[allow(dead_code)]
    write_buf: Vec<u8>,
    #[allow(dead_code)]
    in_flight: usize,
}

impl Connection {
    /// Creates a new [`Connection`] in [`ConnState::Open`] with a heap-allocated
    /// read buffer of `read_buf_size` bytes, an empty write buffer, and zero
    /// in-flight SQEs.
    #[allow(unused_variables)]
    pub fn new(fd: RawFd, read_buf_size: usize) -> Self {
        todo!("initialize Connection with fd, ConnState::Open, boxed read_buf, empty write_buf, and in_flight = 0")
    }

    /// Returns the raw file descriptor for this connection.
    pub fn fd(&self) -> RawFd {
        todo!("return self.fd")
    }

    /// Returns the current [`ConnState`] of this connection.
    pub fn state(&self) -> ConnState {
        todo!("return self.state")
    }
}
