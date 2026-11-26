//! Minimal raw Linux `io_uring` reactor skeleton for Protohackers servers.
//!
//! # Ring Lifecycle (SQE -> Kernel -> CQE)
//!
//! An `io_uring` instance exposes two ring buffers shared between userspace and
//! the kernel:
//!
//! 1. **Submission Queue (SQ):** Userspace constructs Submission Queue Entries
//!    (SQEs) describing asynchronous operations (`Accept`, `Recv`, `Send`,
//!    `Shutdown`, `Close`), pushes them into the SQ ring, and notifies the
//!    kernel via `io_uring_enter` (`IoUring::submit` / `submit_and_wait`).
//! 2. **Kernel execution:** The kernel consumes SQEs and performs the I/O
//!    asynchronously, reading from or writing into the raw pointers supplied in
//!    each SQE.
//! 3. **Completion Queue (CQ):** Once an operation finishes (or is canceled),
//!    the kernel posts a Completion Queue Entry (CQE) carrying the original
//!    `user_data` tag and a signed `i32` result (`>= 0` for byte counts / FDs,
//!    `< 0` for negated `-errno`).
//!
//! # Buffer Ownership Across Kernel Completion
//!
//! Unlike readiness-based `epoll`, `io_uring` is completion-based: the kernel
//! holds raw pointers into userspace buffers for the entire interval between
//! SQE submission and CQE reaping. Any buffer passed to `opcode::Recv` or
//! `opcode::Send` **must remain alive and at an unchanging heap address**
//! (never moved by a `Vec` reallocation, never dropped, and never mutated in
//! place by userspace) until the matching CQE has been reaped.
//!
//! # `user_data` Tagging
//!
//! Every SQE carries an opaque 64-bit `user_data` value that the kernel echoes
//! back verbatim in the corresponding CQE. [`UserData`] packs a slab token
//! ([`ConnId`]) and an [`OpKind`] into those 64 bits so the reactor can route
//! completions back to the right [`Connection`] and operation without extra
//! allocations.

pub mod connection;
pub mod reactor;

pub use connection::{ConnId, ConnState, Connection, OpKind, UserData};
pub use reactor::{Handler, Reactor, ReactorConfig};
