# Mission

## Objective

Build deep, first-principles systems intuition for raw Linux `io_uring` by solving the Protohackers network programming challenges directly on top of kernel rings:

- **Ring mechanics:** Submission Queue (SQ) and Completion Queue (CQ) lifecycle, batching, SQ backpressure, and `io_uring_enter` wakeups.
- **Buffer ownership & pinning invariants:** Keeping heap memory at stable addresses until the kernel posts the matching CQE, and decoupling FD lifecycle from Rust drop order via explicit `IORING_OP_CLOSE`.
- **Advanced `io_uring` features:** Multishot `accept` and `recv`, provided buffer rings, and avoiding unnecessary copies/syscalls.
- **TCP semantics under stress:** Half-close (`SHUT_WR` / `FIN` handling without discarding queued outbound data), short reads/writes, partial framing, slowloris peers, and backpressure.
- **Empirical trade-offs:** Comparing raw `io_uring` against `epoll`/async baselines with real measurements rather than folklore.

## Non-Goals

- **No async runtimes** (`tokio`, `async-std`, `smol`) in `crates/uring-core` or `crates/p*`.
- **No leaky or opaque abstractions** that hide syscalls, ring pressure, buffer lifetimes, or state transitions behind executor magic.

## Division of Labor

- **Human:** Writes every line of `crates/uring-core` and every protocol server in `crates/p*`.
- **Homelab KVM microVM orbs:** Provide deterministic Nix/Cargo tooling, guided `todo!()` skeletons, adversarial compliance and concurrency test suites in `crates/harness` designed to expose subtle TCP/ring edge cases, and optional isolated bake-off baselines in separate crates.

## Engineering Principles

1. **Explicit state machines:** Every connection transition (`Open` -> `PeerEof` -> `ShuttingDown` -> `Closing`) is explicit and auditable; no implicit cleanup while SQEs remain in flight.
2. **Bounded memory & explicit backpressure:** Connection counts, buffer capacities, and outbound queues are bounded intentionally; overload is handled deliberately, never by unbounded allocation.
3. **Measurements over assumptions:** Validate syscall counts, packet sequences, and throughput/latency hypotheses using `strace`, `tcpdump`, and reproducible benchmarks.
