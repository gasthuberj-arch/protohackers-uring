# Agent Guidelines

This repository is a hands-on systems learning project for building deep Linux `io_uring` fluency from first principles. Read and follow every rule below before writing or modifying code.

## 1. Strict Human vs. Agent Boundary

- **Human owns (NEVER implement):**
  - `crates/uring-core` — raw `io_uring` (`io-uring` + `slab`) SQE/CQE reactor loop, buffer ownership/pinning, and connection state machine.
  - `crates/p*` (`crates/p0-smoke`, `crates/p1-prime-time`, etc.) — Protohackers protocol server logic.
  - Agents MUST NEVER implement `io_uring` reactor logic or protocol logic unless an issue explicitly requests an isolated "trade-off baseline" in a separate crate (for example, a standalone `tokio` or `epoll` comparison crate).
- **Agents / orbs own:**
  - Repository scaffolding, Nix `flake.nix` (and lockfiles), CI/quality checks, and documentation.
  - Guided `todo!()` skeletons (types, traits, doc comments, and stub methods only — zero runtime implementation).
  - Adversarial compliance, concurrency, and stress test suites (`crates/harness`).
  - Optional isolated trade-off baselines (only when explicitly requested) and architecture/trade-off notes.
- **Ambiguity or boundary conflict:**
  - If an issue is ambiguous, missing key requirements, or appears to violate the Human vs. Agent boundary, stop immediately and write a concise list of blocking questions to `.agent-clarification.md` in the repository root instead of guessing or writing code.

## 2. Stdout Readiness Contract (`crates/p*`)

Every protocol binary in `crates/p*` MUST obey this contract so `crates/harness` can spawn it deterministically on an ephemeral port:

1. Accept `--bind <addr>` (and `--bind=<addr>`), defaulting to `127.0.0.1:0`.
2. Immediately after `bind` and `listen` succeed, print exactly:
   ```text
   LISTENING <actual-socket-addr>
   ```
   followed by `\n` to `stdout`, and flush `stdout` before entering the event loop.

## 3. Required Verification Commands

All of the following commands MUST exit `0` before committing:

```sh
nix flake check --no-build
nix develop --command cargo check --workspace --all-targets
nix develop --command cargo clippy --workspace --all-targets -- -D warnings
nix develop --command cargo fmt --all -- --check
nix develop --command cargo test -p harness
```

Note: When modifying or adding `flake.nix`, stage it (`git add flake.nix`) before invoking `nix flake check` or `nix develop`, because Nix ignores untracked files in git working trees.

## 4. Commit Convention

Use clear, descriptive commit messages ending with `Fixes #<N>` for the corresponding GitHub issue.
