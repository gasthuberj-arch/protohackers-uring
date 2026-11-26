//! Integration test harness for spawning and managing Protohackers server
//! binaries.

use std::ffi::OsStr;
use std::io::{self, BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Default timeout to wait for a spawned server binary to print its
/// `LISTENING <addr>` readiness line on stdout.
pub const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum number of bytes retained from a child process's stderr for startup
/// diagnostics. Any bytes beyond this limit are still drained until EOF so the
/// child never blocks on a full stderr pipe.
const MAX_STDERR_BYTES: usize = 64 * 1024;

/// Maximum time to wait for the stderr drainer thread to finish flushing after
/// child termination when assembling a startup failure message.
const STDERR_FLUSH_WAIT: Duration = Duration::from_millis(200);

/// A running Protohackers server child process bound to an ephemeral address.
///
/// Dropping `TestServer` kills and reaps the child process so tests never leak
/// background server processes, even on panic.
pub struct TestServer {
    child: Child,
    addr: SocketAddr,
}

impl TestServer {
    /// Spawns `bin` with `--bind 127.0.0.1:0` and waits up to
    /// [`DEFAULT_STARTUP_TIMEOUT`] for `LISTENING <addr>` on stdout.
    pub fn spawn(bin: impl AsRef<Path>) -> io::Result<Self> {
        Self::spawn_with_timeout(bin, DEFAULT_STARTUP_TIMEOUT)
    }

    /// Spawns `bin` with `--bind 127.0.0.1:0` and waits up to `timeout` for a
    /// line strictly matching `LISTENING <SocketAddr>` on stdout.
    pub fn spawn_with_timeout(bin: impl AsRef<Path>, timeout: Duration) -> io::Result<Self> {
        let bin_path = bin.as_ref();
        let mut child = Command::new(bin_path)
            .args(["--bind", "127.0.0.1:0"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!("failed to spawn '{}': {err}", bin_path.display()),
                )
            })?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("child stdout pipe was not captured"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("child stderr pipe was not captured"))?;

        let (line_tx, line_rx) = mpsc::channel::<io::Result<String>>();
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            let mut send_open = true;

            loop {
                line.clear();
                match reader.read_line(&mut line) {
                    Ok(0) => break,
                    Ok(_) => {
                        if send_open && line_tx.send(Ok(line.clone())).is_err() {
                            send_open = false;
                        }
                    }
                    Err(err) => {
                        if send_open {
                            let _ = line_tx.send(Err(err));
                        }
                        break;
                    }
                }
            }
        });

        let stderr_buf = Arc::new(Mutex::new(String::new()));
        let stderr_buf_writer = Arc::clone(&stderr_buf);
        let (stderr_done_tx, stderr_done_rx) = mpsc::sync_channel::<()>(1);
        thread::spawn(move || {
            let mut stderr = stderr;
            let mut chunk = [0u8; 4096];
            let mut captured = 0usize;

            loop {
                match stderr.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if captured < MAX_STDERR_BYTES {
                            let remaining = MAX_STDERR_BYTES - captured;
                            let to_take = n.min(remaining);
                            let text = String::from_utf8_lossy(&chunk[..to_take]);
                            if let Ok(mut guard) = stderr_buf_writer.lock() {
                                guard.push_str(&text);
                            }
                            captured += to_take;
                        }
                    }
                }
            }

            let _ = stderr_done_tx.send(());
        });

        let deadline = Instant::now() + timeout;

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let stderr_text =
                    cleanup_and_collect_stderr(&mut child, &stderr_buf, &stderr_done_rx, true);
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format_startup_error(
                        bin_path,
                        &format!(
                            "timed out after {timeout:?} waiting for 'LISTENING <addr>' on stdout"
                        ),
                        &stderr_text,
                    ),
                ));
            }

            match line_rx.recv_timeout(remaining) {
                Ok(Ok(line)) => {
                    let trimmed = line.trim();
                    if let Some(rest) = trimmed.strip_prefix("LISTENING ") {
                        match rest.parse::<SocketAddr>() {
                            Ok(addr) => {
                                return Ok(Self { child, addr });
                            }
                            Err(err) => {
                                let stderr_text = cleanup_and_collect_stderr(
                                    &mut child,
                                    &stderr_buf,
                                    &stderr_done_rx,
                                    true,
                                );
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format_startup_error(
                                        bin_path,
                                        &format!(
                                            "invalid socket address in readiness line '{trimmed}': {err}"
                                        ),
                                        &stderr_text,
                                    ),
                                ));
                            }
                        }
                    } else if trimmed == "LISTENING" {
                        let stderr_text = cleanup_and_collect_stderr(
                            &mut child,
                            &stderr_buf,
                            &stderr_done_rx,
                            true,
                        );
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format_startup_error(
                                bin_path,
                                "malformed readiness line 'LISTENING' missing socket address",
                                &stderr_text,
                            ),
                        ));
                    }
                }
                Ok(Err(err)) => {
                    let stderr_text =
                        cleanup_and_collect_stderr(&mut child, &stderr_buf, &stderr_done_rx, true);
                    return Err(io::Error::new(
                        err.kind(),
                        format_startup_error(
                            bin_path,
                            &format!("failed reading child stdout: {err}"),
                            &stderr_text,
                        ),
                    ));
                }
                Err(RecvTimeoutError::Timeout) => {
                    let stderr_text =
                        cleanup_and_collect_stderr(&mut child, &stderr_buf, &stderr_done_rx, true);
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format_startup_error(
                            bin_path,
                            &format!(
                                "timed out after {timeout:?} waiting for 'LISTENING <addr>' on stdout"
                            ),
                            &stderr_text,
                        ),
                    ));
                }
                Err(RecvTimeoutError::Disconnected) => {
                    let status = wait_or_kill(&mut child, &stderr_done_rx);
                    let stderr_text = read_stderr(&stderr_buf);
                    let detail = match status {
                        Some(s) => format!("child exited before printing 'LISTENING <addr>' ({s})"),
                        None => {
                            "child closed stdout before printing 'LISTENING <addr>'".to_string()
                        }
                    };
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format_startup_error(bin_path, &detail, &stderr_text),
                    ));
                }
            }
        }
    }

    /// Spawns a workspace binary by name.
    ///
    /// If the `PROTOHACKERS_BIN` environment variable is set and non-empty,
    /// that executable path is spawned directly. Otherwise, the binary is
    /// resolved next to the current test executable's target profile directory
    /// (walking up from `deps/` when invoked via `cargo test`).
    ///
    /// Does not invoke `cargo build`; if the binary is missing, returns
    /// [`io::ErrorKind::NotFound`] advising `cargo build --bin <bin_name>`.
    pub fn spawn_workspace_bin(bin_name: &str) -> io::Result<Self> {
        if let Some(override_bin) = std::env::var_os("PROTOHACKERS_BIN") {
            if !override_bin.is_empty() {
                return Self::spawn(PathBuf::from(override_bin));
            }
        }

        let current_exe = std::env::current_exe()?;
        let mut dir = current_exe.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "could not determine parent directory of current executable '{}'",
                    current_exe.display()
                ),
            )
        })?;

        if dir.file_name() == Some(OsStr::new("deps")) {
            if let Some(parent) = dir.parent() {
                dir = parent;
            }
        }

        let bin_file = format!("{bin_name}{}", std::env::consts::EXE_SUFFIX);
        let bin_path = dir.join(bin_file);

        if !bin_path.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "workspace binary '{bin_name}' not found at '{}'; build it first with `cargo build --bin {bin_name}`",
                    bin_path.display()
                ),
            ));
        }

        Self::spawn(bin_path)
    }

    /// Returns the socket address reported by the server's `LISTENING <addr>`
    /// readiness line.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Returns the OS process identifier of the running server child process.
    pub fn child_id(&self) -> u32 {
        self.child.id()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn cleanup_and_collect_stderr(
    child: &mut Child,
    stderr_buf: &Arc<Mutex<String>>,
    stderr_done_rx: &mpsc::Receiver<()>,
    force_kill: bool,
) -> String {
    if force_kill {
        let _ = child.kill();
    }
    let _ = child.wait();
    let _ = stderr_done_rx.recv_timeout(STDERR_FLUSH_WAIT);
    read_stderr(stderr_buf)
}

fn wait_or_kill(
    child: &mut Child,
    stderr_done_rx: &mpsc::Receiver<()>,
) -> Option<std::process::ExitStatus> {
    let _ = stderr_done_rx.recv_timeout(STDERR_FLUSH_WAIT);
    if let Ok(Some(status)) = child.try_wait() {
        return Some(status);
    }
    let _ = child.kill();
    let status = child.wait().ok();
    let _ = stderr_done_rx.recv_timeout(STDERR_FLUSH_WAIT);
    status
}

fn read_stderr(stderr_buf: &Arc<Mutex<String>>) -> String {
    stderr_buf
        .lock()
        .map(|guard| guard.clone())
        .unwrap_or_default()
}

fn format_startup_error(bin_path: &Path, reason: &str, stderr_text: &str) -> String {
    let trimmed_stderr = stderr_text.trim();
    if trimmed_stderr.is_empty() {
        format!("'{}': {reason}", bin_path.display())
    } else {
        format!(
            "'{}': {reason}\nstderr:\n{trimmed_stderr}",
            bin_path.display()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempScript {
        dir: PathBuf,
        script_path: PathBuf,
    }

    impl TempScript {
        fn with_dir(name: &str, build_body: impl FnOnce(&Path) -> String) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "protohackers-harness-test-{}-{}-{id}",
                std::process::id(),
                name
            ));
            fs::create_dir_all(&dir).expect("create temp test dir");
            let script_path = dir.join("server.sh");
            let body = build_body(&dir);

            {
                let mut file = File::create(&script_path).expect("create temp script");
                file.write_all(body.as_bytes()).expect("write temp script");
                file.sync_all().expect("sync temp script");
            }

            let mut perms = fs::metadata(&script_path)
                .expect("stat temp script")
                .permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&script_path, perms).expect("chmod temp script");

            Self { dir, script_path }
        }

        fn new(name: &str, body: &str) -> Self {
            Self::with_dir(name, |_| body.to_string())
        }

        fn path(&self) -> &Path {
            &self.script_path
        }

        fn dir(&self) -> &Path {
            &self.dir
        }
    }

    impl Drop for TempScript {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    fn process_exists(pid: u32) -> bool {
        Path::new(&format!("/proc/{pid}")).exists()
    }

    #[test]
    fn spawn_happy_path_parses_addr_and_reaps_on_drop() {
        let script = TempScript::new(
            "happy",
            "#!/bin/sh\necho 'LISTENING 127.0.0.1:12345'\nexec sleep 60\n",
        );

        let server = TestServer::spawn(script.path()).expect("server should start cleanly");
        assert_eq!(
            server.addr(),
            "127.0.0.1:12345".parse::<SocketAddr>().unwrap()
        );

        let pid = server.child_id();
        assert!(process_exists(pid), "child process {pid} should be alive");

        drop(server);

        assert!(
            !process_exists(pid),
            "child process {pid} should be killed and reaped after drop"
        );
    }

    #[test]
    fn spawn_timeout_returns_timed_out_and_reaps_child() {
        let temp = TempScript::with_dir("timeout", |dir| {
            let pid_file = dir.join("child.pid");
            format!(
                "#!/bin/sh\necho $$ > '{}'\nexec sleep 60\n",
                pid_file.display()
            )
        });
        let pid_file = temp.dir().join("child.pid");

        let err = TestServer::spawn_with_timeout(temp.path(), Duration::from_millis(200))
            .err()
            .expect("spawn should time out");
        assert_eq!(
            err.kind(),
            io::ErrorKind::TimedOut,
            "expected TimedOut, got: {err}"
        );

        let contents = fs::read_to_string(&pid_file).expect("child should have written pidfile");
        let pid: u32 = contents.trim().parse().expect("valid child pid");
        assert!(
            !process_exists(pid),
            "timed-out child process {pid} should be killed and reaped"
        );
    }

    #[test]
    fn spawn_early_exit_includes_stderr_in_error() {
        let script = TempScript::new(
            "early-exit",
            "#!/bin/sh\necho 'fatal: address already in use' >&2\nexit 1\n",
        );

        let err = TestServer::spawn(script.path())
            .err()
            .expect("spawn should fail on early exit");
        let message = err.to_string();
        assert!(
            message.contains("fatal: address already in use"),
            "expected error message to include captured stderr, got: {message}"
        );
    }
}
