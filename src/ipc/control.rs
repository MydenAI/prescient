use socket2::{Domain, SockAddr, Socket, Type};
use std::io::{self, Read, Write};
use std::mem::MaybeUninit;
use std::net::Shutdown;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const MAX_ENDPOINT_NAME: usize = 20;
const RENDEZVOUS_RETRY: Duration = Duration::from_millis(1);

fn endpoint_path(name: &str) -> io::Result<PathBuf> {
    if name.is_empty()
        || name.len() > MAX_ENDPOINT_NAME
        || !name.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' || byte == b'.'
        })
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "IPC endpoint names must be 1..=20 ASCII letters, digits, dots, dashes, or underscores",
        ));
    }
    Ok(std::env::temp_dir().join(format!("prescient-{name}.sock")))
}

fn deadline(timeout: Duration) -> io::Result<Instant> {
    Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "IPC timeout is too large"))
}

pub(crate) fn accept_named(name: &str, timeout: Duration) -> io::Result<Control> {
    let listener = ControlListener::bind(endpoint_path(name)?)?;
    listener.set_nonblocking(true)?;
    let deadline = deadline(timeout)?;
    loop {
        match listener.accept() {
            Ok(control) => {
                // Accepted sockets inherit listener nonblocking state on macOS and Windows.
                // Setup framing is blocking; liveness switches its clones back afterward.
                control.set_nonblocking(false)?;
                return Ok(control);
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "timed out waiting for IPC peer",
                    ));
                }
                std::thread::sleep(RENDEZVOUS_RETRY);
            }
            Err(error) => return Err(error),
        }
    }
}

pub(crate) fn connect_named(name: &str, timeout: Duration) -> io::Result<Control> {
    let path = endpoint_path(name)?;
    let deadline = deadline(timeout)?;
    loop {
        match Control::connect(&path) {
            Ok(control) => return Ok(control),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound
                        | io::ErrorKind::ConnectionRefused
                        | io::ErrorKind::AddrNotAvailable
                ) =>
            {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "timed out waiting for IPC endpoint",
                    ));
                }
                std::thread::sleep(RENDEZVOUS_RETRY);
            }
            Err(error) => return Err(error),
        }
    }
}

/// A connected local socket for private IPC setup and peer liveness.
pub(crate) struct Control(Socket);

impl Control {
    /// Connect to a local `AF_UNIX` stream socket.
    fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
        let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        socket.connect(&SockAddr::unix(path)?)?;
        Ok(Self(socket))
    }

    /// Construct a connected control pair without a filesystem pathname.
    #[cfg(all(test, unix))]
    pub(crate) fn pair() -> io::Result<(Self, Self)> {
        Socket::pair(Domain::UNIX, Type::STREAM, None)
            .map(|(left, right)| (Self(left), Self(right)))
    }

    /// Construct a connected control pair through a private pathname.
    #[cfg(all(test, windows))]
    pub(crate) fn pair() -> io::Result<(Self, Self)> {
        use std::sync::atomic::{AtomicU64, Ordering};

        static NEXT_PATH: AtomicU64 = AtomicU64::new(0);
        let sequence = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("prescient-{}-{sequence}.sock", std::process::id()));
        let listener = ControlListener::bind(&path)?;
        let client = Self::connect(&path)?;
        let server = listener.accept()?;
        Ok((client, server))
    }

    pub(crate) fn try_clone(&self) -> io::Result<Self> {
        self.0.try_clone().map(Self)
    }

    pub(crate) fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.0.set_nonblocking(nonblocking)
    }

    pub(crate) fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.0.set_read_timeout(timeout)
    }

    pub(crate) fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.0.set_write_timeout(timeout)
    }

    pub(crate) fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.0.shutdown(how)
    }

    pub(crate) fn peek(&self, buffer: &mut [MaybeUninit<u8>]) -> io::Result<usize> {
        self.0.peek(buffer)
    }
}

impl Read for Control {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.0.read(buffer)
    }
}

impl Write for Control {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

/// A pathname-bound local listener for IPC control sessions.
///
/// Dropping the listener removes its socket pathname.
pub(crate) struct ControlListener {
    socket: Socket,
    path: std::path::PathBuf,
}

impl ControlListener {
    /// Bind and listen on an `AF_UNIX` socket pathname.
    fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_owned();
        let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        socket.bind(&SockAddr::unix(&path)?)?;
        socket.listen(128)?;
        Ok(Self { socket, path })
    }

    /// Accept one connected control session.
    fn accept(&self) -> io::Result<Control> {
        self.socket.accept().map(|(socket, _)| Control(socket))
    }

    /// Select blocking or nonblocking acceptance.
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.socket.set_nonblocking(nonblocking)
    }
}

impl Drop for ControlListener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn endpoint_names_are_bounded_and_path_safe() {
        for invalid in ["", "slash/name", "white space", "123456789012345678901"] {
            assert_eq!(
                endpoint_path(invalid).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
        assert!(endpoint_path("orders.v2_live-1").is_ok());
    }

    #[test]
    fn listener_drop_removes_socket_path() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "prescient-control-test-{}-{nonce}.sock",
            std::process::id()
        ));
        let listener = ControlListener::bind(&path).unwrap();
        assert!(path.exists());
        drop(listener);
        assert!(!path.exists());
    }
}
