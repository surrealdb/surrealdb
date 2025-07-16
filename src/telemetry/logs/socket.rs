use std::io::{Result, Write};
use std::net::{TcpStream, ToSocketAddrs};

/// Simple writer that outputs log lines to a TCP socket.
pub struct SocketWriter {
    stream: TcpStream,
}

impl SocketWriter {
    /// Connect to the given socket address.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true).ok();
        Ok(Self { stream })
    }
}

impl Write for SocketWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.stream.write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        self.stream.flush()
    }
}
