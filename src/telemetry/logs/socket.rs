use std::io::{Result, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};

/// Simple writer that outputs log lines to a TCP socket.
pub struct Socket {
	stream: TcpStream,
}

/// Connect to the given socket address.
pub fn connect(addr: SocketAddr) -> Result<Socket> {
	Socket::connect(addr)
}

impl Socket {
	/// Connect to the given socket address.
	pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
		// Open a TCP stream to the given address
		let stream = TcpStream::connect(addr)?;
		// Ensure logs are sent immediately
		stream.set_nodelay(true).ok();
		// Return the socket writer
		Ok(Self {
			stream,
		})
	}
}

impl Write for Socket {
	fn write(&mut self, buf: &[u8]) -> Result<usize> {
		self.stream.write(buf)
	}

	fn flush(&mut self) -> Result<()> {
		self.stream.flush()
	}
}
