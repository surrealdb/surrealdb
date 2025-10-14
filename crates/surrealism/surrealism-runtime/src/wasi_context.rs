use std::sync::Arc;

use anyhow::Result;
use wasmtime_wasi::{p2::{StdoutStream, WasiCtxBuilder, OutputStream, StreamError, Pollable}, preview1::WasiP1Ctx};
use bytes::Bytes;
use async_trait::async_trait;

use crate::host::Host;

pub fn build(host: Arc<dyn Host>) -> Result<WasiP1Ctx> {
    let ctx = WasiCtxBuilder::new() // No stdin for now
        .stdout(HostStdout(host.clone()))
        .stderr(HostStderr(host.clone()))
        .inherit_env()
        .build_p1();

    Ok(ctx)
}

pub struct HostStdout(pub Arc<dyn Host>);
pub struct HostStderr(pub Arc<dyn Host>);

struct HostOutputStream(pub Arc<dyn Host>);
struct HostErrorStream(pub Arc<dyn Host>);

impl OutputStream for HostOutputStream {
    fn write(&mut self, bytes: Bytes) -> Result<(), StreamError> {
        let output = String::from_utf8_lossy(&bytes);
        self.0.stdout(&output).map_err(|e| StreamError::trap(&format!("Host stdout error: {e}")))?;
        Ok(())
    }
    fn flush(&mut self) -> Result<(), StreamError> {
        Ok(())
    }
    fn check_write(&mut self) -> Result<usize, StreamError> {
        Ok(1024 * 1024)
    }
}

impl OutputStream for HostErrorStream {
    fn write(&mut self, bytes: Bytes) -> Result<(), StreamError> {
        let output = String::from_utf8_lossy(&bytes);
        self.0.stderr(&output).map_err(|e| StreamError::trap(&format!("Host stderr error: {e}")))?;
        Ok(())
    }
    fn flush(&mut self) -> Result<(), StreamError> {
        Ok(())
    }
    fn check_write(&mut self) -> Result<usize, StreamError> {
        Ok(1024 * 1024)
    }
}

impl StdoutStream for HostStdout {
    fn stream(&self) -> Box<dyn OutputStream> {
        Box::new(HostOutputStream(self.0.clone()))
    }
    fn isatty(&self) -> bool {
        false
    }
}

impl StdoutStream for HostStderr {
    fn stream(&self) -> Box<dyn OutputStream> {
        Box::new(HostErrorStream(self.0.clone()))
    }
    fn isatty(&self) -> bool {
        false
    }
}

// Implement Pollable for HostOutputStream and HostErrorStream
#[async_trait]
impl Pollable for HostOutputStream {
    async fn ready(&mut self) {}
}

#[async_trait]
impl Pollable for HostErrorStream {
    async fn ready(&mut self) {}
}

