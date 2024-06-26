/// LoggingLifecycle is used to create log messages upon creation, and log messages when it is dropped
#[doc(hidden)]
#[non_exhaustive]
pub struct LoggingLifecycle {
	identifier: String,
}

impl LoggingLifecycle {
	#[doc(hidden)]
	pub fn new(identifier: String) -> Self {
		debug!("Started {}", identifier);
		Self {
			identifier,
		}
	}
}

impl Drop for LoggingLifecycle {
	fn drop(&mut self) {
		debug!("Stopped {}", self.identifier);
	}
}
