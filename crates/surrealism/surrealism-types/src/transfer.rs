use std::ops::{Deref, DerefMut};

use anyhow::Result;

use crate::controller::MemoryController;

pub trait Transfer {
	/// Transfers the value into WASM memory, returns a `Transferred` handle
	fn transfer(self, controller: &mut dyn MemoryController) -> Result<Ptr>;

	/// Default implementation of `accept`, does nothing unless overridden
	fn receive(ptr: Ptr, controller: &mut dyn MemoryController) -> Result<Self>
	where
		Self: Sized;
}

pub struct Ptr(u32);

impl Deref for Ptr {
	type Target = u32;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for Ptr {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl From<u32> for Ptr {
	fn from(ptr: u32) -> Self {
		Ptr(ptr)
	}
}

impl From<Ptr> for u32 {
	fn from(ptr: Ptr) -> Self {
		ptr.0
	}
}

impl TryFrom<i32> for Ptr {
	type Error = anyhow::Error;

	fn try_from(value: i32) -> Result<Self> {
		if value < 0 {
			Err(anyhow::anyhow!("Invalid pointer: {}", value))
		} else {
			Ok(Ptr(value as u32))
		}
	}
}
