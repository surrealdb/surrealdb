use anyhow::Result;
use surrealism_types::controller::MemoryController;

use crate::memory::{__sr_alloc, __sr_free};

/// Guest-side WASM memory controller that delegates to the `__sr_alloc`/`__sr_free` exports.
pub struct Controller {}

impl MemoryController for Controller {
	fn alloc(&mut self, len: u32) -> Result<u32> {
		let result = __sr_alloc(len);
		if result == 0 {
			anyhow::bail!("Memory allocation failed");
		}
		Ok(result)
	}

	fn free(&mut self, ptr: u32, len: u32) -> Result<()> {
		let result = __sr_free(ptr, len);
		if result == 0 {
			anyhow::bail!("Memory deallocation failed");
		}
		Ok(())
	}

	/// # Safety
	/// Relies on the caller providing a valid (ptr, len) within WASM linear memory.
	fn mut_mem(&mut self, ptr: u32, len: u32) -> &mut [u8] {
		unsafe {
			let ptr = ptr as usize as *mut u8;
			std::slice::from_raw_parts_mut(ptr, len as usize)
		}
	}
}
