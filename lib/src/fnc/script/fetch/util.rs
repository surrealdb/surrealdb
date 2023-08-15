use js::{Exception, Object, Result};

/// Returns wether the status code is an null body status
pub fn is_null_body_status(status: u16) -> bool {
	matches!(status, 101 | 103 | 204 | 205 | 304)
}

/// Returns wether the status code is an ok status
pub fn is_ok_status(status: u16) -> bool {
	(200..=299).contains(&status)
}

/// Returns wether the status code is an redirect status
pub fn is_redirect_status(status: u16) -> bool {
	[301, 302, 303, 307, 308].contains(&status)
}

/// Test whether a string matches the reason phrase http spec production.
pub fn is_reason_phrase(text: &str) -> bool {
	// Cannot be empty
	!text.is_empty()
		// all characters match VCHAR (0x21..=0x7E), obs-text (0x80..=0xFF), HTAB, or SP
		&& text.as_bytes().iter().all(|b| matches!(b,0x21..=0x7E | 0x80..=0xFF | b'\t' | b' '))
}

/// Returns the bytes from a buffer source javascript object if the object was one..
pub fn buffer_source_to_bytes<'a>(object: &'a Object) -> Result<Option<&'a [u8]>> {
	let ctx = object.ctx();

	if let Some(x) = object.as_typed_array::<i8>() {
		let bytes =
			x.as_bytes().ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
		return Ok(Some(bytes));
	}
	if let Some(x) = object.as_typed_array::<u8>() {
		let bytes =
			x.as_bytes().ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
		return Ok(Some(bytes));
	}
	if let Some(x) = object.as_typed_array::<i16>() {
		let bytes =
			x.as_bytes().ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
		return Ok(Some(bytes));
	}
	if let Some(x) = object.as_typed_array::<u16>() {
		let bytes =
			x.as_bytes().ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
		return Ok(Some(bytes));
	}
	if let Some(x) = object.as_typed_array::<i32>() {
		let bytes =
			x.as_bytes().ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
		return Ok(Some(bytes));
	}
	if let Some(x) = object.as_typed_array::<u32>() {
		let bytes =
			x.as_bytes().ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
		return Ok(Some(bytes));
	}
	if let Some(x) = object.as_typed_array::<i64>() {
		let bytes =
			x.as_bytes().ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
		return Ok(Some(bytes));
	}
	if let Some(x) = object.as_typed_array::<u64>() {
		let bytes =
			x.as_bytes().ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
		return Ok(Some(bytes));
	}
	if let Some(x) = object.as_array_buffer() {
		let bytes =
			x.as_bytes().ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
		return Ok(Some(bytes));
	}
	Ok(None)
}
