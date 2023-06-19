use bytes::BytesMut;
use js::{bind, prelude::Coerced, ArrayBuffer, Class, Ctx, Exception, FromJs, Result, Value};

pub use blob::Blob as BlobClass;

#[derive(Clone, Copy)]
pub enum EndingType {
	Transparent,
	Native,
}

fn append_blob_part<'js>(
	ctx: Ctx<'js>,
	value: Value<'js>,
	ending: EndingType,
	data: &mut BytesMut,
) -> Result<()> {
	#[cfg(windows)]
	const LINE_ENDING: &[u8] = b"\r\n";
	#[cfg(not(windows))]
	const LINE_ENDING: &[u8] = b"\n";

	if let Some(object) = value.as_object() {
		if let Ok(x) = Class::<BlobClass>::from_object(object.clone()) {
			data.extend_from_slice(&x.borrow().data);
			return Ok(());
		}
		if let Ok(x) = ArrayBuffer::from_object(object.clone()) {
			data.extend_from_slice(x.as_bytes().ok_or_else(|| {
				Exception::throw_type(ctx, "Tried to construct blob with detached buffer")
			})?);
			return Ok(());
		}
	}
	let string = Coerced::<String>::from_js(ctx, value)?.0;
	if let EndingType::Transparent = ending {
		data.extend_from_slice(string.as_bytes());
	} else {
		data.reserve(string.len());
		let mut iter = string.as_bytes().iter().copied();
		// replace all line endings with native.
		while let Some(x) = iter.next() {
			if x == b'\r' {
				// \r\n
				data.extend(LINE_ENDING);
				if let Some(x) = iter.next() {
					if x != b'\n' {
						data.extend([x])
					}
				}
			}
			if x == b'\n' {
				// \n
				data.extend(LINE_ENDING);
			}
		}
	}
	Ok(())
}

// see https://w3c.github.io/FileAPI/#constructorBlob
fn normalize_type(mut ty: String) -> String {
	if ty.contains(|c| !('\u{0020}'..='\u{007E}').contains(&c)) {
		String::new()
	} else {
		ty.make_ascii_lowercase();
		ty
	}
}

#[bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
mod blob {
	use super::*;

	use bytes::{Bytes, BytesMut};
	use js::{
		function::{Opt, Rest},
		ArrayBuffer, Ctx, Exception, Object, Result, Value,
	};

	#[derive(Clone)]
	#[quickjs(cloneable)]
	pub struct Blob {
		pub(crate) mime: String,
		// TODO: make bytes?
		pub(crate) data: Bytes,
	}

	impl Blob {
		// ------------------------------
		// Constructor
		// ------------------------------

		#[quickjs(constructor)]
		pub fn new<'js>(
			ctx: Ctx<'js>,
			parts: Opt<Value<'js>>,
			options: Opt<Object<'js>>,
			_rest: Rest<()>,
		) -> Result<Self> {
			let mut r#type = String::new();
			let mut endings = EndingType::Transparent;

			if let Some(obj) = options.into_inner() {
				if let Some(x) = obj.get::<_, Option<Coerced<String>>>("type")? {
					r#type = normalize_type(x.to_string());
				}
				if let Some(Coerced(x)) = obj.get::<_, Option<Coerced<String>>>("endings")? {
					if x == "native" {
						endings = EndingType::Native;
					} else if x != "transparent" {
						return Err(Exception::throw_type(
							ctx,
							",expected endings to be either 'transparent' or 'native'",
						));
					}
				}
			}

			let data = if let Some(parts) = parts.into_inner() {
				let array = parts
					.into_array()
					.ok_or_else(|| Exception::throw_type(ctx, "Blob parts are not a sequence"))?;

				let mut buffer = BytesMut::new();

				for elem in array.iter::<Value>() {
					let elem = elem?;
					append_blob_part(ctx, elem, endings, &mut buffer)?;
				}
				buffer.freeze()
			} else {
				Bytes::new()
			};
			Ok(Self {
				mime: r#type,
				data,
			})
		}

		// ------------------------------
		// Instance properties
		// ------------------------------

		#[quickjs(get)]
		pub fn size(&self) -> usize {
			self.data.len()
		}

		#[quickjs(get)]
		pub fn r#type(&self) -> String {
			self.mime.clone()
		}

		pub fn slice(
			&self,
			start: Opt<isize>,
			end: Opt<isize>,
			content_type: Opt<String>,
			_rest: Rest<()>,
		) -> Blob {
			// see https://w3c.github.io/FileAPI/#slice-blob
			let start = start.into_inner().unwrap_or_default();
			let start = (self.data.len() as isize + start).max(0) as usize;
			let end = end.into_inner().unwrap_or_default();
			let end = (self.data.len() as isize + end).max(0) as usize;
			let data = self.data.slice(start..end);
			let content_type = content_type.into_inner().map(normalize_type).unwrap_or_default();
			Blob {
				mime: content_type,
				data,
			}
		}

		pub async fn text(&self) -> Result<String> {
			let text = String::from_utf8(self.data.to_vec())?;
			Ok(text)
		}

		pub async fn arrayBuffer<'js>(&self, ctx: Ctx<'js>) -> Result<ArrayBuffer<'js>> {
			ArrayBuffer::new(ctx, self.data.to_vec())
		}

		// ------------------------------
		// Instance methods
		// ------------------------------

		// Convert the object to a string
		pub fn toString(&self) -> String {
			String::from("[object Blob]")
		}
	}
}
