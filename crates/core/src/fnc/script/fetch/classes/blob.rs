//! Blob class implementation

use bytes::{Bytes, BytesMut};
use js::class::Trace;
use js::prelude::{Coerced, Opt};
use js::{ArrayBuffer, Class, Ctx, Exception, FromJs, JsLifetime, Object, Result, Value};

#[derive(Clone, Copy)]
pub enum EndingType {
	Transparent,
	Native,
}

fn append_blob_part<'js>(
	ctx: &Ctx<'js>,
	value: Value<'js>,
	ending: EndingType,
	data: &mut BytesMut,
) -> Result<()> {
	#[cfg(windows)]
	const LINE_ENDING: &[u8] = b"\r\n";
	#[cfg(not(windows))]
	const LINE_ENDING: &[u8] = b"\n";

	if let Some(object) = value.as_object() {
		if let Some(x) = Class::<Blob>::from_object(object) {
			data.extend_from_slice(&x.borrow().data);
			return Ok(());
		}
		if let Some(x) = ArrayBuffer::from_object(object.clone()) {
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
				if let Some(x) = iter.next() {
					if x != b'\n' {
						data.extend([b'\r', x])
					} else {
						data.extend(LINE_ENDING);
					}
				} else {
					data.extend([b'\r'])
				}
			} else if x == b'\n' {
				// \n
				data.extend(LINE_ENDING);
			} else {
				data.extend([x])
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

#[derive(Clone, Trace, JsLifetime)]
#[js::class]
pub struct Blob {
	pub(crate) mime: String,
	// TODO: make bytes?
	#[qjs(skip_trace)]
	pub(crate) data: Bytes,
}

#[js::methods]
impl Blob {
	// ------------------------------
	// Constructor
	// ------------------------------

	#[qjs(constructor)]
	pub fn new<'js>(
		ctx: Ctx<'js>,
		parts: Opt<Value<'js>>,
		options: Opt<Object<'js>>,
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
						&ctx,
						",expected endings to be either 'transparent' or 'native'",
					));
				}
			}
		}

		let data = if let Some(parts) = parts.into_inner() {
			let array = parts
				.into_array()
				.ok_or_else(|| Exception::throw_type(&ctx, "Blob parts are not a sequence"))?;

			let mut buffer = BytesMut::new();

			for elem in array.iter::<Value>() {
				let elem = elem?;
				append_blob_part(&ctx, elem, endings, &mut buffer)?;
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

	#[qjs(get)]
	pub fn size(&self) -> usize {
		self.data.len()
	}

	#[qjs(get, rename = "type")]
	pub fn r#type(&self) -> String {
		self.mime.clone()
	}

	pub fn slice(&self, start: Opt<isize>, end: Opt<isize>, content_type: Opt<String>) -> Blob {
		// see https://w3c.github.io/FileAPI/#slice-blob
		let start = start.into_inner().unwrap_or_default();
		let start = if start < 0 {
			(self.data.len() as isize + start).max(0) as usize
		} else {
			start as usize
		};
		let end = end.into_inner().unwrap_or_default();
		let end = if end < 0 {
			(self.data.len() as isize + end).max(0) as usize
		} else {
			end as usize
		};
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

	#[qjs(rename = "arrayBuffer")]
	pub async fn array_buffer<'js>(&self, ctx: Ctx<'js>) -> Result<ArrayBuffer<'js>> {
		ArrayBuffer::new(ctx, self.data.to_vec())
	}

	// ------------------------------
	// Instance methods
	// ------------------------------

	// Convert the object to a string
	#[qjs(rename = "toString")]
	pub fn js_to_string(&self) -> String {
		String::from("[object Blob]")
	}
}

#[cfg(test)]
mod test {
	use js::CatchResultExt;
	use js::promise::Promise;

	use crate::fnc::script::fetch::test::create_test_context;

	#[tokio::test]
	async fn basic_blob_use() {
		create_test_context!(ctx => {
			#[cfg(windows)]
			const NATIVE_LINE_ENDING: &str = "\r\n";
			#[cfg(not(windows))]
			const NATIVE_LINE_ENDING: &str = "\n";

			ctx.globals().set("NATIVE_LINE_ENDING",NATIVE_LINE_ENDING).unwrap();
			ctx.eval::<Promise,_>(r#"(async () => {
				let blob = new Blob();
				assert.eq(blob.size,0);
				assert.eq(blob.type,"");

				blob = new Blob(["text"],{type: "some-text"});
				assert.eq(blob.size,4);
				assert.eq(blob.type,"some-text");
				assert.eq(await blob.text(),"text");
				assert.eq(await blob.slice(2,4).text(),"xt");

				blob = new Blob(["\n\r\n \n\r"],{endings: "transparent"});
				assert.eq(blob.size,6)
					assert.eq(await blob.text(),"\n\r\n \n\r");
				blob = new Blob(["\n\r\n \n\r"],{endings: "native"});
				// \n \r\n and the \n from \n\r are converted.
				// the part of the string which isn't converted is the space and the \r
				assert.eq(await blob.text(),`${NATIVE_LINE_ENDING}${NATIVE_LINE_ENDING} ${NATIVE_LINE_ENDING}\r`);
				assert.eq(blob.size,NATIVE_LINE_ENDING.length*3 + 2)

					assert.mustThrow(() => new Blob("text"));
				assert.mustThrow(() => new Blob(["text"], {endings: "invalid value"}));
			})()
			"#).catch(&ctx).unwrap().into_future::<()>().await.catch(&ctx).unwrap();
		})
		.await
	}
}
