#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(unused_variables)]
#[allow(non_snake_case)]
pub mod blob {

	use js::Rest;

	use super::{BlobArray, BlobOptions};

	#[derive(Clone)]
	#[quickjs(class)]
	pub struct Blob {
		#[quickjs(hide)]
		pub(crate) mime: String,
		#[quickjs(hide)]
		pub(crate) data: Vec<u8>,
	}

	impl Blob {
		#[quickjs(constructor)]
		pub fn new(data: BlobArray, mut args: Rest<BlobOptions>) -> Self {
			let mut blob = Blob {
				data: data.0,
				mime: String::new(),
			};
			if let Some(options) = args.pop() {
				if let Some(mime) = options.r#type {
					blob.mime = mime;
				}
			}
			blob
		}
		#[quickjs(get)]
		pub fn size(&self) -> usize {
			self.data.len()
		}
		#[quickjs(get)]
		pub fn r#type(&self) -> &str {
			&self.mime
		}

		// Convert the object to a string
		pub fn toString(&self) -> String {
			String::from("[object Blob]")
		}
	}
}

use std::str::FromStr;

use surf::http::Mime;

use crate::fnc::script::util::is_typed_array;

#[derive(Default, Clone)]
pub struct BlobOptions {
	r#type: Option<String>,
	endings: Option<String>,
}

impl<'js> js::FromJs<'js> for BlobOptions {
	fn from_js(_ctx: js::Ctx<'js>, value: js::Value<'js>) -> js::Result<Self> {
		let mut options = BlobOptions::default();
		if value.is_object() {
			let object = value.into_object().unwrap();
			let mime_str = object.get::<_, String>("type")?;
			if !mime_str.is_empty() {
				let _ = Mime::from_str(&mime_str).map_err(|e| throw_js_exception!(e))?;
				options.r#type = Some(mime_str);
			}
			let endings = object.get::<_, String>("endings")?;
			if endings.to_ascii_lowercase() == "native" {
				options.endings = Some("native".to_owned());
			}
			if endings.to_ascii_lowercase() == "transparent" {
				options.endings = Some("transparent".to_owned());
			}
		}
		Ok(options)
	}
}

pub struct BlobArray(Vec<u8>);

impl<'js> js::FromJs<'js> for BlobArray {
	fn from_js(ctx: js::Ctx<'js>, value: js::Value<'js>) -> js::Result<Self> {
		if value.is_array() {
			let arr = value.into_array().unwrap();
			let data = arr
				.into_iter()
				.map(|v| -> js::Result<Vec<u8>> {
					if let Ok(body) = v {
						if body.is_string() {
							if let Some(js_str) = body.as_string() {
								return Ok(js_str.to_string()?.as_bytes().to_vec());
							}
						}
						if body.is_object() {
							if let Some(body) = body.into_object() {
								let array_buffer: js::Object = ctx.globals().get("ArrayBuffer")?;
								// from arrayBuffer
								if body.is_instance_of(array_buffer) {
									let js_ab = js::ArrayBuffer::from_object(body.clone())?;
									let buf: &[u8] = js_ab.as_ref();
									return Ok(buf.to_vec());
								}

								// from typedArray
								if is_typed_array(ctx.clone(), body.clone())? {
									// typedArray
									let js_ab: js::ArrayBuffer = body.get("buffer")?;
									let buf: &[u8] = js_ab.as_ref();
									return Ok(buf.to_vec());
								}
								// from blob
								if body.clone().instance_of::<blob::Blob>() {
									let v = body.into_instance::<blob::Blob>().unwrap();
									let v: &blob::Blob = v.as_ref();
									return Ok(v.data.clone());
								}
							}
						}
					}
					return Ok(vec![]);
				})
				.fold(vec![], |mut acc, curr| {
					if let Ok(buf) = curr {
						acc.append(&mut buf.to_vec());
					}
					acc
				});
			return Ok(BlobArray(data));
		}
		Ok(BlobArray(vec![]))
	}
}
