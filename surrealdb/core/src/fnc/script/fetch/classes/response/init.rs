use std::string::String as StdString;

use js::class::{Trace, Tracer};
use js::{Class, Coerced, Ctx, Exception, FromJs, JsLifetime, Object, Result, Value};

use crate::fnc::script::fetch::classes::Headers;
use crate::fnc::script::fetch::util;

/// Struct containing data from the init argument from the Response constructor.
#[derive(Clone, JsLifetime)]
pub struct ResponseInit<'js> {
	// u16 instead of reqwest::StatusCode since javascript allows non valid status codes in some
	// circumstances.
	pub status: u16,
	pub status_text: StdString,
	pub headers: Class<'js, Headers>,
}

impl<'js> Trace<'js> for ResponseInit<'js> {
	fn trace<'a>(&self, tracer: Tracer<'a, 'js>) {
		self.headers.trace(tracer);
	}
}

impl<'js> ResponseInit<'js> {
	/// Returns a ResponseInit object with all values as the default value.
	pub fn default(ctx: Ctx<'js>) -> Result<Self> {
		let headers = Class::instance(ctx, Headers::new_empty())?;
		Ok(ResponseInit {
			status: 200,
			status_text: StdString::new(),
			headers,
		})
	}
}

impl<'js> FromJs<'js> for ResponseInit<'js> {
	fn from_js(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
		let object = Object::from_js(ctx, value)?;

		// Extract status.
		let status =
			if let Some(Coerced(status)) = object.get::<_, Option<Coerced<i32>>>("status")? {
				if !(200..=599).contains(&status) {
					return Err(Exception::throw_range(ctx, "response status code outside range"));
				}
				status as u16
			} else {
				200u16
			};

		// Extract status text.
		let status_text = if let Some(Coerced(string)) =
			object.get::<_, Option<Coerced<StdString>>>("statusText")?
		{
			if !util::is_reason_phrase(string.as_str()) {
				return Err(Exception::throw_type(ctx, "statusText was not a valid reason phrase"));
			}
			string
		} else {
			StdString::new()
		};

		// Extract headers.
		let headers = if let Some(headers) = object.get::<_, Option<Value>>("headers")? {
			let headers = Headers::new_inner(ctx, headers)?;
			Class::instance(ctx.clone(), headers)?
		} else {
			Class::instance(ctx.clone(), Headers::new_empty())?
		};

		Ok(ResponseInit {
			status,
			status_text,
			headers,
		})
	}
}
