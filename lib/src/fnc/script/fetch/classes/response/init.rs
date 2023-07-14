use std::string::String as StdString;

use js::{
	class::{HasRefs, RefsMarker},
	prelude::*,
	Class, Ctx, Exception, FromJs, Object, Persistent, Result, Value,
};

use crate::fnc::script::fetch::{classes::HeadersClass, util};

/// Struct containing data from the init argument from the Response constructor.
#[derive(Clone)]
pub struct ResponseInit {
	// u16 instead of reqwest::StatusCode since javascript allows non valid status codes in some
	// circumstances.
	pub status: u16,
	pub status_text: StdString,
	pub headers: Persistent<Class<'static, HeadersClass>>,
}

impl HasRefs for ResponseInit {
	fn mark_refs(&self, marker: &RefsMarker) {
		self.headers.mark_refs(marker);
	}
}

impl ResponseInit {
	/// Returns a ResponseInit object with all values as the default value.
	pub fn default(ctx: Ctx<'_>) -> Result<ResponseInit> {
		let headers = Class::instance(ctx, HeadersClass::new_empty())?;
		let headers = Persistent::save(ctx, headers);
		Ok(ResponseInit {
			status: 200,
			status_text: StdString::new(),
			headers,
		})
	}
}

impl<'js> FromJs<'js> for ResponseInit {
	fn from_js(ctx: Ctx<'js>, value: Value<'js>) -> Result<Self> {
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
			let headers = HeadersClass::new_inner(ctx, headers)?;
			Class::instance(ctx, headers)?
		} else {
			Class::instance(ctx, HeadersClass::new_empty())?
		};
		let headers = Persistent::save(ctx, headers);

		Ok(ResponseInit {
			status,
			status_text,
			headers,
		})
	}
}
