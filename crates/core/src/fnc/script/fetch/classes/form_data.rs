//! FormData class implementation

use std::collections::HashMap;
use std::string::String as StdString;

use js::class::{Class, Trace};
use js::function::{Opt, Rest};
use js::prelude::Coerced;
use js::{Ctx, Exception, FromJs, JsLifetime, Result, String, Value};
use reqwest::multipart::{Form, Part};

use crate::fnc::script::fetch::classes::Blob;

#[derive(Clone, JsLifetime)]
pub enum FormDataValue<'js> {
	String(String<'js>),
	Blob {
		data: Class<'js, Blob>,
		filename: Option<String<'js>>,
	},
}

impl<'js> FormDataValue<'js> {
	fn from_arguments(
		ctx: &Ctx<'js>,
		value: Value<'js>,
		filename: Opt<Coerced<String<'js>>>,
		error: &'static str,
	) -> Result<Self> {
		if let Some(blob) = value.as_object().and_then(Class::<Blob>::from_object) {
			let filename = filename.into_inner().map(|x| x.0);

			Ok(FormDataValue::Blob {
				data: blob,
				filename,
			})
		} else if filename.into_inner().is_some() {
			return Err(Exception::throw_type(ctx, error));
		} else {
			let value = Coerced::<String>::from_js(ctx, value)?;
			Ok(FormDataValue::String(value.0))
		}
	}
}

#[js::class]
#[derive(Clone, Trace, JsLifetime)]
pub struct FormData<'js> {
	#[qjs(skip_trace)]
	pub(crate) values: HashMap<StdString, Vec<FormDataValue<'js>>>,
}

#[js::methods]
impl<'js> FormData<'js> {
	// ------------------------------
	// Constructor
	// ------------------------------

	// FormData spec states that FormDa takes two html elements as arguments
	// which does not make sense implementing fetch outside a browser.
	// So we ignore those arguments.
	#[qjs(constructor)]
	pub fn new(ctx: Ctx<'js>, args: Rest<()>) -> Result<Self> {
		if !args.is_empty() {
			return Err(Exception::throw_internal(
				&ctx,
				"Cant call FormData with arguments as the dom elements required are not available",
			));
		}
		Ok(FormData {
			values: HashMap::new(),
		})
	}

	pub fn append(
		&mut self,
		ctx: Ctx<'js>,
		name: Coerced<StdString>,
		value: Value<'js>,
		filename: Opt<Coerced<String<'js>>>,
	) -> Result<()> {
		let value = FormDataValue::from_arguments(
			&ctx,
			value,
			filename,
			"Can't call `append` on `FormData` with a filename when value isn't of type `Blob`",
		)?;

		self.values.entry(name.0).or_default().push(value);

		Ok(())
	}

	pub fn set(
		&mut self,
		ctx: Ctx<'js>,
		name: Coerced<StdString>,
		value: Value<'js>,
		filename: Opt<Coerced<String<'js>>>,
	) -> Result<()> {
		let value = FormDataValue::from_arguments(
			&ctx,
			value,
			filename,
			"Can't call `set` on `FormData` with a filename when value isn't of type `Blob`",
		)?;

		self.values.insert(name.0, vec![value]);

		Ok(())
	}

	pub fn has(&self, name: Coerced<StdString>) -> bool {
		self.values.contains_key(&name.0)
	}

	pub fn delete(&mut self, name: Coerced<StdString>) {
		self.values.remove(&name.0);
	}

	#[qjs(skip)]
	pub fn to_form(&self) -> Result<Form> {
		let mut res = Form::new();
		for (k, v) in self.values.iter() {
			for v in v {
				match v {
					FormDataValue::String(x) => {
						res = res.text(k.clone(), x.to_string()?);
					}
					FormDataValue::Blob {
						data,
						filename,
					} => {
						let mut part = Part::bytes(data.borrow().data.to_vec());
						if let Some(filename) = filename {
							let filename = filename.to_string()?;
							part = part.file_name(filename);
						}
						res = res.part(k.clone(), part);
					}
				}
			}
		}
		Ok(res)
	}
}
