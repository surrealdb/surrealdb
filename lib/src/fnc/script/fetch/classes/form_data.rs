//! FormData class implementation

use js::{
	bind, function::Opt, prelude::Coerced, Class, Ctx, Exception, FromJs, Persistent, Result,
	String, Value,
};
use std::string::String as StdString;

use crate::fnc::script::fetch::classes::BlobClass;

#[derive(Clone)]
pub enum FormDataValue {
	String(Persistent<String<'static>>),
	Blob {
		data: Persistent<Class<'static, BlobClass>>,
		filename: Option<Persistent<String<'static>>>,
	},
}

impl FormDataValue {
	fn from_arguments<'js>(
		ctx: Ctx<'js>,
		value: Value<'js>,
		filename: Opt<Coerced<String<'js>>>,
		error: &'static str,
	) -> Result<FormDataValue> {
		if let Some(blob) =
			value.as_object().and_then(|value| Class::<BlobClass>::from_object(value.clone()).ok())
		{
			let blob = Persistent::save(ctx, blob);
			let filename = filename.into_inner().map(|x| Persistent::save(ctx, x.0));

			Ok(FormDataValue::Blob {
				data: blob,
				filename,
			})
		} else if filename.into_inner().is_some() {
			return Err(Exception::throw_type(ctx, error));
		} else {
			let value = Coerced::<String>::from_js(ctx, value)?;
			let value = Persistent::save(ctx, value.0);
			Ok(FormDataValue::String(value))
		}
	}
}

pub use form_data::FormData as FormDataClass;
#[bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod form_data {
	use super::*;
	use std::{cell::RefCell, collections::HashMap};

	use js::{
		function::Opt,
		prelude::{Coerced, Rest},
		Ctx, Result, String, Value,
	};
	use reqwest::multipart::{Form, Part};

	#[derive(Clone)]
	#[quickjs(cloneable)]
	pub struct FormData {
		pub(crate) values: RefCell<HashMap<StdString, Vec<FormDataValue>>>,
	}

	impl FormData {
		// ------------------------------
		// Constructor
		// ------------------------------

		// FormData spec states that FormDa takes two html elements as arguments
		// which does not make sense implementing fetch outside a browser.
		// So we ignore those arguments.
		#[quickjs(constructor)]
		pub fn new(ctx: Ctx<'_>, args: Rest<()>) -> Result<FormData> {
			if args.len() > 0 {
				return Err(Exception::throw_internal(ctx,"Cant call FormData with arguments as the dom elements required are not available"));
			}
			Ok(FormData {
				values: RefCell::new(HashMap::new()),
			})
		}

		pub fn append<'js>(
			&self,
			ctx: Ctx<'js>,
			name: Coerced<StdString>,
			value: Value<'js>,
			filename: Opt<Coerced<String<'js>>>,
		) -> Result<()> {
			let value = FormDataValue::from_arguments(
				ctx,
				value,
				filename,
				"Can't call `append` on `FormData` with a filename when value isn't of type `Blob`",
			)?;

			self.values.borrow_mut().entry(name.0).or_insert_with(Vec::new).push(value);

			Ok(())
		}

		pub fn set<'js>(
			&self,
			ctx: Ctx<'js>,
			name: Coerced<StdString>,
			value: Value<'js>,
			filename: Opt<Coerced<String<'js>>>,
		) -> Result<()> {
			let value = FormDataValue::from_arguments(
				ctx,
				value,
				filename,
				"Can't call `set` on `FormData` with a filename when value isn't of type `Blob`",
			)?;

			self.values.borrow_mut().insert(name.0, vec![value]);

			Ok(())
		}

		pub fn has(&self, ctx: Ctx<'_>, name: Coerced<StdString>) -> bool {
			self.values.borrow().contains_key(&name.0)
		}

		pub fn delete(&self, ctx: Ctx<'_>, name: Coerced<StdString>) {
			self.values.borrow_mut().remove(&name.0);
		}

		#[quickjs(skip)]
		pub fn to_form(&self, ctx: Ctx<'_>) -> Result<Form> {
			let lock = self.values.borrow();
			let mut res = Form::new();
			for (k, v) in lock.iter() {
				for v in v {
					match v {
						FormDataValue::String(x) => {
							let x = x.clone().restore(ctx).unwrap();
							res = res.text(k.clone(), x.to_string()?);
						}
						FormDataValue::Blob {
							data,
							filename,
						} => {
							let mut part = Part::bytes(
								data.clone().restore(ctx).unwrap().borrow().data.to_vec(),
							);
							if let Some(filename) = filename {
								let filename =
									filename.clone().restore(ctx).unwrap().to_string()?;
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
}
