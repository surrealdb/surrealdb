use std::collections::BTreeMap;

use crate::sql::{self, subquery::Subquery, Value as SurValue};

use js::{
	class::Trace,
	prelude::{Coerced, Opt},
	Array, Ctx, Exception, FromJs, Result, Value,
};

#[js::class]
#[derive(Trace, Clone)]
#[non_exhaustive]
pub struct Query {
	#[qjs(skip_trace)]
	pub(crate) query: Subquery,
	#[qjs(skip_trace)]
	pub(crate) vars: Option<BTreeMap<String, SurValue>>,
}

#[derive(Default, Clone)]
#[non_exhaustive]
pub struct QueryVariables(pub BTreeMap<String, SurValue>);

impl QueryVariables {
	pub fn new() -> Self {
		QueryVariables(BTreeMap::new())
	}

	pub fn from_value<'js>(ctx: &Ctx<'js>, val: Value<'js>) -> Result<Self> {
		static INVALID_ERROR: &str = "Query argument was neither sequence<[String,SurValue]> or record<ByteString, SurValue>";
		let mut res = Self::new();

		// TODO Set and Map,
		if let Some(array) = val.as_array() {
			// a sequence<sequence<String>>;
			for v in array.iter::<Array>() {
				let v = match v {
					Ok(x) => x,
					Err(e) => {
						if e.is_from_js() {
							return Err(Exception::throw_type(ctx, INVALID_ERROR));
						}
						return Err(e);
					}
				};
				let key = match v.get::<Coerced<String>>(0) {
					Ok(x) => x,
					Err(e) => {
						if e.is_from_js() {
							return Err(Exception::throw_type(ctx, INVALID_ERROR));
						}
						return Err(e);
					}
				};
				let value = match v.get::<SurValue>(1) {
					Ok(x) => x,
					Err(e) => {
						if e.is_from_js() {
							return Err(Exception::throw_type(ctx, INVALID_ERROR));
						}
						return Err(e);
					}
				};
				res.0.insert(key.0, value);
			}
		} else if let Some(obj) = val.as_object() {
			// a record<String,String>;
			for prop in obj.props::<String, SurValue>() {
				let (key, value) = match prop {
					Ok(x) => x,
					Err(e) => {
						if e.is_from_js() {
							return Err(Exception::throw_type(ctx, INVALID_ERROR));
						}
						return Err(e);
					}
				};
				res.0.insert(key, value);
			}
		} else {
			return Err(Exception::throw_type(ctx, INVALID_ERROR));
		}

		Ok(res)
	}
}

impl<'js> FromJs<'js> for QueryVariables {
	fn from_js(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
		QueryVariables::from_value(ctx, value)
	}
}

#[js::methods]
impl Query {
	#[qjs(constructor)]
	pub fn new(ctx: Ctx<'_>, text: String, variables: Opt<QueryVariables>) -> Result<Self> {
		let query = sql::subquery(&text).map_err(|e| {
			let error_text = format!("{}", e);
			Exception::throw_type(&ctx, &error_text)
		})?;
		let vars = variables.into_inner().map(|x| x.0);
		Ok(Query {
			query,
			vars,
		})
	}

	#[qjs(rename = "toString")]
	pub fn js_to_string(&self) -> String {
		format!("{}", self.query)
	}

	pub fn bind(&mut self, key: Coerced<String>, value: SurValue) {
		self.vars.get_or_insert_with(BTreeMap::new).insert(key.0, value);
	}
}
