use std::borrow::Cow;
use std::slice::Iter;

use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use super::paths::ID;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Expr, FlowResultExt as _, Function, Idiom, Part};
use crate::fnc::args::FromArgs;
use crate::syn;
use crate::val::{Array, Value};

/// The `foo,bar,*` part of statements like `SELECT foo,bar.* FROM faz`.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum Fields {
	/// Fields had the `VALUE` clause and should only return the given selector
	///
	/// This variant should not contain Field::All
	/// TODO: Encode the above variant into the type.
	Value(Box<Selector>),
	/// Normal fields where an object with the selected fields is expected
	Select(Vec<Field>),
}

impl ToSql for Fields {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let fields: crate::sql::field::Fields = self.clone().into();
		fields.fmt_sql(f, fmt);
	}
}

impl Fields {
	/// Returns true if computing this value can be done on a read only
	/// transaction.
	pub fn read_only(&self) -> bool {
		match self {
			Fields::Value(field) => field.read_only(),
			Fields::Select(fields) => fields.iter().all(|x| x.read_only()),
		}
	}

	/// Create a new `*` field projection
	pub fn all() -> Self {
		Fields::Select(vec![Field::All])
	}

	/// Check to see if this field is a `*` projection
	pub fn has_all_selection(&self) -> bool {
		match self {
			Fields::Select(x) => x.iter().any(|x| matches!(x, Field::All)),
			Fields::Value(_) => false,
		}
	}
	/// Create a new `VALUE id` field projection
	pub(crate) fn value_id() -> Self {
		Fields::Value(Box::new(Selector {
			expr: Expr::Idiom(Idiom(ID.to_vec())),
			alias: None,
		}))
	}

	/// Returns an iterator which returns all fields which are not `Field::All`.
	pub(crate) fn iter_non_all_fields(&self) -> FieldsIter<'_> {
		match self {
			Fields::Value(selector) => FieldsIter::Single(Some(selector)),
			Fields::Select(fields) => FieldsIter::Multiple(fields.iter()),
		}
	}

	/// Check to see if this field is a single VALUE clause
	pub fn is_single(&self) -> bool {
		matches!(self, Fields::Value(_))
	}
	/// Check if the fields are only about counting
	pub(crate) fn is_count_all_only(&self) -> bool {
		fn field_is_count(f: &Field) -> bool {
			match f {
				Field::All => false,
				Field::Single(selector) => selector_is_count(selector),
			}
		}
		fn selector_is_count(f: &Selector) -> bool {
			let Expr::FunctionCall(x) = &f.expr else {
				return false;
			};
			if !x.arguments.is_empty() {
				return false;
			}
			let Function::Normal(name) = &x.receiver else {
				return false;
			};
			name == "count"
		}

		match self {
			Fields::Value(field) => selector_is_count(field),
			Fields::Select(fields) => !fields.is_empty() && fields.iter().all(field_is_count),
		}
	}

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		if let Some(doc) = doc {
			self.compute_value(stk, ctx, opt, doc).await
		} else {
			let doc = Value::None.into();
			self.compute_value(stk, ctx, opt, &doc).await
		}
	}

	async fn compute_value(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: &CursorDoc,
	) -> Result<Value> {
		// Process the desired output

		// TODO: This makes it so that with selection `SELECT 1 as foo,*,bar` if `foo`
		// is in the document it will be overwritten with 1. It might be slightly more
		// usefull to have the ordering matter and make `1 as foo,*` provide the foo
		// from the document and have `*, 1 as foo` provide the overwritten foo.
		let mut out = if self.has_all_selection() {
			doc.doc.as_ref().clone()
		} else {
			Value::empty_object()
		};

		for v in self.iter_non_all_fields() {
			let name = v
				.alias
				.as_ref()
				.map(Cow::Borrowed)
				.unwrap_or_else(|| Cow::Owned(v.expr.to_idiom()));
			match &v.expr {
				// This expression is a multi-output graph traversal
				Expr::Idiom(i) if i.is_multi_yield() => {
					// Store the different output yields here
					let mut res: Vec<(&[Part], Value)> = Vec::new();
					// Split the expression by each output alias
					for v in i.split_inclusive(Idiom::part_is_multi_yield) {
						// Use the last fetched value for each fetch
						let x = match res.last() {
							Some((_, r)) => r,
							None => doc.doc.as_ref(),
						};
						// Continue fetching the next idiom part
						let x = x
							.get(stk, ctx, opt, Some(doc), v)
							.await
							.catch_return()?
							// TODO: Controlflow winding up to here has some strange
							// implications, check validity.
							.flatten();
						// Add the result to the temporary store
						res.push((v, x));
					}
					// Assign each fetched yield to the output
					for (p, x) in res {
						match p.last().expect("idiom is non-empty").alias() {
							// This is an alias expression part
							Some(a) => {
								if let Some(i) = &v.alias {
									out.set(stk, ctx, opt, &i.0, x.clone()).await?;
								}
								out.set(stk, ctx, opt, a, x).await?;
							}
							// This is the end of the expression
							None => {
								out.set(stk, ctx, opt, v.alias.as_ref().unwrap_or(i), x).await?
							}
						}
					}
				}
				// TODO: This section should not be handled here, this should be catched by
				// an analysis pass and optimized.
				Expr::FunctionCall(f) => {
					// functions 'type::fields' and 'type::field' are specially handled
					// here as they don't just return a result but also set fields on
					// the document, so `type::field("foo")` results in `{ foo: "value"
					// }` instead of `{ ["type::field('foo')"]: "value" }`
					match f.receiver {
						Function::Normal(ref x) if x == "type::fields" => {
							// Some manual reimplemenation of type::fields to make it
							// more efficient.
							let mut arguments = Vec::new();
							for arg in f.arguments.iter() {
								arguments.push(
									stk.run(|stk| arg.compute(stk, ctx, opt, Some(doc)))
										.await
										.catch_return()?,
								);
							}

							// replicate the same error that would happen with normal
							// function calls
							let (args,) = <(Vec<String>,)>::from_args("type::fields", arguments)?;

							// manually do the implementation of type::fields
							let mut idioms = Vec::<Idiom>::new();
							for arg in args {
								idioms.push(syn::idiom(&arg)?.into())
							}

							let mut idiom_results = Vec::new();
							for idiom in idioms.iter() {
								let res =
									idiom.compute(stk, ctx, opt, Some(doc)).await.catch_return()?;
								idiom_results.push(res);
							}
							// Check if this is a single VALUE field expression
							if self.is_single() {
								out = Value::Array(Array(idiom_results));
							} else {
								// TODO: Alias is ignored here, figure out the right
								// behaviour. Maybe make an alias result in sub fields?
								// `select type::fields(["foo","faz"]) as bar` resulting
								// in `{ "bar": { foo: value, faz: value} }`?
								for (idiom, idiom_res) in
									idioms.iter().zip(idiom_results.into_iter())
								{
									out.set(stk, ctx, opt, &idiom.0, idiom_res).await?;
								}
							}
						}
						Function::Normal(ref x) if x == "type::field" => {
							// Some manual reimplemenation of type::field to make it
							// more efficient.
							let mut arguments = Vec::new();
							for arg in f.arguments.iter() {
								arguments.push(
									stk.run(|stk| arg.compute(stk, ctx, opt, Some(doc)))
										.await
										.catch_return()?,
								);
							}

							// replicate the same error that would happen with normal
							// function calls
							let (arg,) = <(String,)>::from_args("type::field", arguments)?;

							// manually do the implementation of type::field
							let idiom: Idiom = syn::idiom(&arg)?.into();

							let res =
								idiom.compute(stk, ctx, opt, Some(doc)).await.catch_return()?;

							if let Some(alias) = &v.alias {
								out.set(stk, ctx, opt, alias, res).await?;
							} else if self.is_single() {
								out = res
							} else {
								out.set(stk, ctx, opt, &idiom.0, res).await?;
							}
						}
						_ => {
							let expr = stk
								.run(|stk| v.expr.compute(stk, ctx, opt, Some(doc)))
								.await
								.catch_return()?;

							if self.is_single() {
								out = expr;
							} else {
								out.set(stk, ctx, opt, name.as_ref(), expr).await?;
							}
						}
					}
				}

				// This expression is a normal field expression
				_ => {
					let expr = stk
						.run(|stk| v.expr.compute(stk, ctx, opt, Some(doc)))
						.await
						.catch_return()?;
					// Check if this is a single VALUE field expression
					if self.is_single() {
						out = expr;
					} else {
						out.set(stk, ctx, opt, name.as_ref(), expr).await?;
					}
				}
			}
		}
		Ok(out)
	}
}

pub(crate) enum FieldsIter<'a> {
	Single(Option<&'a Selector>),
	Multiple(Iter<'a, Field>),
}

impl<'a> Iterator for FieldsIter<'a> {
	type Item = &'a Selector;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			FieldsIter::Single(field) => field.take(),
			FieldsIter::Multiple(iter) => loop {
				if let Field::Single(x) = iter.next()? {
					return Some(x);
				}
			},
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		match self {
			FieldsIter::Single(field) => {
				if field.is_some() {
					(1, Some(1))
				} else {
					(0, Some(0))
				}
			}
			FieldsIter::Multiple(iter) => iter.size_hint(),
		}
	}
}
impl ExactSizeIterator for FieldsIter<'_> {}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) enum Field {
	/// The `*` in `SELECT * FROM ...`
	#[default]
	All,
	/// The 'rating' in `SELECT rating FROM ...`
	Single(Selector),
}

impl Field {
	/// Check if computing this type can be done on a read only transaction.
	pub fn read_only(&self) -> bool {
		match self {
			Field::All => true,
			Field::Single(x) => x.read_only(),
		}
	}
}

impl ToSql for Field {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::All => f.push('*'),
			Self::Single(s) => s.fmt_sql(f, fmt),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct Selector {
	pub expr: Expr,
	/// The `quality` in `SELECT rating AS quality FROM ...`
	pub alias: Option<Idiom>,
}

impl Selector {
	pub fn read_only(&self) -> bool {
		self.expr.read_only()
	}
}

impl ToSql for Selector {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.expr.fmt_sql(f, fmt);
		if let Some(alias) = &self.alias {
			f.push_str(" AS ");
			alias.fmt_sql(f, fmt);
		}
	}
}
