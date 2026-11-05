//! The two casts that read the document.
//!
//! `type::field` and `type::fields` take an idiom as a string and evaluate it
//! against the record being iterated, so they need the execution environment. Every other
//! cast in the family answers from its argument and lives one crate down;
//! they are re-exported here so a caller sees one module.

use anyhow::Result;
pub use surrealdb_runtime::r#type::*;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Expr, Idiom};
use crate::syn;
use crate::val::Value;

pub async fn field(
	(ctx, opt, doc): (&FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(val,): (String,),
) -> Result<Value> {
	match opt {
		Some(opt) => {
			let exec = crate::exec::entry::frame(ctx, opt, doc).await;
			// Parse the string as an Idiom
			let idi: Idiom = syn::idiom(&val)?;
			// Return the Idiom or fetch the field
			crate::exec::entry::scalar(&exec, &Expr::Idiom(idi)).await
		}
		_ => Ok(Value::None),
	}
}

pub async fn fields(
	(ctx, opt, doc): (&FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(val,): (Vec<String>,),
) -> Result<Value> {
	match opt {
		Some(opt) => {
			let exec = crate::exec::entry::frame(ctx, opt, doc).await;
			let mut args: Vec<Value> = Vec::with_capacity(val.len());
			for v in val {
				// Parse the string as an Idiom
				let idi: Idiom = syn::idiom(&v)?;
				// Return the Idiom or fetch the field
				args.push(crate::exec::entry::scalar(&exec, &Expr::Idiom(idi)).await?);
			}
			Ok(args.into())
		}
		_ => Ok(Value::None),
	}
}
