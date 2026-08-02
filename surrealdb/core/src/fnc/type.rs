//! The two casts that read the document.
//!
//! `type::field` and `type::fields` take an idiom as a string and evaluate it
//! against the record being iterated, so they need the evaluator. Every other
//! cast in the family answers from its argument and lives one crate down;
//! they are re-exported here so a caller sees one module.

use anyhow::Result;
use reblessive::tree::Stk;
pub use surrealdb_runtime::r#type::*;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt as _;
use crate::expr::Idiom;
use crate::val::Value;
use crate::{legacy, syn};

pub async fn field(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(val,): (String,),
) -> Result<Value> {
	match opt {
		Some(opt) => {
			// Parse the string as an Idiom
			let idi: Idiom = syn::idiom(&val)?.into();
			// Return the Idiom or fetch the field
			legacy::idiom_compute(&idi, stk, ctx, opt, doc).await.catch_return()
		}
		_ => Ok(Value::None),
	}
}

pub async fn fields(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(val,): (Vec<String>,),
) -> Result<Value> {
	match opt {
		Some(opt) => {
			let mut args: Vec<Value> = Vec::with_capacity(val.len());
			for v in val {
				// Parse the string as an Idiom
				let idi: Idiom = syn::idiom(&v)?.into();
				// Return the Idiom or fetch the field
				args.push(legacy::idiom_compute(&idi, stk, ctx, opt, doc).await.catch_return()?);
			}
			Ok(args.into())
		}
		_ => Ok(Value::None),
	}
}
