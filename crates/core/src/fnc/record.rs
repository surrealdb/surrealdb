use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::capabilities::ExperimentalTarget;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::paths::ID;
use crate::expr::thing::Thing;
use crate::expr::value::Value;
use crate::expr::{Array, FlowResultExt as _, Idiom, Kind, Literal, Part, Table};
use anyhow::{Result, ensure};
use reblessive::tree::Stk;

use super::args::Optional;

pub async fn exists(
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(arg,): (Thing,),
) -> Result<Value> {
	if let Some(opt) = opt {
		let v = Value::Thing(arg).get(stk, ctx, opt, doc, ID.as_ref()).await.catch_return()?;
		Ok(Value::Bool(!v.is_none()))
	} else {
		Ok(Value::None)
	}
}

pub fn id((arg,): (Thing,)) -> Result<Value> {
	Ok(arg.id.into())
}

pub fn tb((arg,): (Thing,)) -> Result<Value> {
	Ok(arg.tb.into())
}

pub async fn refs(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(id, Optional(ft), Optional(ff)): (Thing, Optional<String>, Optional<String>),
) -> Result<Value> {
	ensure!(
		ctx.get_capabilities().allows_experimental(&ExperimentalTarget::RecordReferences),
		Error::InvalidFunction {
			name: "record::refs".to_string(),
			message: "Experimental feature is disabled".to_string(),
		}
	);

	// Process the inputs and make sure they are valid
	let ft = ft.map(Table::from);
	let ff = match ff {
		Some(ff) => Some(crate::syn::idiom(&ff)?.into()),
		None => None,
	};

	// Check to see if the user is allowed to select the origin record id
	if exists((stk, ctx, Some(opt), doc), (id.clone(),)).await?.is_false() {
		return Ok(Value::Array(Array::default()));
	}

	// If both a table and a field are provided, attempt to correct the field if needed
	let ff = match (&ft, ff) {
		(Some(ft), Some(ff)) => Some(correct_refs_field(ctx, opt, ft, ff).await?),
		(_, ff) => ff,
	};

	// Get the references
	let ids = id.refs(ctx, opt, ft.as_ref(), ff.as_ref()).await?;
	// Convert the references into a value
	let val = ids.into_iter().map(Value::Thing).collect();

	Ok(Value::Array(val))
}

async fn correct_refs_field(ctx: &Context, opt: &Options, ft: &Table, ff: Idiom) -> Result<Idiom> {
	// Obtain the field definition
	let (ns, db) = ctx.get_ns_db_ids(opt)?;
	let Some(fd) = ctx.tx().get_tb_field(ns, db, &ft.to_string(), &ff.to_string()).await? else {
		// If the field does not exist, there is nothing to correct
		return Ok(ff);
	};

	// Check if the field is an array-like value and thus "containing" references
	let is_contained = if let Some(kind) = &fd.kind {
		matches!(
			kind.get_optional_inner_kind(),
			Kind::Array(_, _) | Kind::Set(_, _) | Kind::Literal(Literal::Array(_))
		)
	} else {
		false
	};

	// If the field is an array-like value, add the `.*` part
	if is_contained {
		Ok(ff.push(Part::All))
	} else {
		Ok(ff)
	}
}
