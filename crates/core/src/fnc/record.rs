use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::paths::ID;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use crate::sql::{Idiom, Kind, Literal, Part, Table};
use reblessive::tree::Stk;

pub async fn exists(
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(arg,): (Thing,),
) -> Result<Value, Error> {
	if let Some(opt) = opt {
		Ok(match Value::Thing(arg).get(stk, ctx, opt, doc, ID.as_ref()).await? {
			Value::None => Value::Bool(false),
			_ => Value::Bool(true),
		})
	} else {
		Ok(Value::None)
	}
}

pub fn id((arg,): (Thing,)) -> Result<Value, Error> {
	Ok(arg.id.into())
}

pub fn tb((arg,): (Thing,)) -> Result<Value, Error> {
	Ok(arg.tb.into())
}

pub async fn refs(
	(ctx, opt): (&Context, &Options),
	(id, ft, ff): (Thing, Option<String>, Option<String>),
) -> Result<Value, Error> {
	// Process the inputs and make sure they are valid
	let ft = ft.map(Table::from);
	let ff = match ff {
		Some(ff) => Some(crate::syn::idiom(&ff)?),
		None => None,
	};

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

async fn correct_refs_field(
	ctx: &Context,
	opt: &Options,
	ft: &Table,
	ff: Idiom,
) -> Result<Idiom, Error> {
	// Obtain the field definition
	let fd =
		match ctx.tx().get_tb_field(opt.ns()?, opt.db()?, &ft.to_string(), &ff.to_string()).await {
			Ok(fd) => fd,
			// If the field does not exist, there is nothing to correct
			Err(Error::FdNotFound {
				..
			}) => return Ok(ff),
			Err(e) => return Err(e),
		};

	// Check if the field is an array-like value and thus "containing" references
	let is_contained = if let Some(kind) = &fd.kind {
		matches!(
			kind.non_optional(),
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
