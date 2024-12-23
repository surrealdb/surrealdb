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
	let ft = ft.map(Table::from);
	let ff = match ff {
		Some(ff) => Some(crate::syn::idiom(&ff)?),
		None => None,
	};

	let ff = match (&ft, ff) {
		(Some(ft), Some(ff)) => Some(correct_refs_field(ctx, opt, ft, ff).await?),
		(_, ff) => ff,
	};

	let ids = id.refs(ctx, opt, ft.as_ref(), ff.as_ref()).await?;
	let val = ids.into_iter().map(Value::Thing).collect();

	Ok(Value::Array(val))
}

async fn correct_refs_field(ctx: &Context, opt: &Options, ft: &Table, ff: Idiom) -> Result<Idiom, Error> {
	let tb = match ctx
		.tx()
		.get_tb_field(opt.ns()?, opt.db()?, &ft.to_string(), &ff.to_string())
		.await
	{
		Ok(tb) => tb,
		Err(Error::FdNotFound {
			..
		}) => return Ok(ff),
		Err(e) => return Err(e),
	};

	let is_contained = matches!(
		tb.kind,
		Some(Kind::Array(_, _) | Kind::Set(_, _) | Kind::Literal(Literal::Array(_)))
	);

	if is_contained {
		let ff = ff.push(Part::All);
		return Ok(ff);
	} else {
		return Ok(ff);
	}
}