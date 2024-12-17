use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::paths::ID;
use crate::sql::statements::SelectStatement;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use crate::sql::{Field, Fields, Idiom, Values};
use reblessive::tree::Stk;
use crate::key::r#ref::Ref;

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
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(id, ft,): (Thing, String,)
) -> Result<Value, Error> {
	let ns = opt.ns()?;
	let db = opt.db()?;
	let prefix = crate::key::r#ref::ftprefix(ns, db, &id.tb, &id.id, &ft);
	let suffix = crate::key::r#ref::ftsuffix(ns, db, &id.tb, &id.id, &ft);
	println!("prefix: {:?}", prefix);
	println!("suffix: {:?}", suffix);
	let rng = prefix..suffix;

	let raw = ctx.tx().keys(rng, 1000, opt.version).await?;
	let val: Vec<Value> = raw
		.iter()
		.map(|x| {
			let key = Ref::from(x);
			println!("found: {:?}", key);
			Value::Thing(Thing {
				tb: key.ft.to_string(),
				id: key.fk,
			})
		})
		.collect();

	let stm = SelectStatement {
		expr: Fields(
			vec![
				Field::Single {
					expr: Idiom::from("id").into(),
					alias: None,
				}
			],
			true
		),
		what: Values(val),
		..Default::default()
	};

	let res = stm.compute(stk, ctx, opt, doc).await?;

	Ok(res)
}
