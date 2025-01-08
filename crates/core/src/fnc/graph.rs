use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::array::{Flatten, Uniq};
use crate::sql::value::Value;
use crate::sql::{Array, Dir, Graph, Part};
use reblessive::tree::Stk;

pub async fn find_relations(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(arg,): (Array,),
) -> Result<Value, Error> {
	let arg = arg.uniq();
	let path = [
		Part::Graph(Graph {
			dir: Dir::Both,
			..Default::default()
		}),
		Part::All,
	];

	let items = Value::Array(arg.clone()).get(stk, ctx, opt, doc, &path).await?;
	if let Value::Array(arr) = items {
		let vals = arr
			.flatten()
			.into_iter()
			.filter_map(|v| if let Value::Object(obj) = v {
				// if obj.contains_key("__") {
					println!("obj {obj}");
					let r#in = match obj.get("in") {
						Some(v) => v.to_owned(),
						_ => return None
					};
					let id = match obj.get("id") {
						Some(v) => v.to_owned(),
						_ => return None
					};
					let out = match obj.get("out") {
						Some(v) => v.to_owned(),
						_ => return None
					};

					if arg.contains(&r#in) && arg.contains(&out) {
						let ids = vec![r#in, id, out];
						Some(Ok(Value::from(ids)))
					} else {
						None
					}
				// } else {
				// 	println!("obj {obj}");
				// 	None
				// }
			} else {
				println!("v {v}");
				Some(Err(Error::Unreachable("expected an array of objects to be returned from the graph lookup".into())))
			})
			.collect::<Result<Vec<Value>, Error>>()?;
		
		let arr = Array(vals).uniq();

		Ok(Value::from(arr))
	} else {
		Err(Error::Unreachable("expected an array to be returned from the graph lookup".into()))
	}
}