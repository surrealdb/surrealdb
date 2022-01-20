use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::field::{Field, Fields};
use crate::sql::idiom::Idiom;
use crate::sql::part::Part;
use crate::sql::statements::select::SelectStatement;
use crate::sql::value::{Value, Values};
use async_recursion::async_recursion;

impl Value {
	#[async_recursion]
	pub async fn get(
		&self,
		ctx: &Runtime,
		opt: &Options<'_>,
		exe: &mut Executor,
		path: &Idiom,
	) -> Result<Self, Error> {
		match path.parts.first() {
			// Get the current path part
			Some(p) => match self {
				// Current path part is an object
				Value::Object(v) => match p {
					Part::Field(p) => match v.value.get(&p.name) {
						Some(v) => v.get(ctx, opt, exe, &path.next()).await,
						None => Ok(Value::None),
					},
					_ => Ok(Value::None),
				},
				// Current path part is an array
				Value::Array(v) => match p {
					Part::All => {
						let mut a = Vec::new();
						for v in &v.value {
							a.push(v.get(ctx, opt, exe, &path.next()).await?)
						}
						Ok(a.into())
					}
					Part::First => match v.value.first() {
						Some(v) => v.get(ctx, opt, exe, &path.next()).await,
						None => Ok(Value::None),
					},
					Part::Last => match v.value.last() {
						Some(v) => v.get(ctx, opt, exe, &path.next()).await,
						None => Ok(Value::None),
					},
					Part::Index(i) => match v.value.get(i.to_usize()) {
						Some(v) => v.get(ctx, opt, exe, &path.next()).await,
						None => Ok(Value::None),
					},
					Part::Where(w) => {
						let mut a = Vec::new();
						for v in &v.value {
							if w.compute(ctx, opt, exe, Some(&v)).await?.is_truthy() {
								a.push(v.get(ctx, opt, exe, &path.next()).await?)
							}
						}
						Ok(a.into())
					}
					_ => Ok(Value::None),
				},
				// Current path part is a thing
				Value::Thing(v) => match path.parts.len() {
					// No remote embedded fields, so just return this
					1 => Ok(Value::Thing(v.clone())),
					// Remote embedded field, so fetch the thing
					_ => {
						let stm = SelectStatement {
							expr: Fields(vec![Field::All]),
							what: Values(vec![Value::Thing(v.clone())]),
							..SelectStatement::default()
						};
						match stm.compute(ctx, opt, exe, None).await {
							Ok(v) => v.get(ctx, opt, exe, &path.next()).await,
							Err(_) => Ok(Value::None),
						}
					}
				},
				// Ignore everything else
				_ => Ok(Value::None),
			},
			// No more parts so get the value
			None => Ok(self.clone()),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::sql::test::Parse;
	use crate::sql::thing::Thing;

	#[tokio::test]
	async fn get_none() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::default();
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = val.get(&ctx, &opt, &mut exe, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn get_basic() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test.something");
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = val.get(&ctx, &opt, &mut exe, &idi).await.unwrap();
		assert_eq!(res, Value::from(123));
	}

	#[tokio::test]
	async fn get_thing() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test.other");
		let val = Value::parse("{ test: { other: test:tobie, something: 123 } }");
		let res = val.get(&ctx, &opt, &mut exe, &idi).await.unwrap();
		assert_eq!(
			res,
			Value::from(Thing {
				tb: String::from("test"),
				id: String::from("tobie")
			})
		);
	}

	#[tokio::test]
	async fn get_array() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test.something[1]");
		let val = Value::parse("{ test: { something: [123, 456, 789] } }");
		let res = val.get(&ctx, &opt, &mut exe, &idi).await.unwrap();
		assert_eq!(res, Value::from(456));
	}

	#[tokio::test]
	async fn get_array_thing() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test.something[1]");
		let val = Value::parse("{ test: { something: [test:tobie, test:jaime] } }");
		let res = val.get(&ctx, &opt, &mut exe, &idi).await.unwrap();
		assert_eq!(
			res,
			Value::from(Thing {
				tb: String::from("test"),
				id: String::from("jaime")
			})
		);
	}

	#[tokio::test]
	async fn get_array_field() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test.something[1].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &mut exe, &idi).await.unwrap();
		assert_eq!(res, Value::from(36));
	}

	#[tokio::test]
	async fn get_array_fields() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test.something[*].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &mut exe, &idi).await.unwrap();
		assert_eq!(res, Value::from(vec![34, 36]));
	}

	#[tokio::test]
	async fn get_array_where_field() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test.something[WHERE age > 35].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &mut exe, &idi).await.unwrap();
		assert_eq!(res, Value::from(vec![36]));
	}

	#[tokio::test]
	async fn get_array_where_fields() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test.something[WHERE age > 35]");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &mut exe, &idi).await.unwrap();
		assert_eq!(
			res,
			Value::from(vec![Value::from(map! {
				"age".to_string() => Value::from(36),
			})])
		);
	}
}
