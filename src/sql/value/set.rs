use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::idiom::Idiom;
use crate::sql::part::Part;
use crate::sql::value::Value;
use async_recursion::async_recursion;
use futures::future::try_join_all;

impl Value {
	#[async_recursion]
	pub async fn set(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		exe: &Executor<'_>,
		path: &Idiom,
		val: Value,
	) -> Result<(), Error> {
		match path.parts.first() {
			// Get the current path part
			Some(p) => match self {
				// Current path part is an object
				Value::Object(v) => match p {
					Part::Field(p) => match v.value.get_mut(&p.name) {
						Some(v) if v.is_some() => v.set(ctx, opt, exe, &path.next(), val).await,
						_ => {
							let mut obj = Value::base();
							obj.set(ctx, opt, exe, &path.next(), val).await?;
							v.insert(&p.name, obj);
							Ok(())
						}
					},
					_ => Ok(()),
				},
				// Current path part is an array
				Value::Array(v) => match p {
					Part::All => {
						let pth = path.next();
						let fut =
							v.value.iter_mut().map(|v| v.set(ctx, opt, exe, &pth, val.clone()));
						try_join_all(fut).await?;
						Ok(())
					}
					Part::First => match v.value.first_mut() {
						Some(v) => v.set(ctx, opt, exe, &path.next(), val).await,
						None => Ok(()),
					},
					Part::Last => match v.value.last_mut() {
						Some(v) => v.set(ctx, opt, exe, &path.next(), val).await,
						None => Ok(()),
					},
					Part::Index(i) => match v.value.get_mut(i.to_usize()) {
						Some(v) => v.set(ctx, opt, exe, &path.next(), val).await,
						None => Ok(()),
					},
					Part::Where(w) => {
						let pth = path.next();
						for v in &mut v.value {
							if w.compute(ctx, opt, exe, Some(&v)).await?.is_truthy() {
								v.set(ctx, opt, exe, &pth, val.clone()).await?;
							}
						}
						Ok(())
					}
					_ => Ok(()),
				},
				// Current path part is empty
				Value::Null => {
					*self = Value::base();
					self.set(ctx, opt, exe, path, val).await
				}
				// Current path part is empty
				Value::None => {
					*self = Value::base();
					self.set(ctx, opt, exe, path, val).await
				}
				// Ignore everything else
				_ => Ok(()),
			},
			// No more parts so set the value
			None => {
				*self = val;
				Ok(())
			}
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::sql::test::Parse;

	#[tokio::test]
	async fn set_none() {
		let (ctx, opt, exe) = mock();
		let idi = Idiom::default();
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("999");
		val.set(&ctx, &opt, &exe, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_empty() {
		let (ctx, opt, exe) = mock();
		let idi = Idiom::parse("test");
		let mut val = Value::None;
		let res = Value::parse("{ test: 999 }");
		val.set(&ctx, &opt, &exe, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_blank() {
		let (ctx, opt, exe) = mock();
		let idi = Idiom::parse("test.something");
		let mut val = Value::None;
		let res = Value::parse("{ test: { something: 999 } }");
		val.set(&ctx, &opt, &exe, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_reset() {
		let (ctx, opt, exe) = mock();
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: 999 }");
		val.set(&ctx, &opt, &exe, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_basic() {
		let (ctx, opt, exe) = mock();
		let idi = Idiom::parse("test.something");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: null, something: 999 } }");
		val.set(&ctx, &opt, &exe, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_allow() {
		let (ctx, opt, exe) = mock();
		let idi = Idiom::parse("test.something.allow");
		let mut val = Value::parse("{ test: { other: null } }");
		let res = Value::parse("{ test: { other: null, something: { allow: 999 } } }");
		val.set(&ctx, &opt, &exe, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_wrong() {
		let (ctx, opt, exe) = mock();
		let idi = Idiom::parse("test.something.wrong");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: null, something: 123 } }");
		val.set(&ctx, &opt, &exe, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_other() {
		let (ctx, opt, exe) = mock();
		let idi = Idiom::parse("test.other.something");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: { something: 999 }, something: 123 } }");
		val.set(&ctx, &opt, &exe, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array() {
		let (ctx, opt, exe) = mock();
		let idi = Idiom::parse("test.something[1]");
		let mut val = Value::parse("{ test: { something: [123, 456, 789] } }");
		let res = Value::parse("{ test: { something: [123, 999, 789] } }");
		val.set(&ctx, &opt, &exe, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_field() {
		let (ctx, opt, exe) = mock();
		let idi = Idiom::parse("test.something[1].age");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 34 }, { age: 21 }] } }");
		val.set(&ctx, &opt, &exe, &idi, Value::from(21)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_fields() {
		let (ctx, opt, exe) = mock();
		let idi = Idiom::parse("test.something[*].age");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 21 }, { age: 21 }] } }");
		val.set(&ctx, &opt, &exe, &idi, Value::from(21)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_where_field() {
		let (ctx, opt, exe) = mock();
		let idi = Idiom::parse("test.something[WHERE age > 35].age");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 34 }, { age: 21 }] } }");
		val.set(&ctx, &opt, &exe, &idi, Value::from(21)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_where_fields() {
		let (ctx, opt, exe) = mock();
		let idi = Idiom::parse("test.something[WHERE age > 35]");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 34 }, 21] } }");
		val.set(&ctx, &opt, &exe, &idi, Value::from(21)).await.unwrap();
		assert_eq!(res, val);
	}
}
