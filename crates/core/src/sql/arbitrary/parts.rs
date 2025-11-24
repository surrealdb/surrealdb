use std::ops::Bound;

use arbitrary::Arbitrary;

use crate::sql::arbitrary::idiom::plain_idiom;
use crate::sql::arbitrary::{arb_vec1, atleast_one, basic_idiom};
use crate::sql::field::Selector;
use crate::sql::order::{OrderList, Ordering};
use crate::sql::statements::access::Subject;
use crate::sql::statements::define::config::api::Middleware;
use crate::sql::{
	Closure, Data, Expr, Fetch, Field, Fields, Function, FunctionCall, Group, Groups, Idiom, Kind,
	Lookup, Model, Order, RecordIdKeyLit, RecordIdKeyRangeLit, RecordIdLit, Split, Splits,
};
use crate::syn::parser::PATHS;

impl<'a> Arbitrary<'a> for Function {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		match u.int_in_range::<u8>(0..=5)? {
			0 => {
				let pick = u.int_in_range(0..=(PATHS.len() - 1))?;
				Ok(Function::Normal(
					PATHS.keys().nth(pick).expect("should be in range").to_string(),
				))
			}
			1 => Ok(Self::Custom(u.arbitrary()?)),
			2 => Ok(Self::Script(u.arbitrary()?)),
			3 => Ok(Self::Model(u.arbitrary()?)),
			4 => Ok(Self::Module(u.arbitrary()?, u.arbitrary()?)),
			5 => Ok(Self::Silo {
				org: u.arbitrary()?,
				pkg: u.arbitrary()?,
				major: u.arbitrary()?,
				minor: u.arbitrary()?,
				patch: u.arbitrary()?,
				sub: u.arbitrary()?,
			}),
			_ => unreachable!(),
		}
	}
}

impl<'a> Arbitrary<'a> for Closure {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let args = u.arbitrary()?;
		let returns: Option<Kind> = u.arbitrary()?;
		let body = if returns.is_some() {
			Expr::Block(u.arbitrary()?)
		} else {
			u.arbitrary()?
		};
		Ok(Closure {
			args,
			returns,
			body,
		})
	}
}

pub fn data_values<'a>(
	u: &mut arbitrary::Unstructured<'a>,
) -> arbitrary::Result<Vec<Vec<(Idiom, Expr)>>> {
	let mut idioms: Vec<Idiom> = vec![plain_idiom(u)?];
	for _ in 0..u.arbitrary_len::<Idiom>()? {
		idioms.push(plain_idiom(u)?);
	}

	let mut values = Vec::with_capacity(idioms.len());
	for i in idioms.iter() {
		values.push((i.clone(), u.arbitrary()?));
	}
	let mut res = vec![values];
	res.reserve_exact(u.arbitrary_len::<Vec<Expr>>()?);

	for _ in 1..res.capacity() {
		let mut values = Vec::with_capacity(idioms.len());

		for i in idioms.iter() {
			values.push((i.clone(), u.arbitrary()?));
		}
		res.push(values)
	}
	Ok(res)
}

impl<'a> Arbitrary<'a> for Data {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let r = match u.int_in_range(0u8..=5)? {
			0 => Data::SetExpression(atleast_one(u)?),
			1 => Data::UnsetExpression(arb_vec1(u, |u| plain_idiom(u))?),
			2 => Data::PatchExpression(u.arbitrary()?),
			3 => Data::MergeExpression(u.arbitrary()?),
			4 => Data::ReplaceExpression(u.arbitrary()?),
			5 => Data::ContentExpression(u.arbitrary()?),
			_ => unreachable!(),
		};
		Ok(r)
	}
}

pub fn insert_data<'a>(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Data> {
	let res = match u.int_in_range::<u8>(0..=1)? {
		0 => Data::SingleExpression(u.arbitrary()?),
		1 => {
			let res = data_values(u)?;
			Data::ValuesExpression(res)
		}
		_ => unreachable!(),
	};
	Ok(res)
}

impl<'a> Arbitrary<'a> for Model {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let version = format!(
			"{}.{}.{}",
			u.arbitrary::<u32>()?,
			u.arbitrary::<u32>()?,
			u.arbitrary::<u32>()?
		);

		Ok(Model {
			version,
			name: u.arbitrary()?,
		})
	}
}

impl<'a> Arbitrary<'a> for Fetch {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let r = match u.int_in_range(0u8..=2)? {
			0 => Expr::Param(u.arbitrary()?),
			1 => Expr::FunctionCall(Box::new(FunctionCall {
				receiver: Function::Normal("type::field".to_string()),
				arguments: u.arbitrary()?,
			})),
			2 => Expr::Idiom(plain_idiom(u)?),
			_ => unreachable!(),
		};
		Ok(Fetch(r))
	}
}

impl<'a> Arbitrary<'a> for RecordIdKeyRangeLit {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let start = match u.int_in_range(0u8..=2)? {
			0 => Bound::Unbounded,
			1 => Bound::Included(record_id_key_no_range(u)?),
			2 => Bound::Excluded(record_id_key_no_range(u)?),
			_ => unreachable!(),
		};
		let end = match u.int_in_range(0u8..=2)? {
			0 => Bound::Unbounded,
			1 => Bound::Included(record_id_key_no_range(u)?),
			2 => Bound::Excluded(record_id_key_no_range(u)?),
			_ => unreachable!(),
		};
		Ok(RecordIdKeyRangeLit {
			start,
			end,
		})
	}
}

pub fn record_id_no_range<'a>(
	u: &mut arbitrary::Unstructured<'a>,
) -> arbitrary::Result<RecordIdLit> {
	Ok(RecordIdLit {
		table: u.arbitrary()?,
		key: record_id_key_no_range(u)?,
	})
}

pub fn record_id_key_no_range<'a>(
	u: &mut arbitrary::Unstructured<'a>,
) -> arbitrary::Result<RecordIdKeyLit> {
	let r = match u.int_in_range(0u8..=5)? {
		0 => RecordIdKeyLit::Number(u.arbitrary()?),
		1 => RecordIdKeyLit::String(u.arbitrary()?),
		2 => RecordIdKeyLit::Uuid(u.arbitrary()?),
		3 => RecordIdKeyLit::Array(u.arbitrary()?),
		4 => RecordIdKeyLit::Object(u.arbitrary()?),
		5 => RecordIdKeyLit::Generate(u.arbitrary()?),
		_ => unreachable!(),
	};
	Ok(r)
}

impl<'a> Arbitrary<'a> for Subject {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let r = match u.int_in_range(0u8..=1)? {
			0 => Subject::User(u.arbitrary()?),
			1 => Subject::Record(record_id_no_range(u)?),
			_ => unreachable!(),
		};
		Ok(r)
	}
}

impl<'a> Arbitrary<'a> for Selector {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let alias = if u.arbitrary()? {
			Some(basic_idiom(u)?)
		} else {
			None
		};

		let expr = match u.arbitrary()? {
			Expr::Table(x) => Expr::Idiom(Idiom::field(x)),
			x => x,
		};

		Ok(Selector {
			expr,
			alias,
		})
	}
}

impl<'a> Arbitrary<'a> for Lookup {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let r = match u.int_in_range(0u8..=2)? {
			0 => Lookup {
				kind: u.arbitrary()?,
				..Default::default()
			},
			1 => Lookup {
				kind: u.arbitrary()?,
				what: vec![u.arbitrary()?],
				..Default::default()
			},
			2 => {
				let expr: Option<Fields> = u.arbitrary()?;

				let (split, group, order) = if let Some(expr) = &expr {
					let split = if u.arbitrary()? {
						Some(arb_splits(u, expr)?)
					} else {
						None
					};
					let group = if u.arbitrary()? {
						Some(arb_group(u, expr)?)
					} else {
						None
					};

					let order = if u.arbitrary()? {
						Some(arb_order(u, expr)?)
					} else {
						None
					};

					(split, group, order)
				} else {
					(None, None, None)
				};

				let alias = match u.int_in_range(0u8..=1)? {
					0 => Some(plain_idiom(u)?),
					1 => None,
					_ => unreachable!(),
				};

				Lookup {
					kind: u.arbitrary()?,
					expr,
					what: u.arbitrary()?,
					cond: u.arbitrary()?,
					split,
					group,
					order,
					limit: u.arbitrary()?,
					start: u.arbitrary()?,
					alias,
				}
			}
			_ => unreachable!(),
		};

		Ok(r)
	}
}

impl<'a> Arbitrary<'a> for Middleware {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let mut name = "api::".to_string();
		name.push_str(u.arbitrary()?);

		Ok(Middleware {
			name,
			args: u.arbitrary()?,
		})
	}
}

pub fn either_kind<'a>(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Vec<Kind>> {
	arb_vec1(u, |u| {
		let r = match u.int_in_range(0u8..=23)? {
			0 => Kind::None,
			1 => Kind::Null,
			2 => Kind::Bool,
			3 => Kind::Bytes,
			4 => Kind::Datetime,
			5 => Kind::Decimal,
			6 => Kind::Duration,
			7 => Kind::Float,
			8 => Kind::Int,
			9 => Kind::Number,
			10 => Kind::Object,
			11 => Kind::String,
			12 => Kind::Uuid,
			13 => Kind::Regex,
			14 => Kind::Table(u.arbitrary()?),
			15 => Kind::Record(u.arbitrary()?),
			16 => Kind::Geometry(u.arbitrary()?),
			17 => Kind::Either(either_kind(u)?),
			18 => Kind::Set(u.arbitrary()?, u.arbitrary()?),
			19 => Kind::Array(u.arbitrary()?, u.arbitrary()?),
			20 => Kind::Function(u.arbitrary()?, u.arbitrary()?),
			21 => Kind::Range,
			22 => Kind::Literal(u.arbitrary()?),
			23 => Kind::File(u.arbitrary()?),

			_ => unreachable!(),
		};
		Ok(r)
	})
}

pub fn arb_splits<'a>(
	u: &mut arbitrary::Unstructured<'a>,
	expr: &Fields,
) -> arbitrary::Result<Splits> {
	if expr.contains_all() {
		return arb_vec1(u, |u| basic_idiom(u).map(Split)).map(Splits);
	}
	let mut res = vec![Split(idiom_from_expr(u, expr)?)];
	res.reserve_exact(u.arbitrary_len::<Idiom>()?);
	for _ in 1..=res.capacity() {
		res.push(Split(idiom_from_expr(u, expr)?))
	}
	Ok(Splits(res))
}

pub fn arb_order<'a>(
	u: &mut arbitrary::Unstructured<'a>,
	expr: &Fields,
) -> arbitrary::Result<Ordering> {
	if u.arbitrary()? {
		return Ok(Ordering::Random);
	}

	if expr.contains_all() {
		return Ok(Ordering::Order(u.arbitrary()?));
	}

	let mut res = vec![Order {
		value: idiom_from_expr(u, expr)?,
		collate: u.arbitrary()?,
		numeric: u.arbitrary()?,
		direction: u.arbitrary()?,
	}];
	res.reserve_exact(u.arbitrary_len::<Idiom>()?);
	for _ in 1..=res.capacity() {
		res.push(Order {
			value: idiom_from_expr(u, expr)?,
			collate: u.arbitrary()?,
			numeric: u.arbitrary()?,
			direction: u.arbitrary()?,
		});
	}
	Ok(Ordering::Order(OrderList(res)))
}

pub fn arb_group<'a>(
	u: &mut arbitrary::Unstructured<'a>,
	expr: &Fields,
) -> arbitrary::Result<Groups> {
	if expr.contains_all() {
		return arb_vec1(u, |u| basic_idiom(u).map(Group)).map(Groups);
	}

	let mut res = vec![Group(idiom_from_expr(u, expr)?)];
	res.reserve_exact(u.arbitrary_len::<Idiom>()?);
	for _ in 1..=res.capacity() {
		res.push(Group(idiom_from_expr(u, expr)?))
	}
	Ok(Groups(res))
}

fn idiom_from_expr<'a>(
	u: &mut arbitrary::Unstructured<'a>,
	expr: &Fields,
) -> arbitrary::Result<Idiom> {
	match expr {
		Fields::Value(selector) => {
			if let Some(alias) = selector.alias.clone() {
				return Ok(alias);
			}

			match &selector.expr {
				Expr::Idiom(x) => Ok(x.clone()),
				x => Ok(x.to_idiom()),
			}
		}
		Fields::Select(fields) => {
			let Field::Single(selector) = u.choose(fields)? else {
				return basic_idiom(u);
			};
			if let Some(alias) = selector.alias.clone() {
				return Ok(alias);
			}

			match &selector.expr {
				Expr::Idiom(x) => Ok(x.clone()),
				x => Ok(x.to_idiom()),
			}
		}
	}
}
