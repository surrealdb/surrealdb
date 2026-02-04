use std::ops::Bound;
use std::sync::LazyLock;

use arbitrary::Arbitrary;

use crate::dbs::Capabilities;
use crate::sql::access_type::{JwtAccess, JwtAccessIssue, JwtAccessVerify};
use crate::sql::arbitrary::idiom::plain_idiom;
use crate::sql::arbitrary::{arb_vec1, arb_vec2, atleast_one, basic_idiom};
use crate::sql::index::HnswParams;
use crate::sql::order::{OrderList, Ordering};
use crate::sql::statements::access::Subject;
use crate::sql::{
	Base, Closure, Data, Expression, Fetch, Field, Fields, Function, Graph, Group, Groups, Id,
	IdRange, Idiom, Kind, Mock, Model, Operator, Order, Part, Scoring, Split, Splits, Thing, Value,
};
use crate::syn::parser::{PathKind, PATHS};

static IGNORED_FUNCTIONS: &[&str] = &["migration::diagnose", "api::invoke", "record::refs"];
static FN_PATH_INDECIES: LazyLock<Vec<usize>> = LazyLock::new(|| {
	PATHS
		.entries()
		.enumerate()
		.filter(|(_, (k, v))| matches!(v, PathKind::Function) && !IGNORED_FUNCTIONS.contains(k))
		.map(|x| x.0)
		.collect()
});

impl<'a> Arbitrary<'a> for Function {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		match u.int_in_range::<u8>(0..=3)? {
			0 => {
				let pick = u.int_in_range(0..=(FN_PATH_INDECIES.len() - 1))?;
				let idx = FN_PATH_INDECIES[pick];
				Ok(Function::Normal(
					PATHS.keys().nth(idx).expect("should be in range").to_string(),
					u.arbitrary()?,
				))
			}
			1 => Ok(Self::Custom(u.arbitrary()?, u.arbitrary()?)),
			2 => Ok(Self::Script(u.arbitrary()?, u.arbitrary()?)),
			3 => Ok(Self::Anonymous(u.arbitrary()?, u.arbitrary()?, u.arbitrary()?)),
			_ => unreachable!(),
		}
	}
}

impl<'a> Arbitrary<'a> for Closure {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let args = u.arbitrary()?;
		let returns: Option<Kind> = u.arbitrary()?;
		let body = if returns.is_some() {
			Value::Block(u.arbitrary()?)
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
) -> arbitrary::Result<Vec<Vec<(Idiom, Value)>>> {
	let mut idioms: Vec<Idiom> = vec![plain_idiom(u)?];
	for _ in 0..u.arbitrary_len::<Idiom>()? {
		idioms.push(plain_idiom(u)?);
	}

	let mut values = Vec::with_capacity(idioms.len());
	for i in idioms.iter() {
		values.push((i.clone(), u.arbitrary()?));
	}
	let mut res = vec![values];
	res.reserve_exact(u.arbitrary_len::<Vec<Value>>()?);

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
			0 => {
				let data = arb_vec1(u, |u| {
					let op = match u.int_in_range(0u8..=3)? {
						0 => Operator::Equal,
						1 => Operator::Inc,
						2 => Operator::Dec,
						3 => Operator::Ext,
						_ => unreachable!(),
					};

					Ok((plain_idiom(u)?, op, u.arbitrary()?))
				})?;
				Data::SetExpression(data)
			}
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
			args: u.arbitrary()?,
		})
	}
}

impl<'a> Arbitrary<'a> for Fetch {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let r = match u.int_in_range(0u8..=2)? {
			0 => Value::Param(u.arbitrary()?),
			1 => Value::Function(Box::new(Function::Normal(
				"type::field".to_string(),
				u.arbitrary()?,
			))),
			2 => Value::Idiom(plain_idiom(u)?),
			_ => unreachable!(),
		};
		Ok(Fetch(r))
	}
}

impl<'a> Arbitrary<'a> for IdRange {
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
		Ok(IdRange {
			beg: start,
			end,
		})
	}
}

pub fn record_id_no_range<'a>(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Thing> {
	Ok(Thing {
		tb: u.arbitrary()?,
		id: record_id_key_no_range(u)?,
	})
}

pub fn record_id_key_no_range<'a>(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Id> {
	let r = match u.int_in_range(0u8..=4)? {
		0 => Id::Number(u.arbitrary()?),
		1 => Id::String(u.arbitrary()?),
		2 => Id::Uuid(u.arbitrary()?),
		3 => Id::Array(u.arbitrary()?),
		4 => Id::Object(u.arbitrary()?),
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

impl<'a> Arbitrary<'a> for Field {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		match u.int_in_range(0u8..=1)? {
			0 => Ok(Field::All),
			1 => {
				let alias = if u.arbitrary()? {
					Some(basic_idiom(u)?)
				} else {
					None
				};

				let expr = match u.arbitrary()? {
					Value::Table(x) => Value::Idiom(Idiom::from(x.0)),
					x => x,
				};

				Ok(Field::Single {
					expr,
					alias,
				})
			}
			_ => unreachable!(),
		}
	}
}

impl<'a> Arbitrary<'a> for Graph {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let r = match u.int_in_range(0u8..=2)? {
			0 => Graph {
				..Default::default()
			},
			1 => Graph {
				what: u.arbitrary()?,
				..Default::default()
			},
			2 => {
				let mut expr: Option<Fields> = u.arbitrary()?;

				let (split, group, order) = if let Some(expr) = &mut expr {
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

				Graph {
					dir: u.arbitrary()?,
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

pub fn geometry_kind<'a>(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Vec<String>> {
	arb_vec1(u, |u| {
		Ok(u.choose(&[
			"point",
			"line",
			"polygon",
			"multipoint",
			"multiline",
			"multipolygon",
			"collection",
		])?
		.to_string())
	})
}

pub fn option_kind<'a>(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Box<Kind>> {
	let r = match u.int_in_range(1u8..=20)? {
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
		13 => Kind::Record(u.arbitrary()?),
		14 => Kind::Geometry(geometry_kind(u)?),
		15 => Kind::Either(either_kind(u)?),
		16 => Kind::Set(u.arbitrary()?, u.arbitrary()?),
		17 => Kind::Array(u.arbitrary()?, u.arbitrary()?),
		18 => Kind::Function(u.arbitrary()?, u.arbitrary()?),
		19 => Kind::Range,
		20 => Kind::Literal(u.arbitrary()?),
		_ => unreachable!(),
	};
	Ok(Box::new(r))
}

pub fn either_kind<'a>(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Vec<Kind>> {
	let k = arb_vec2(u, |u| {
		let r = match u.int_in_range(1u8..=19)? {
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
			13 => Kind::Record(u.arbitrary()?),
			14 => Kind::Geometry(geometry_kind(u)?),
			15 => Kind::Set(u.arbitrary()?, u.arbitrary()?),
			16 => Kind::Array(u.arbitrary()?, u.arbitrary()?),
			17 => Kind::Function(u.arbitrary()?, u.arbitrary()?),
			18 => Kind::Range,
			19 => Kind::Literal(u.arbitrary()?),

			_ => unreachable!(),
		};
		Ok(r)
	})?;

	Ok(k)
}

pub fn arb_splits<'a>(
	u: &mut arbitrary::Unstructured<'a>,
	expr: &mut Fields,
) -> arbitrary::Result<Splits> {
	if expr.is_all() {
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
	expr: &mut Fields,
) -> arbitrary::Result<Ordering> {
	if u.arbitrary()? {
		return Ok(Ordering::Random);
	}

	if expr.is_all() {
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
	expr: &mut Fields,
) -> arbitrary::Result<Groups> {
	if expr.is_all() {
		return arb_vec1(u, |u| basic_idiom(u).map(Group)).map(Groups);
	}

	let mut res = vec![Group(idiom_from_expr(u, expr)?)];
	res.reserve_exact(u.arbitrary_len::<Idiom>()?);
	for _ in 1..=res.capacity() {
		res.push(Group(idiom_from_expr(u, expr)?))
	}
	Ok(Groups(res))
}

fn idiom_is_basic(idiom: &Idiom) -> bool {
	let Some(Part::Field(_)) = idiom.0.first() else {
		return false;
	};

	for p in idiom.0[1..].iter() {
		if !matches!(p, Part::Field(_) | Part::All | Part::Value(Value::Number(_))) {
			return false;
		}
	}

	true
}

fn idiom_from_expr<'a>(
	u: &mut arbitrary::Unstructured<'a>,
	fields: &mut Fields,
) -> arbitrary::Result<Idiom> {
	if fields.is_all() {
		return basic_idiom(u);
	}

	if fields.1 {
		let Some(Field::Single {
			expr,
			alias,
		}) = fields.0.first_mut()
		else {
			return Ok(Idiom::from("a"));
		};

		if let Some(alias) = alias.clone() {
			return Ok(alias);
		}

		match &expr {
			Value::Idiom(x) => {
				if idiom_is_basic(x) {
					return Ok(x.clone());
				}

				let res = basic_idiom(u)?;
				*fields = Fields(
					vec![
						Field::Single {
							expr: expr.clone(),
							alias: alias.clone(),
						},
						Field::Single {
							expr: Value::Idiom(res.clone()),
							alias: None,
						},
					],
					false,
				);

				Ok(res)
			}
			_ => {
				let new_alias = basic_idiom(u)?;
				*alias = Some(new_alias.clone());
				Ok(new_alias)
			}
		}
	} else {
		let idx = u.choose_index(fields.0.len())?;
		let Field::Single {
			expr,
			alias,
		} = &mut fields.0[idx]
		else {
			return basic_idiom(u);
		};

		if let Some(alias) = alias.clone() {
			return Ok(alias);
		}

		match &expr {
			Value::Idiom(x) => {
				if idiom_is_basic(x) {
					return Ok(x.clone());
				}

				let res = basic_idiom(u)?;
				*fields = Fields(
					vec![
						Field::Single {
							expr: expr.clone(),
							alias: alias.clone(),
						},
						Field::Single {
							expr: Value::Idiom(res.clone()),
							alias: None,
						},
					],
					false,
				);

				Ok(res)
			}
			_ => {
				let new_alias = basic_idiom(u)?;
				*alias = Some(new_alias.clone());
				Ok(new_alias)
			}
		}
	}
}

impl<'a> Arbitrary<'a> for JwtAccess {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let verify = u.arbitrary()?;
		let issue = if u.arbitrary()? {
			let alg = if let JwtAccessVerify::Key(ref ver) = verify {
				ver.alg
			} else {
				u.arbitrary()?
			};

			if alg.is_symmetric() {
				None
			} else {
				let key = u.arbitrary()?;
				Some(JwtAccessIssue {
					alg,
					key,
				})
			}
		} else {
			None
		};

		Ok(JwtAccess {
			verify,
			issue,
		})
	}
}

impl<'a> Arbitrary<'a> for Scoring {
	fn size_hint(depth: usize) -> (usize, Option<usize>) {
		arbitrary::size_hint::and(f32::size_hint(depth), f32::size_hint(depth))
	}

	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		Ok(Scoring::Bm {
			k1: u.arbitrary()?,
			b: u.arbitrary()?,
		})
	}
}

impl<'a> Arbitrary<'a> for Expression {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		if u.arbitrary()? {
			let op = match u.int_in_range(0..=2)? {
				0 => Operator::Add,
				1 => Operator::Neg,
				2 => Operator::Not,
				_ => unreachable!(),
			};
			Ok(Expression::Unary {
				o: op,
				v: u.arbitrary()?,
			})
		} else {
			let op = match u.int_in_range(0..=39)? {
				0 => Operator::Or,
				1 => Operator::And,
				2 => Operator::Tco,
				3 => Operator::Nco,
				4 => Operator::Exact,
				5 => Operator::NotEqual,
				6 => Operator::AllEqual,
				7 => Operator::AnyEqual,
				8 => Operator::Equal,
				9 => Operator::NotLike,
				10 => Operator::AllLike,
				11 => Operator::AnyLike,
				12 => Operator::Like,
				13 => Operator::Matches(u.arbitrary()?),
				14 => Operator::LessThanOrEqual,
				15 => Operator::LessThan,
				16 => Operator::MoreThanOrEqual,
				17 => Operator::MoreThan,
				18 => Operator::Pow,
				19 => Operator::Add,
				20 => Operator::Sub,
				21 => Operator::Mul,
				22 => Operator::Div,
				23 => Operator::Rem,
				24 => Operator::Contain,
				25 => Operator::NotContain,
				26 => Operator::Inside,
				27 => Operator::NotInside,
				28 => Operator::ContainAll,
				29 => Operator::ContainNone,
				30 => Operator::AllInside,
				31 => Operator::AnyInside,
				32 => Operator::NoneInside,
				33 => Operator::NotEqual,
				34 => Operator::Equal,
				35 => Operator::Outside,
				36 => Operator::Intersects,
				37 => Operator::NotInside,
				38 => Operator::Inside,
				39 => Operator::ContainAny,
				_ => unreachable!(),
			};
			Ok(Expression::Binary {
				l: u.arbitrary()?,
				o: op,
				r: u.arbitrary()?,
			})
		}
	}
}

impl<'a> Arbitrary<'a> for Base {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		match u.int_in_range(0..=2)? {
			0 => Ok(Base::Root),
			1 => Ok(Base::Ns),
			2 => Ok(Base::Db),
			_ => unreachable!(),
		}
	}
}

impl<'a> Arbitrary<'a> for HnswParams {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		Ok(HnswParams {
			dimension: u.arbitrary()?,
			distance: u.arbitrary()?,
			vector_type: u.arbitrary()?,
			m: u.int_in_range(0..=127)?,
			m0: u.arbitrary()?,
			ef_construction: u.arbitrary()?,
			extend_candidates: u.arbitrary()?,
			keep_pruned_connections: u.arbitrary()?,
			ml: u.arbitrary()?,
		})
	}
}

impl<'a> Arbitrary<'a> for Mock {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		if u.arbitrary()? {
			Ok(Mock::Count(u.arbitrary()?, u.int_in_range(0..=(i64::MAX as u64))?))
		} else {
			Ok(Mock::Range(
				u.arbitrary()?,
				u.int_in_range(0..=(i64::MAX as u64))?,
				u.int_in_range(0..=(i64::MAX as u64))?,
			))
		}
	}
}
