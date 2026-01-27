use arbitrary::Arbitrary;
use rust_decimal::Decimal;

use crate::sql::arbitrary::{arb_opt, arb_vec1, atleast_one, record_id_key_no_range};
use crate::sql::part::RecurseInstruction;
use crate::sql::{Expr, Idiom, Literal, Part, RecordIdLit};

impl<'a> Arbitrary<'a> for Idiom {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let res = match u.int_in_range(0..=2)? {
			0 => Part::Value(u.arbitrary()?),
			1 => Part::Field(u.arbitrary()?),
			2 => Part::Graph(u.arbitrary()?),

			_ => unreachable!(),
		};
		let mut res = vec![res];
		if matches!(res[0], Part::Value(_)) {
			res.push(u.arbitrary()?);
		}

		let len = u.arbitrary_len::<Part>()?;
		res.reserve(len);
		for _ in 0..len {
			res.push(u.arbitrary()?);
		}

		Ok(Idiom(res))
	}
}

impl<'a> Arbitrary<'a> for Part {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let res = match u.int_in_range(0..=12)? {
			0 => Part::All,
			1 => Part::Flatten,
			2 => Part::Last,
			3 => Part::First,
			4 => Part::Field(u.arbitrary()?),
			5 => Part::Where(u.arbitrary()?),
			6 => Part::Graph(u.arbitrary()?),
			7 => Part::Method(u.arbitrary()?, atleast_one(u)?),
			8 => Part::Destructure(atleast_one(u)?),
			9 => Part::Optional,
			10 => Part::Recurse(u.arbitrary()?, arb_opt(u, continued_idiom)?, u.arbitrary()?),
			11 => Part::RepeatRecurse,
			12 => Part::Value(u.arbitrary()?),
			_ => unreachable!(),
		};
		Ok(res)
	}
}

pub fn plain_idiom<'a>(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Idiom> {
	let res = match u.int_in_range(0..=1)? {
		0 => Part::Field(u.arbitrary()?),
		1 => Part::Graph(u.arbitrary()?),

		_ => unreachable!(),
	};
	let mut res = vec![res];
	let len = u.arbitrary_len::<Part>()?;
	res.reserve(len);
	for _ in 0..len {
		res.push(u.arbitrary()?);
	}

	Ok(Idiom(res))
}

pub fn local_idiom<'a>(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Idiom> {
	let mut res = vec![Part::Field(u.arbitrary()?)];

	for _ in 0..u.arbitrary_len::<Part>()? {
		match u.int_in_range(0u8..=2)? {
			0 => res.push(Part::Field(u.arbitrary()?)),
			1 => res.push(Part::All),
			2 => {
				let number = match u.int_in_range(0u8..=2)? {
					0 => {
						let n = u.arbitrary::<i64>()?;
						let n = if n < 0 {
							n.saturating_neg()
						} else {
							n
						};
						Literal::Integer(n)
					}
					1 => {
						let n = u.arbitrary::<f64>()?;

						let n = if !n.is_finite() {
							0.0
						} else if n.is_sign_negative() {
							-n
						} else {
							n
						};
						Literal::Float(n)
					}
					2 => {
						let mut n = u.arbitrary::<Decimal>()?;
						n.set_sign_positive(true);
						Literal::Decimal(n)
					}
					_ => unreachable!(),
				};

				res.push(Part::Value(Expr::Literal(number)));
			}
			_ => unreachable!(),
		}
	}

	if u.arbitrary()? {
		res.push(Part::Flatten);
	};
	Ok(Idiom(res))
}

pub fn basic_idiom<'a>(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Idiom> {
	let mut res = vec![Part::Field(u.arbitrary()?)];

	for _ in 0..u.arbitrary_len::<Part>()? {
		match u.int_in_range(0u8..=2)? {
			0 => res.push(Part::Field(u.arbitrary()?)),
			1 => res.push(Part::All),
			2 => {
				let number = match u.int_in_range(0u8..=2)? {
					0 => {
						let n = u.arbitrary::<i64>()?;
						let n = if n < 0 {
							n.saturating_neg()
						} else {
							n
						};
						Literal::Integer(n)
					}
					1 => {
						let n = u.arbitrary::<f64>()?;
						let n = if !n.is_finite() {
							0.0
						} else if n.is_sign_negative() {
							-n
						} else {
							n
						};
						Literal::Float(n)
					}
					2 => {
						let mut n = u.arbitrary::<Decimal>()?;
						n.set_sign_positive(true);
						Literal::Decimal(n)
					}
					_ => unreachable!(),
				};

				res.push(Part::Value(Expr::Literal(number)));
			}
			_ => unreachable!(),
		}
	}

	Ok(Idiom(res))
}

pub fn continued_idiom<'a>(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Idiom> {
	arb_vec1(u, Part::arbitrary).map(Idiom)
}

impl<'a> Arbitrary<'a> for RecurseInstruction {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let r = match u.int_in_range(0u8..=2)? {
			0 => RecurseInstruction::Path {
				inclusive: u.arbitrary()?,
			},
			1 => RecurseInstruction::Collect {
				inclusive: u.arbitrary()?,
			},
			2 => {
				let expects = if u.arbitrary()? {
					Expr::Param(u.arbitrary()?)
				} else {
					Expr::Literal(Literal::RecordId(RecordIdLit {
						table: u.arbitrary()?,
						key: record_id_key_no_range(u)?,
					}))
				};
				RecurseInstruction::Shortest {
					expects,
					inclusive: u.arbitrary()?,
				}
			}
			_ => unreachable!(),
		};
		Ok(r)
	}
}
