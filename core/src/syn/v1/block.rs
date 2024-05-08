use super::{
	comment::mightbespace,
	common::{closebraces, colons, expect_delimited, openbraces},
	stmt::{
		create, define, delete, foreach, ifelse, insert, output, r#break, r#continue, rebuild,
		relate, remove, select, set, throw, update,
	},
	value::value,
	IResult,
};
use crate::sql::{block::Entry, Block};
use nom::{
	branch::alt,
	combinator::map,
	multi::{many0, separated_list0},
	sequence::delimited,
};

pub fn block(i: &str) -> IResult<&str, Block> {
	expect_delimited(
		openbraces,
		|i| {
			let (i, v) = separated_list0(colons, entry)(i)?;
			let (i, _) = many0(colons)(i)?;
			Ok((i, Block(v)))
		},
		closebraces,
	)(i)
}

pub fn entry(i: &str) -> IResult<&str, Entry> {
	delimited(
		mightbespace,
		alt((
			map(set, Entry::Set),
			map(output, Entry::Output),
			map(ifelse, Entry::Ifelse),
			map(select, Entry::Select),
			map(create, Entry::Create),
			map(update, Entry::Update),
			map(relate, Entry::Relate),
			map(delete, Entry::Delete),
			map(insert, Entry::Insert),
			map(define, Entry::Define),
			map(rebuild, Entry::Rebuild),
			map(remove, Entry::Remove),
			map(throw, Entry::Throw),
			map(r#break, Entry::Break),
			map(r#continue, Entry::Continue),
			map(foreach, Entry::Foreach),
			map(value, Entry::Value),
		)),
		mightbespace,
	)(i)
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn block_empty() {
		let sql = "{}";
		let res = block(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn block_value() {
		let sql = "{ 80 }";
		let res = block(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn block_ifelse() {
		let sql = "{ RETURN IF true THEN 50 ELSE 40 END; }";
		let res = block(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn block_multiple() {
		let sql = r#"{

	LET $person = (SELECT * FROM person WHERE first = $first AND last = $last AND birthday = $birthday);

	RETURN IF $person[0].id THEN
		$person[0]
	ELSE
		(CREATE person SET first = $first, last = $last, birthday = $birthday)
	END;

}"#;
		let res = block(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{:#}", out))
	}
}
