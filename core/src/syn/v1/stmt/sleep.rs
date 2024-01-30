use super::super::{comment::shouldbespace, literal::duration, IResult};
use crate::sql::statements::SleepStatement;
use nom::bytes::complete::tag_no_case;

pub fn sleep(i: &str) -> IResult<&str, SleepStatement> {
	let (i, _) = tag_no_case("SLEEP")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = duration(i)?;
	Ok((
		i,
		SleepStatement {
			duration: v,
		},
	))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_sleep_statement_sec() {
		let sql = "SLEEP 2s";
		let res = sleep(sql);
		let out = res.unwrap().1;
		assert_eq!("SLEEP 2s", format!("{}", out))
	}

	#[test]
	fn test_sleep_statement_ms() {
		let sql = "SLEEP 500ms";
		let res = sleep(sql);
		let out = res.unwrap().1;
		assert_eq!("SLEEP 500ms", format!("{}", out))
	}
}
