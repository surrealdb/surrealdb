use crate::dbs::res::{Response, Responses};
use crate::err::Error;
use crate::sql::query::Query;
use crate::sql::statement::Statement;
use ctx::Context;
use std::fs;
use std::time::Instant;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct Executor {
	id: String,
	ns: String,
	db: String,
}

impl Executor {
	pub fn new() -> Executor {
		Executor {
			id: String::from("id"),
			ns: String::from("ns"),
			db: String::from("db"),
		}
	}

	pub fn execute(&self, qry: Query) -> Result<Responses, Error> {
		let mut r: Vec<Response> = vec![];

		for stm in qry.statements().iter() {
			let now = Instant::now();
			let res = stm.execute();
			let dur = now.elapsed();
			r.push(Response {
				sql: format!("{}", stm),
				time: format!("{:?}", dur),
				status: String::from(""),
				detail: String::from(""),
				result: Some(res),
			});
		}
		Ok(Responses(r))
	}
}
