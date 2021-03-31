use crate::dbs::response::{Response, Responses};
use crate::dbs::Process;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::query::Query;
use std::time::Instant;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Executor {
	pub id: String,
	pub ns: String,
	pub db: String,
}

impl Executor {
	pub fn new() -> Executor {
		Executor {
			id: String::from("id"),
			ns: String::from("ns"),
			db: String::from("db"),
		}
	}

	pub fn execute(&self, ctx: &Runtime, qry: Query) -> Result<Responses, Error> {
		let mut r: Vec<Response> = vec![];

		for stm in qry.statements().iter() {
			// Get the statement start time
			let now = Instant::now();
			// Process a single statement
			let res = stm.process(&ctx, self, None);
			// Get the statement end time
			let dur = now.elapsed();

			r.push(Response {
				sql: format!("{}", stm),
				time: format!("{:?}", dur),
				status: match res {
					Ok(_) => String::from("OK"),
					Err(_) => String::from("ERR"),
				},
				result: match res {
					Ok(v) => Some(v),
					Err(_) => None,
				},
			})
		}
		Ok(Responses(r))
	}
}
