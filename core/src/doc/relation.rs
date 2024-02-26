#![cfg(feature = "sql2")]

use crate::ctx::Context;
use crate::dbs::Statement;
use crate::dbs::{Options, Transaction};
use crate::doc::Document;
use crate::err::Error;
use crate::sql::TableType;

impl<'a> Document<'a> {
	pub async fn relation(
		&mut self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		let table_type = if let Statement::Relate(_) = stm {
			TableType::Relation(Default::default())
		} else {
			TableType::Normal
		};
		// TODO: Implicit table definition doesn't define in/out fields
		let tb = self.tb_with_rel(opt, txn, table_type).await?;

		let rid = self.id.as_ref().unwrap();
		match stm {
			Statement::Create(_) | Statement::Insert(_) => {
				if !tb.allows_normal() {
					return Err(Error::TableCheck {
						thing: rid.to_string(),
						relation: false,
					});
				}
			}
			Statement::Relate(_) => {
				if !tb.allows_relation() {
					return Err(Error::TableCheck {
						thing: rid.to_string(),
						relation: true,
					});
				}
			}
			_ => {}
		}
		// Carry on
		Ok(())
	}
}
