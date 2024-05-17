use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::dbs::{Statement, Workable};
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn relation(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		let tb = self.tb(ctx, opt).await?;

		let rid = self.id.as_ref().unwrap();
		match stm {
			Statement::Create(_) => {
				if !tb.allows_normal() {
					return Err(Error::TableCheck {
						thing: rid.to_string(),
						relation: false,
						target_type: tb.kind.clone(),
					});
				}
			}
			Statement::Insert(_) => match self.extras {
				Workable::Relate(_, _, _) => {
					if !tb.allows_relation() {
						return Err(Error::TableCheck {
							thing: rid.to_string(),
							relation: true,
							target_type: tb.kind.clone(),
						});
					}
				}
				_ => {
					if !tb.allows_normal() {
						return Err(Error::TableCheck {
							thing: rid.to_string(),
							relation: false,
							target_type: tb.kind.clone(),
						});
					}
				}
			},
			Statement::Relate(_) => {
				if !tb.allows_relation() {
					return Err(Error::TableCheck {
						thing: rid.to_string(),
						relation: true,
						target_type: tb.kind.clone(),
					});
				}
			}
			_ => {}
		}
		// Carry on
		Ok(())
	}
}
