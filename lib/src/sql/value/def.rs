use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::part::Part;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use once_cell::sync::Lazy;

static RID: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("id")]);
static MTB: Lazy<[Part; 2]> = Lazy::new(|| [Part::from("meta"), Part::from("tb")]);
static MID: Lazy<[Part; 2]> = Lazy::new(|| [Part::from("meta"), Part::from("id")]);

impl Value {
	pub async fn def(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		val: Option<&Thing>,
	) -> Result<(), Error> {
		match val {
			Some(id) => {
				let id = id.clone();
				let md = id.clone();
				self.set(ctx, opt, txn, RID.as_ref(), id.into()).await?;
				self.set(ctx, opt, txn, MTB.as_ref(), md.tb.into()).await?;
				self.set(ctx, opt, txn, MID.as_ref(), md.id.into()).await?;
				Ok(())
			}
			None => unreachable!(),
		}
	}
}
