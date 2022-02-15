use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::idiom::Idiom;
use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	pub async fn first(
		&self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction<'_>,
	) -> Result<Self, Error> {
		self.get(ctx, opt, txn, &Idiom::from(vec![Part::First])).await
	}
}
