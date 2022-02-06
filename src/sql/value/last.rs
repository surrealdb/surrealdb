use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::idiom::Idiom;
use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	pub async fn last(
		&self,
		ctx: &Runtime,
		opt: &Options,
		exe: &Executor<'_>,
	) -> Result<Self, Error> {
		self.get(ctx, opt, exe, &Idiom::from(vec![Part::Last])).await
	}
}
