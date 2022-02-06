use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::idiom::Idiom;
use crate::sql::part::Part;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use once_cell::sync::Lazy;

static RID: Lazy<Idiom> = Lazy::new(|| Idiom {
	parts: vec![Part::from("id")],
});

static MTB: Lazy<Idiom> = Lazy::new(|| Idiom {
	parts: vec![Part::from("meta"), Part::from("tb")],
});

static MID: Lazy<Idiom> = Lazy::new(|| Idiom {
	parts: vec![Part::from("meta"), Part::from("id")],
});

impl Value {
	pub async fn def(
		&mut self,
		ctx: &Runtime,
		opt: &Options<'_>,
		exe: &Executor<'_>,
		val: Option<&Thing>,
	) -> Result<(), Error> {
		match val {
			Some(id) => {
				let id = id.clone();
				let md = id.clone();
				self.set(ctx, opt, exe, &RID, id.into()).await?;
				self.set(ctx, opt, exe, &MTB, md.tb.into()).await?;
				self.set(ctx, opt, exe, &MID, md.id.into()).await?;
				Ok(())
			}
			None => unreachable!(),
		}
	}
}
