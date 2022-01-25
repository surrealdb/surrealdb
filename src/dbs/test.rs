use crate::ctx::Context;
use crate::dbs::executor::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;

pub fn mock<'a>() -> (Runtime, Options<'a>, Executor<'a>) {
	let ctx = Context::default().freeze();
	let opt = Options::default();
	let exe = Executor::new();
	(ctx, opt, exe)
}
