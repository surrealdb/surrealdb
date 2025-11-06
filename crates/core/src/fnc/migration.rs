use crate::cnf::MIGRATION_TABLE_PROBE_COUNT;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::key::thing;
use crate::kvs::version::v3::{MigrationIssue, MigratorPass, PassState};
use crate::kvs::KeyDecode;
use crate::sql::visit::Visitor;
use crate::sql::{Base, Value};
use std::fmt::Write;

pub async fn diagnose(
	(ctx, opts): (&Context, &Options),
	(probe,): (Option<bool>,),
) -> Result<Value, Error> {
	let probe = probe.unwrap_or(true);

	let mut issues = Vec::new();
	let mut export = String::new();

	if let Ok(x) = opts.ns() {
		diagnose_ns(ctx, opts, probe, x, &mut issues, &mut export).await?;
	} else {
		opts.is_allowed(Action::View, ResourceKind::Namespace, &Base::Root)?;

		for ns in ctx.tx().all_ns().await?.iter() {
			diagnose_ns(ctx, opts, probe, &ns.name.0, &mut issues, &mut export).await?;
		}
	}

	let res = issues.iter().map(|x| Value::from(x.to_object())).collect::<Value>();

	Ok(res)
}

async fn diagnose_ns(
	ctx: &Context,
	opts: &Options,
	probe: bool,
	ns: &str,
	issues: &mut Vec<MigrationIssue>,
	export: &mut String,
) -> Result<(), Error> {
	let opts = opts.clone().with_ns(Some(ns.into()));

	if let Ok(db) = opts.db() {
		diagnose_ns_db(ctx, &opts, probe, ns, db, issues, export).await?
	} else {
		opts.is_allowed(Action::View, ResourceKind::Database, &Base::Ns)?;

		for db in ctx.tx().all_db(ns).await?.iter() {
			diagnose_ns_db(ctx, &opts, probe, ns, &db.name.0, issues, export).await?
		}
	}
	Ok(())
}

async fn diagnose_ns_db(
	ctx: &Context,
	opts: &Options,
	_probe: bool,
	ns: &str,
	db: &str,
	issues: &mut Vec<MigrationIssue>,
	export: &mut String,
) -> Result<(), Error> {
	let opts = opts.clone().with_db(Some(db.into()));

	opts.is_allowed(Action::View, ResourceKind::Database, &Base::Db)?;

	let mut path = format!("/ns/{ns}/db/{db}");

	let tx = ctx.tx();

	for f in tx.all_db_functions(ns, db).await?.iter() {
		let mut pass = MigratorPass::new(issues, export, &mut path, PassState::default());
		let _ = pass.visit_define_function(f);
	}

	// TODO: No versioning at the moment,
	// Possibly add?
	for t in tx.all_tb(ns, db, None).await?.iter() {
		{
			let mut pass = MigratorPass::new(issues, export, &mut path, PassState::default());
			let _ = pass.visit_define_table(t);
			for f in tx.all_tb_fields(ns, db, &t.name.0, None).await?.iter() {
				let _ = pass.visit_define_field(f);
			}
		}

		let len = path.len();
		write!(&mut path, "/table/{}", &t.name.0).expect("Writing into a string cannot fail");

		let begin = thing::prefix(ns, db, &t.name)?;
		let end = thing::suffix(ns, db, &t.name)?;
		let r = tx.scan(begin..end, *MIGRATION_TABLE_PROBE_COUNT, None).await?;
		for (k, v) in r {
			let k = thing::Thing::decode(&k)?;
			let len = path.len();
			write!(&mut path, "/record/{}", k.id).expect("Writing into a string cannot fail");

			let v = revision::from_slice::<Value>(&v)?;

			{
				let mut pass = MigratorPass::new(
					issues,
					export,
					&mut path,
					PassState {
						breaking_futures: true,
						..PassState::default()
					},
				);
				let _ = pass.visit_value(&v);
				for f in tx.all_tb_fields(ns, db, &t.name.0, None).await?.iter() {
					let _ = pass.visit_define_field(f);
				}
			}

			path.truncate(len);
		}

		path.truncate(len);
	}

	Ok(())
}
