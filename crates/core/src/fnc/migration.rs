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
	let mut path = String::new();

	if let Ok(x) = opts.ns() {
		diagnose_ns(ctx, opts, probe, x, &mut issues, &mut path, &mut export).await?;
	} else {
		opts.is_allowed(Action::View, ResourceKind::Namespace, &Base::Root)?;

		let tx = ctx.tx();

		for ns in tx.all_ns().await?.iter() {
			diagnose_ns(ctx, opts, probe, &ns.name.0, &mut issues, &mut path, &mut export).await?;
		}

		for access in tx.all_root_accesses().await?.iter() {
			let mut pass =
				MigratorPass::new(&mut issues, &mut export, &mut path, PassState::default());
			let _ = pass.visit_define_access(access);
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
	path: &mut String,
	export: &mut String,
) -> Result<(), Error> {
	let opts = opts.clone().with_ns(Some(ns.into()));

	let len = path.len();
	write!(path, "/ns/{ns}").expect("Writing into a string cannot fail");

	if let Ok(db) = opts.db() {
		diagnose_ns_db(ctx, &opts, probe, ns, db, issues, path, export).await?
	} else {
		opts.is_allowed(Action::View, ResourceKind::Database, &Base::Ns)?;

		let tx = ctx.tx();

		for db in tx.all_db(ns).await?.iter() {
			diagnose_ns_db(ctx, &opts, probe, ns, &db.name.0, issues, path, export).await?
		}

		for access in tx.all_ns_accesses(ns).await?.iter() {
			let mut pass = MigratorPass::new(issues, export, path, PassState::default());
			let _ = pass.visit_define_access(access);
		}
	}

	path.truncate(len);
	Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn diagnose_ns_db(
	ctx: &Context,
	opts: &Options,
	_probe: bool,
	ns: &str,
	db: &str,
	issues: &mut Vec<MigrationIssue>,
	path: &mut String,
	export: &mut String,
) -> Result<(), Error> {
	let opts = opts.clone().with_db(Some(db.into()));

	opts.is_allowed(Action::View, ResourceKind::Database, &Base::Db)?;

	let len = path.len();
	write!(path, "/db/{db}").expect("Writing into a string cannot fail");

	let tx = ctx.tx();

	for f in tx.all_db_functions(ns, db).await?.iter() {
		let mut pass = MigratorPass::new(issues, export, path, PassState::default());
		let _ = pass.visit_define_function(f);
	}

	// TODO: No versioning at the moment,
	// Possibly add?
	for t in tx.all_tb(ns, db, None).await?.iter() {
		{
			let mut pass = MigratorPass::new(issues, export, path, PassState::default());
			let _ = pass.visit_define_table(t);
			for f in tx.all_tb_fields(ns, db, &t.name.0, None).await?.iter() {
				let _ = pass.visit_define_field(f);
			}
		}

		let len = path.len();
		write!(path, "/table/{}", &t.name.0).expect("Writing into a string cannot fail");

		{
			let mut pass = MigratorPass::new(issues, export, path, PassState::default());
			for e in tx.all_tb_events(ns, db, &t.name).await?.iter() {
				let _ = pass.visit_define_event(e);
			}
		}

		let begin = thing::prefix(ns, db, &t.name)?;
		let end = thing::suffix(ns, db, &t.name)?;
		let r = tx.scan(begin..end, *MIGRATION_TABLE_PROBE_COUNT, None).await?;
		for (k, v) in r {
			let k = thing::Thing::decode(&k)?;
			let len = path.len();
			write!(path, "/record/{}", k.id).expect("Writing into a string cannot fail");

			let v = revision::from_slice::<Value>(&v)?;

			{
				let mut pass = MigratorPass::new(
					issues,
					export,
					path,
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

	for access in tx.all_db_accesses(ns, db).await?.iter() {
		let mut pass = MigratorPass::new(issues, export, path, PassState::default());
		let _ = pass.visit_define_access(access);
	}

	for api in tx.all_db_apis(ns, db).await?.iter() {
		let mut pass = MigratorPass::new(issues, export, path, PassState::default());
		let _ = pass.visit_api_definition(api);
	}

	for api in tx.all_db_params(ns, db).await?.iter() {
		let mut pass = MigratorPass::new(issues, export, path, PassState::default());
		let _ = pass.visit_define_param(api);
	}

	path.truncate(len);

	Ok(())
}
