//! The transactional halves of the statements that change the request's session.
//!
//! `OPTION`, `USE` and a session `LET` each have a part that belongs to the statement's
//! transaction (an authorisation, a catalog creation, the evaluation of an operand) and a part
//! that belongs to the executor that owns the session (`Options`, the `$session` value, the
//! request's parameters). The functions here are the first parts and the only copy of them: the
//! executor calls them for a statement it runs as written, and a program's store instructions call
//! them for the same statement lowered, so the checks, their order and their error text are defined
//! once.
//!
//! The second parts are [`SessionEffect`]s. A body hands each effect to its caller only after the
//! work that precedes it has succeeded, so a statement that fails part-way leaves only the effects
//! it reached before the failure.

use anyhow::{Result, bail};
use surrealdb_strand::Strand;

use crate::catalog::providers::{CatalogProvider, DatabaseProvider, NamespaceProvider};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::exec::Error as ExecError;
use crate::exec::context::SessionEffect;
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::kvs::Transaction;
use crate::val::Value;

/// The check an `OPTION` makes before it changes anything: the principal may edit options on the
/// database.
pub(crate) fn option_allowed(ctx: &Context, opt: &Options) -> Result<()> {
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Option, Base::Db)
}

/// The name a `USE` operand gives: `value`, the evaluated operand, read as a string whose refusal
/// names `into` (`"namespace"` or `"database"`).
pub(crate) fn use_name(value: Value, into: &str) -> Result<String> {
	crate::ddl::scalar::name_of(value, into)
}

/// The row a `USE` answers: the namespace and database the session selects once it has run.
pub(crate) fn use_answer(ns: Option<&str>, db: Option<&str>) -> Value {
	Value::from(map! {
		"namespace" => ns.map(|x| Value::String(x.into())).unwrap_or(Value::None),
		"database" => db.map(|x| Value::String(x.into())).unwrap_or(Value::None),
	})
}

/// Runs the transactional half of a `USE` in `txn`: the authorisation of the target, the creation
/// of the namespace and database it names, and the refusal of a database with no namespace.
///
/// `use_ns` and `use_db` are the names the statement selects (`None` where it leaves the level
/// alone), and `opt` is the session before the statement. `on_level` is called once per level,
/// namespace first, after the catalog work of that level has succeeded; it receives a
/// [`SessionEffect::Use`] naming that level only. When the database level runs after a namespace
/// level, it creates its database under the namespace that level selected, not the session's.
///
/// SECURITY: `USE` moves the session's selected namespace and database to caller-supplied operands,
/// so the authenticated level, not the selection about to be replaced, is the source of truth for
/// tenant scope. The principal must be authorised for the target before anything is created or
/// recorded, and a level it may not reach is refused here. Creating a namespace or database through
/// `USE` requires the authorisation that `DEFINE NAMESPACE` and `DEFINE DATABASE` require.
/// Re-selecting an existing one is unrestricted. A missing one the principal may not create
/// surfaces as `NsNotFound` or `DbNotFound` where it is used, never as a resource created for a
/// session that was not allowed to create it.
pub(crate) async fn use_targets(
	ctx: &Context,
	opt: &Options,
	txn: &Transaction,
	use_ns: Option<String>,
	use_db: Option<String>,
	mut on_level: impl FnMut(SessionEffect),
) -> Result<()> {
	// A switch that only changes one of ns/db keeps the other from the current session. Root and
	// anonymous (`Level::No`) principals are unrestricted by `can_access_ns_db`.
	let target_ns = use_ns.as_deref().or(opt.ns.as_deref());
	let target_db = use_db.as_deref().or(opt.db.as_deref());
	if let Some(ns_name) = target_ns {
		let db_name = target_db.unwrap_or_default();
		if !opt.auth.can_access_ns_db(ns_name, db_name) {
			// Report the narrowest boundary that failed: if the namespace itself is in scope, the
			// database is the offending operand, otherwise the namespace is.
			let err = if opt.auth.level().ns() == Some(ns_name) {
				ExecError::DbNotAllowed {
					db: db_name.to_owned(),
				}
			} else {
				ExecError::NsNotAllowed {
					ns: ns_name.to_owned(),
				}
			};
			return Err(anyhow::Error::new(err));
		}
	}

	let create_ns = if let Some(ns_name) = use_ns.as_deref() {
		txn.get_ns_by_name(ns_name, None).await?.is_some()
			|| (!ctx.auth_enabled() && opt.auth.is_anon())
			|| opt.auth.is_allowed(Action::Edit, &ResourceKind::Namespace.on_root()).is_ok()
	} else {
		false
	};
	let create_db = if let Some(db_name) = use_db.as_deref() {
		let target_ns = use_ns.as_deref().or(opt.ns.as_deref());
		if let Some(ns_name) = target_ns {
			if txn.get_db_by_name(ns_name, db_name, None).await?.is_some()
				|| (!ctx.auth_enabled() && opt.auth.is_anon())
			{
				true
			} else if txn.get_ns_by_name(ns_name, None).await?.is_none()
				&& opt.auth.is_allowed(Action::Edit, &ResourceKind::Namespace.on_root()).is_err()
			{
				// `ensure_ns_db` creates a missing parent namespace as a side effect
				// (`get_or_add_db_upwards` with `upwards`). A principal that may create
				// databases but not namespaces, including one whose token names a dropped
				// namespace, must not recreate that parent through `USE NS dropped DB anything`
				// or `USE DB anything`, so the parent is left alone and the missing database
				// surfaces as `DbNotFound` where it is used.
				false
			} else {
				opt.auth.is_allowed(Action::Edit, &ResourceKind::Database.on_ns(ns_name)).is_ok()
			}
		} else {
			false
		}
	} else {
		false
	};

	// The namespace the database level runs under: the session's, until a namespace level of this
	// statement selects another.
	let mut selected: Option<String> = opt.ns.as_deref().map(str::to_owned);

	if let Some(ns) = use_ns {
		if create_ns {
			txn.get_or_add_ns(Some(ctx), &ns).await?;
		}
		on_level(SessionEffect::Use {
			ns: Some(Strand::from(ns.as_str())),
			db: None,
		});
		selected = Some(ns);
	}

	if let Some(db) = use_db {
		let Some(ns) = selected.as_deref() else {
			bail!("Cannot use database without namespace");
		};
		if create_db {
			txn.ensure_ns_db(Some(ctx), ns, &db).await?;
		}
		on_level(SessionEffect::Use {
			ns: None,
			db: Some(Strand::from(db.as_str())),
		});
	}
	Ok(())
}
