//! Functions over the labels of a union type of the catalog, which order by declaration.

use anyhow::{Result, bail};

use crate::catalog::providers::TypeProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

/// The labels an enum function works over, in declaration order, and the name an error gives
/// them.
struct Labels {
	name: String,
	labels: Vec<String>,
}

impl Labels {
	fn rank(&self, label: &str) -> Option<usize> {
		self.labels.iter().position(|l| l == label)
	}

	fn describe(&self) -> String {
		self.labels.iter().map(|l| format!("'{l}'")).collect::<Vec<_>>().join(", ")
	}
}

/// The labels `source` names: the union type of that name in the session's database, refused
/// where the session checks permissions and the type is not readable by everyone or is not a
/// union of labels, or, for an array of strings, an inline enum's labels as written.
async fn labels((ctx, opt): (&FrozenContext, &Options), source: &Value) -> Result<Labels> {
	match source {
		Value::Array(items) => {
			let mut labels = Vec::with_capacity(items.len());
			for item in items.iter() {
				let Value::String(label) = item else {
					bail!(
						"An inline enum lists strings, but found {}",
						surrealdb_types::ToSql::to_sql(item)
					);
				};
				labels.push(label.to_string());
			}
			Ok(Labels {
				name: format!(
					"enum<{}>",
					labels.iter().map(|l| format!("'{l}'")).collect::<Vec<_>>().join(", ")
				),
				labels,
			})
		}
		Value::String(name) => {
			let name = name.as_str();
			opt.valid_for_db()?;
			ctx.is_allowed(opt, Action::View, ResourceKind::Parameter, Base::Db)?;
			let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
			let txn = ctx.tx();
			let definition =
				crate::exec::vm::descriptor::object(txn.expect_db_type(ns, db, name)).await?;
			if ctx.check_perms(opt, Action::View)?
				&& !matches!(definition.permissions, crate::catalog::Permission::Full)
			{
				bail!("You don't have permission to read the type '{name}'");
			}
			let Some(labels) = definition.union() else {
				bail!(
					"The type '{name}' is not a union of string labels, so it has no order: it is {}",
					surrealdb_types::ToSql::to_sql(&definition.kind())
				);
			};
			Ok(Labels {
				name: name.to_owned(),
				labels: labels.label_names().map(str::to_owned).collect(),
			})
		}
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Uuid(_)
		| Value::Bytes(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::Object(_)
		| Value::Set(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Closure(_) => bail!(
			"A type is named by a string, but found {}",
			surrealdb_types::ToSql::to_sql(source)
		),
	}
}

/// The rank of `value` among `labels`, an error naming the enum and its labels for a string that
/// is no member.
fn rank_of(labels: &Labels, value: &Value) -> Result<Option<i64>> {
	if matches!(value, Value::None | Value::Null) {
		return Ok(None);
	}
	let Value::String(label) = value else {
		bail!(
			"The type '{}' orders strings, but found {}",
			labels.name,
			surrealdb_types::ToSql::to_sql(value)
		);
	};
	match labels.rank(label.as_str()) {
		Some(rank) => Ok(Some(rank as i64)),
		None => bail!(
			"'{label}' is not a value of the type '{}'; its values are {}",
			labels.name,
			labels.describe()
		),
	}
}

/// `enum::values(name)`: the labels of the enum, in declaration order.
pub async fn values(
	(ctx, opt): (&FrozenContext, Option<&Options>),
	(name,): (Value,),
) -> Result<Value> {
	let Some(opt) = opt else {
		return Ok(Value::None);
	};
	let labels = labels((ctx, opt), &name).await?;
	Ok(Value::Array(labels.labels.into_iter().map(Value::from).collect()))
}

/// `enum::rank(name, value)`: the position of `value` in the declaration order of the enum, from
/// zero; `NONE` for `NONE` and `NULL`.
pub async fn rank(
	(ctx, opt): (&FrozenContext, Option<&Options>),
	(name, value): (Value, Value),
) -> Result<Value> {
	let Some(opt) = opt else {
		return Ok(Value::None);
	};
	let labels = labels((ctx, opt), &name).await?;
	Ok(match rank_of(&labels, &value)? {
		Some(rank) => Value::from(rank),
		None => Value::None,
	})
}

/// The label of `candidates` that ranks first (`want_max` false) or last, `NONE` for no label.
/// `candidates` is a label, or an array of labels; `NONE` and `NULL` take no part.
fn extreme(labels: &Labels, candidates: Value, want_max: bool) -> Result<Value> {
	let items = if let Value::Array(items) = candidates {
		items.0
	} else {
		vec![candidates]
	};
	let mut best: Option<(i64, Value)> = None;
	for item in items {
		let Some(rank) = rank_of(labels, &item)? else {
			continue;
		};
		let better = match &best {
			None => true,
			Some((current, _)) => (rank > *current) == want_max && rank != *current,
		};
		if better {
			best = Some((rank, item));
		}
	}
	Ok(best.map(|(_, label)| label).unwrap_or(Value::None))
}

/// `enum::min(name, values)`: the label that comes first in the declaration order of the enum.
pub async fn min(
	(ctx, opt): (&FrozenContext, Option<&Options>),
	(name, values): (Value, Value),
) -> Result<Value> {
	let Some(opt) = opt else {
		return Ok(Value::None);
	};
	extreme(&labels((ctx, opt), &name).await?, values, false)
}

/// `enum::max(name, values)`: the label that comes last in the declaration order of the enum.
pub async fn max(
	(ctx, opt): (&FrozenContext, Option<&Options>),
	(name, values): (Value, Value),
) -> Result<Value> {
	let Some(opt) = opt else {
		return Ok(Value::None);
	};
	extreme(&labels((ctx, opt), &name).await?, values, true)
}

/// `enum::cast(type, value)`: `value` cast to the type `type` spells, with every named type it
/// holds cast to its body and judged by its checks. A bare name spells the type of that name.
pub async fn cast_in(
	exec: &crate::exec::ExecutionContext,
	(name, value): (Value, Value),
) -> Result<Value> {
	let Value::String(text) = &name else {
		bail!("A type is named by a string, but found {}", surrealdb_types::ToSql::to_sql(&name));
	};
	let kind = crate::syn::kind(text.as_str())?;
	refuse_unreadable_types(exec, &kind).await?;
	let cast = value.cast_to_kind(&kind)?;
	crate::exec::vm::schema_host::cast_types_held(exec, &kind, cast)
		.await
		.map_err(|e| anyhow::anyhow!("Could not cast into `{}`: {e}", text.as_str()))
}

/// Refuses `kind` where the session checks permissions and a type it names is not readable by
/// everyone, as [`labels`] refuses reading one: a cast reveals what the type admits.
async fn refuse_unreadable_types(
	exec: &crate::exec::ExecutionContext,
	kind: &crate::expr::Kind,
) -> Result<()> {
	if !kind.has_named_type() {
		return Ok(());
	}
	let Some(opt) = exec.options() else {
		return Ok(());
	};
	let ctx = exec.ctx();
	if !ctx.check_perms(opt, Action::View)? {
		return Ok(());
	}
	let mut names = Vec::new();
	kind.each_named_type(&mut |name| names.push(name.to_owned()));
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let txn = ctx.tx();
	for name in names {
		let definition =
			crate::exec::vm::descriptor::object(txn.expect_db_type(ns, db, &name)).await?;
		if !matches!(definition.permissions, crate::catalog::Permission::Full) {
			bail!("You don't have permission to read the type '{name}'");
		}
	}
	Ok(())
}

/// `enum::cast(name, value)` where no execution context is at hand (a script): `value` cast to a
/// string that is a label of the union type `name`.
pub async fn cast(
	(ctx, opt): (&FrozenContext, Option<&Options>),
	(name, value): (Value, Value),
) -> Result<Value> {
	let Some(opt) = opt else {
		return Ok(Value::None);
	};
	let labels = labels((ctx, opt), &name).await?;
	let value = value.cast_to_kind(&crate::expr::Kind::String)?;
	let text = if let Value::String(s) = &value {
		s.as_str()
	} else {
		""
	};
	if labels.rank(text).is_none() {
		bail!(
			"Could not cast into `{}` using input `{}`; its values are {}",
			labels.name,
			surrealdb_types::ToSql::to_sql(&value),
			labels.describe()
		);
	}
	Ok(value)
}
