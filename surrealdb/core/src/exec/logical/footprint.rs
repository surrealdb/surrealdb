//! What a statement reads and writes, as the resources two statements may
//! share: the footprint rewrites across statements consult to tell which
//! orders and eliminations are the program's own.
//!
//! [`EffectSet`](crate::expr::logical::EffectSet) answers what kind of effect
//! one expression has; a footprint answers *where*: which tables' records,
//! which catalog definitions. Two statements whose footprints overlap where
//! one writes are ordered — a read after a write, a write after a read, or two
//! writes — and a rewrite that moves one past the other, drops one, or decides
//! one's outcome before the other runs, changes what the program reads of its
//! own writes. Where they do not overlap, the program cannot tell their order.
//!
//! Writes are named by table, from the statement and what the catalog says it
//! triggers; reads are conservative. A resource a footprint cannot name is
//! widened — to every table, to every definition of a kind, or to
//! [`Resource::Unknown`], which overlaps everything. What a statement triggers is part of it: a
//! write reads its table's catalog, and a table's events, views and reference
//! repairs, custom function bodies and closures invoked by value, run what the
//! footprint cannot see and widen it to `Unknown` unless the catalog shows
//! there is nothing to run ([`expand`]).

// A statement, expression or definition kind this layer does not name is
// read conservatively: as every resource of a kind, or as unknown.
#![expect(clippy::wildcard_enum_match_arm, reason = "unnamed kinds widen the footprint")]

use surrealdb_strand::TableName;

use crate::exec::function::FunctionRegistry;
use crate::expr::logical::{
	EffectSet, LogicalDdlStatement, LogicalExpr, LogicalIfOp, LogicalOp, LogicalWriteSource,
	LogicalWriteStatement, SourceRef,
};
use crate::expr::statements::{AlterStatement, DefineStatement, RemoveStatement};
use crate::expr::{Expr, Function};

/// A kind of catalog definition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CatalogKind {
	/// A table's definition. It covers every definition of the table, since
	/// defining or removing a table defines or removes them.
	Table,
	Field,
	Index,
	Event,
	/// A view defined over a table.
	View,
	Function,
	Param,
	Access,
	/// Anything else: analyzers, sequences, users, APIs, buckets, configs,
	/// modules, models, namespaces and databases.
	Other,
}

/// Something a statement may read or write.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Resource {
	/// Records: of `table`, or of every table when `None`.
	Data {
		table: Option<TableName>,
	},
	/// A catalog definition of `kind`: on `table` where it belongs to one
	/// (every table when `None`), named `name` (every name when `None`).
	Catalog {
		kind: CatalogKind,
		table: Option<TableName>,
		name: Option<String>,
	},
	/// Anything at all.
	Unknown,
}

impl Resource {
	/// Every record of `table`.
	pub(crate) fn table(table: TableName) -> Self {
		Resource::Data {
			table: Some(table),
		}
	}

	/// Every record of every table.
	pub(crate) fn any_data() -> Self {
		Resource::Data {
			table: None,
		}
	}

	/// Every definition of `table`.
	pub(crate) fn table_catalog(table: TableName) -> Self {
		Resource::Catalog {
			kind: CatalogKind::Table,
			table: Some(table),
			name: None,
		}
	}

	/// Whether a statement touching `self` and one touching `other` touch a
	/// resource in common.
	pub(crate) fn overlaps(&self, other: &Resource) -> bool {
		fn same<T: PartialEq>(a: &Option<T>, b: &Option<T>) -> bool {
			match (a, b) {
				(Some(a), Some(b)) => a == b,
				_ => true,
			}
		}
		match (self, other) {
			(Resource::Unknown, _) | (_, Resource::Unknown) => true,
			(
				Resource::Data {
					table: ta,
				},
				Resource::Data {
					table: tb,
				},
			) => same(ta, tb),
			(
				Resource::Catalog {
					kind: ka,
					table: ta,
					name: na,
				},
				Resource::Catalog {
					kind: kb,
					table: tb,
					name: nb,
				},
			) => {
				if !same(ta, tb) {
					return false;
				}
				// A table's definition covers every definition on it.
				let tabled = |kind: &CatalogKind| {
					matches!(
						kind,
						CatalogKind::Field
							| CatalogKind::Index | CatalogKind::Event
							| CatalogKind::View
					)
				};
				match (ka, kb) {
					(CatalogKind::Table, other) | (other, CatalogKind::Table)
						if *other == CatalogKind::Table || tabled(other) =>
					{
						ka != kb || same(na, nb)
					}
					_ => ka == kb && same(na, nb),
				}
			}
			_ => false,
		}
	}
}

/// What a statement reads and writes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Footprint {
	pub(crate) reads: Vec<Resource>,
	pub(crate) writes: Vec<Resource>,
}

impl Footprint {
	/// A footprint touching everything.
	pub(crate) fn unknown() -> Self {
		Self {
			reads: vec![Resource::Unknown],
			writes: vec![Resource::Unknown],
		}
	}

	fn read(&mut self, resource: Resource) {
		if !self.reads.contains(&resource) {
			self.reads.push(resource);
		}
	}

	fn write(&mut self, resource: Resource) {
		if !self.writes.contains(&resource) {
			self.writes.push(resource);
		}
	}

	/// Everything `other` touches too.
	pub(crate) fn absorb(&mut self, other: Footprint) {
		other.reads.into_iter().for_each(|r| self.read(r));
		other.writes.into_iter().for_each(|w| self.write(w));
	}

	/// Whether what this footprint's statements write overlaps what
	/// `moved` reads or writes: whether a rewrite that moves `moved` across
	/// them, or decides its outcome before they run, may change what it reads
	/// or leaves.
	#[cfg_attr(
		not(test),
		allow(dead_code, reason = "the overlap tests state the edge module against it")
	)]
	pub(crate) fn conflicts(&self, moved: &Footprint) -> bool {
		self.writes.iter().any(|w| moved.reads.iter().chain(&moved.writes).any(|r| w.overlaps(r)))
	}

	/// Whether the statement may write a catalog definition, or anything it
	/// cannot name.
	pub(crate) fn writes_catalog(&self) -> bool {
		self.writes.iter().any(|w| matches!(w, Resource::Catalog { .. } | Resource::Unknown))
	}

	/// Whether the statement may write what `read` names.
	#[cfg_attr(
		not(test),
		allow(dead_code, reason = "the overlap tests state the edge module against it")
	)]
	pub(crate) fn writes_over(&self, read: &Resource) -> bool {
		self.writes.iter().any(|w| w.overlaps(read))
	}
}

/// The footprint of `op` as written, before what the catalog says it
/// triggers ([`expand`]). A write's table catalog read and every resource the
/// statement names are in it; what it runs that is not written in it is
/// `Unknown`.
pub(crate) fn of(op: &LogicalOp, registry: &FunctionRegistry) -> Footprint {
	let mut out = Footprint::default();
	walk(op, registry, &mut out);
	out
}

/// The footprint of each arm of the statement `IF` `ifs`, in order, the
/// `ELSE` arm last: what running that arm touches, with the conditions
/// evaluated to reach it. The footprint of the whole `IF`, which [`of`]
/// answers, is their union. A rewrite inside one arm consults that arm's
/// alone; one that moves a statement across the `IF` consults the union.
#[cfg_attr(
	not(test),
	allow(
		dead_code,
		reason = "the per-arm summary RFC §3.4 defines; no rewrite in this build works inside one arm"
	)
)]
pub(crate) fn of_arms(ifs: &LogicalIfOp, registry: &FunctionRegistry) -> Vec<Footprint> {
	let mut conditions = Footprint::default();
	let mut out = Vec::with_capacity(ifs.arms.len());
	for (at, arm) in ifs.arms.iter().enumerate() {
		if let Some(condition) = ifs.conditions.get(at) {
			expr_footprint(condition, registry, &mut conditions);
		}
		let mut footprint = conditions.clone();
		walk(arm, registry, &mut footprint);
		out.push(footprint);
	}
	out
}

fn walk(op: &LogicalOp, registry: &FunctionRegistry, out: &mut Footprint) {
	match op {
		LogicalOp::Source(source) => source_ref(&source.source, out),
		LogicalOp::Write(write) => {
			let targets = write_targets(&write.source);
			let delete = matches!(write.statement, LogicalWriteStatement::Delete(_));
			match targets {
				Some(targets) => {
					for table in targets {
						out.read(Resource::table_catalog(table.clone()));
						// A write reads the records it writes, for its `$before`.
						out.read(Resource::table(table.clone()));
						out.write(Resource::table(table));
					}
				}
				None => {
					out.read(Resource::Catalog {
						kind: CatalogKind::Table,
						table: None,
						name: None,
					});
					out.read(Resource::any_data());
					out.write(Resource::any_data());
				}
			}
			// A delete clears the references to what it deletes, in any table.
			if delete {
				out.write(Resource::any_data());
			}
		}
		LogicalOp::Ddl(ddl) => ddl_footprint(&ddl.statement, out),
		// A fetch follows links to records of any table.
		LogicalOp::Fetch(_) => out.read(Resource::any_data()),
		LogicalOp::Info(_) => out.read(Resource::Catalog {
			kind: CatalogKind::Other,
			table: None,
			name: None,
		}),
		_ => {}
	}
	let _ = op.visit_scalars(&mut |scalar| {
		expr_footprint(scalar, registry, out);
		Ok::<(), std::convert::Infallible>(())
	});
	for child in op.children() {
		walk(child, registry, out);
	}
}

/// The tables a write writes; `None` where a target is evaluated.
pub(crate) fn write_targets(source: &LogicalWriteSource) -> Option<Vec<TableName>> {
	match source {
		LogicalWriteSource::Targets(targets) => {
			let mut out = Vec::new();
			for target in targets {
				match target {
					LogicalExpr::Table(table) => out.push(table.clone()),
					LogicalExpr::RecordId(rid) => out.push(rid.table.clone()),
					_ => return None,
				}
			}
			Some(out)
		}
		LogicalWriteSource::Inserts(inserts) => match &inserts.into {
			Some(LogicalExpr::Table(table)) => Some(vec![table.clone()]),
			_ => None,
		},
		LogicalWriteSource::Rows(rows) => {
			let mut tables = Vec::new();
			let mut named = true;
			let _ = rows.visit(&mut |op| {
				if let LogicalOp::Source(source) = op {
					match &source.source {
						SourceRef::Table(table) => tables.push(table.clone()),
						SourceRef::RecordId(rid) => tables.push(rid.table.clone()),
						_ => named = false,
					}
				}
				std::ops::ControlFlow::<()>::Continue(())
			});
			named.then_some(tables)
		}
	}
}

fn source_ref(source: &SourceRef, out: &mut Footprint) {
	match source {
		SourceRef::Table(table) => {
			out.read(Resource::table_catalog(table.clone()));
			out.read(Resource::table(table.clone()));
		}
		SourceRef::RecordId(rid) => {
			out.read(Resource::table_catalog(rid.table.clone()));
			out.read(Resource::table(rid.table.clone()));
		}
		SourceRef::Mock(_) => {}
		// A parameter or expression may name any table.
		SourceRef::Param(_) | SourceRef::Dynamic(_) => {
			out.read(Resource::Catalog {
				kind: CatalogKind::Table,
				table: None,
				name: None,
			});
			out.read(Resource::any_data());
		}
	}
}

/// The footprint of evaluating `expr`, and of every statement written in it.
fn expr_footprint(expr: &LogicalExpr, registry: &FunctionRegistry, out: &mut Footprint) {
	let _ = expr.visit(&mut |e| {
		match e {
			LogicalExpr::Subquery(op) => walk(op, registry, out),
			LogicalExpr::Call(call) => match &call.receiver {
				Function::Normal(name) => {
					let effects =
						registry.get(name).map(|f| f.effects()).unwrap_or(EffectSet::OPAQUE_BODY);
					if effects.intersects(EffectSet::WRITES_DATA.union(EffectSet::OPAQUE_BODY)) {
						out.absorb(Footprint::unknown());
					}
					if effects.intersects(EffectSet::READS_DATA) {
						out.read(Resource::any_data());
					}
					if effects.intersects(EffectSet::READS_CATALOG) {
						out.read(Resource::Catalog {
							kind: CatalogKind::Other,
							table: None,
							name: None,
						});
					}
					if effects.intersects(EffectSet::ADVANCES_STATE) {
						out.write(Resource::Catalog {
							kind: CatalogKind::Other,
							table: None,
							name: None,
						});
					}
				}
				Function::Custom(name) => {
					out.read(Resource::Catalog {
						kind: CatalogKind::Function,
						table: None,
						name: Some(name.clone()),
					});
					// Its body is the catalog's; what it touches is not written here.
					out.absorb(Footprint::unknown());
				}
				_ => out.absorb(Footprint::unknown()),
			},
			// A value invoked, or a lookup over edges the expression may not
			// name: what they run or read is not written here.
			LogicalExpr::Invoke {
				..
			} => out.absorb(Footprint::unknown()),
			LogicalExpr::Param(param) => out.read(Resource::Catalog {
				kind: CatalogKind::Param,
				table: None,
				name: Some(param.as_str().to_owned()),
			}),
			// Nodes that read nothing beyond their children and the row.
			LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Field(_)
			| LogicalExpr::Current
			| LogicalExpr::Prefix {
				..
			}
			| LogicalExpr::Postfix {
				..
			}
			| LogicalExpr::Binary {
				..
			}
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue => {}
			// A path into a value, a lookup, a method: any of them may follow
			// a record link, not typed to a table here, to a record of any
			// table.
			_ => out.read(Resource::any_data()),
		}
		std::ops::ControlFlow::<()>::Continue(())
	});
}

/// The name an identifier or string literal gives by being written.
pub(crate) fn written(expr: &Expr) -> Option<String> {
	match expr {
		Expr::Literal(crate::expr::Literal::String(s)) => Some(s.as_str().to_owned()),
		expr => crate::ddl::scalar::written_name(expr),
	}
}

/// The table schema statement `statement` defines where there is none, as a
/// write defines the table it writes to (`get_or_add_tb`): a `DEFINE FIELD`,
/// `DEFINE INDEX` or `DEFINE EVENT` over a table it names as written. Such a
/// statement does the same whether the table it finds was defined by that
/// write or by its own run.
pub(crate) fn defines_on_demand(statement: &LogicalDdlStatement) -> Option<TableName> {
	let LogicalDdlStatement::Define(define) = statement else {
		return None;
	};
	let table = match define.as_ref() {
		DefineStatement::Field(stmt) => &stmt.what,
		DefineStatement::Index(stmt) => &stmt.what,
		DefineStatement::Event(stmt) => &stmt.target_table,
		DefineStatement::Namespace(_)
		| DefineStatement::Database(_)
		| DefineStatement::Function(_)
		| DefineStatement::Analyzer(_)
		| DefineStatement::Param(_)
		| DefineStatement::Table(_)
		| DefineStatement::User(_)
		| DefineStatement::Model(_)
		| DefineStatement::Access(_)
		| DefineStatement::Config(_)
		| DefineStatement::Api(_)
		| DefineStatement::Bucket(_)
		| DefineStatement::Sequence(_)
		| DefineStatement::Module(_) => return None,
	};
	written(table).map(|table| TableName::new(&table))
}

fn ddl_footprint(statement: &LogicalDdlStatement, out: &mut Footprint) {
	let catalog =
		|kind: CatalogKind, table: Option<&Expr>, name: Option<String>| Resource::Catalog {
			kind,
			table: table.and_then(written).map(|t| TableName::new(&t)),
			name,
		};
	match statement {
		LogicalDdlStatement::Define(define) => match define.as_ref() {
			DefineStatement::Table(stmt) => {
				let table = written(&stmt.name).map(|t| TableName::new(&t));
				out.write(Resource::Catalog {
					kind: CatalogKind::Table,
					table: table.clone(),
					name: None,
				});
				out.read(Resource::Catalog {
					kind: CatalogKind::Table,
					table: table.clone(),
					name: None,
				});
				// A view is filled from its sources, into its table.
				if let Some(view) = &stmt.view {
					for source in &view.what {
						out.read(Resource::table(source.clone()));
						out.write(Resource::Catalog {
							kind: CatalogKind::View,
							table: Some(source.clone()),
							name: None,
						});
					}
					out.write(match table {
						Some(table) => Resource::table(table),
						None => Resource::any_data(),
					});
				}
			}
			DefineStatement::Field(stmt) => {
				let entry = catalog(CatalogKind::Field, Some(&stmt.what), written(&stmt.name));
				out.read(entry.clone());
				// Its checks of the definition read the table, its other
				// fields and its indexes.
				out.read(catalog(CatalogKind::Table, Some(&stmt.what), None));
				out.write(entry);
				// It defines its table where there is none.
				out.write(catalog(CatalogKind::Table, Some(&stmt.what), None));
			}
			DefineStatement::Index(stmt) => {
				let entry = catalog(CatalogKind::Index, Some(&stmt.what), written(&stmt.name));
				out.read(entry.clone());
				out.write(entry);
				out.write(catalog(CatalogKind::Table, Some(&stmt.what), None));
				// Its build reads the rows, and a unique index may refuse them.
				out.read(match written(&stmt.what) {
					Some(table) => Resource::table(TableName::new(&table)),
					None => Resource::any_data(),
				});
			}
			DefineStatement::Event(stmt) => {
				let entry =
					catalog(CatalogKind::Event, Some(&stmt.target_table), written(&stmt.name));
				out.read(entry.clone());
				out.write(entry);
				out.write(catalog(CatalogKind::Table, Some(&stmt.target_table), None));
			}
			DefineStatement::Function(stmt) => {
				let entry = Resource::Catalog {
					kind: CatalogKind::Function,
					table: None,
					name: Some(stmt.name.as_str().to_owned()),
				};
				out.read(entry.clone());
				out.write(entry);
			}
			DefineStatement::Param(stmt) => {
				let entry = Resource::Catalog {
					kind: CatalogKind::Param,
					table: None,
					name: Some(stmt.name.as_str().to_owned()),
				};
				out.read(entry.clone());
				out.write(entry);
			}
			DefineStatement::Access(_) | DefineStatement::User(_) => {
				out.read(catalog(CatalogKind::Access, None, None));
				out.write(catalog(CatalogKind::Access, None, None));
			}
			// A namespace or database changes what every name resolves to.
			DefineStatement::Namespace(_) | DefineStatement::Database(_) => {
				out.absorb(Footprint::unknown())
			}
			_ => {
				out.read(catalog(CatalogKind::Other, None, None));
				out.write(catalog(CatalogKind::Other, None, None));
			}
		},
		LogicalDdlStatement::Remove(remove) => match remove.as_ref() {
			RemoveStatement::Table(stmt) => {
				let table = written(&stmt.name).map(|t| TableName::new(&t));
				out.read(Resource::Catalog {
					kind: CatalogKind::Table,
					table: table.clone(),
					name: None,
				});
				out.write(Resource::Catalog {
					kind: CatalogKind::Table,
					table: table.clone(),
					name: None,
				});
				out.write(match table {
					Some(table) => Resource::table(table),
					None => Resource::any_data(),
				});
			}
			RemoveStatement::Field(stmt) => {
				let entry =
					catalog(CatalogKind::Field, Some(&stmt.table_name), written(&stmt.name));
				out.read(entry.clone());
				out.write(entry);
			}
			RemoveStatement::Index(stmt) => {
				let entry = catalog(CatalogKind::Index, Some(&stmt.what), written(&stmt.name));
				out.read(entry.clone());
				out.write(entry);
			}
			RemoveStatement::Event(stmt) => {
				let entry =
					catalog(CatalogKind::Event, Some(&stmt.table_name), written(&stmt.name));
				out.read(entry.clone());
				out.write(entry);
			}
			RemoveStatement::Function(stmt) => {
				let entry = Resource::Catalog {
					kind: CatalogKind::Function,
					table: None,
					name: Some(stmt.name.as_str().to_owned()),
				};
				out.read(entry.clone());
				out.write(entry);
			}
			RemoveStatement::Param(stmt) => {
				let entry = Resource::Catalog {
					kind: CatalogKind::Param,
					table: None,
					name: Some(stmt.name.as_str().to_owned()),
				};
				out.read(entry.clone());
				out.write(entry);
			}
			RemoveStatement::Namespace(_) | RemoveStatement::Database(_) => {
				out.absorb(Footprint::unknown())
			}
			_ => {
				out.read(catalog(CatalogKind::Other, None, None));
				out.write(catalog(CatalogKind::Other, None, None));
			}
		},
		LogicalDdlStatement::Alter(alter) => match alter.as_ref() {
			AlterStatement::Table(stmt) => {
				let entry = catalog(CatalogKind::Table, Some(&stmt.name), None);
				out.read(entry.clone());
				out.write(entry);
			}
			AlterStatement::Field(stmt) => {
				let entry = catalog(CatalogKind::Field, Some(&stmt.what), written(&stmt.name));
				out.read(entry.clone());
				out.write(entry);
			}
			AlterStatement::Index(stmt) => {
				let entry = catalog(CatalogKind::Index, Some(&stmt.table), written(&stmt.name));
				out.read(entry.clone());
				out.write(entry);
			}
			AlterStatement::Event(stmt) => {
				let entry = catalog(CatalogKind::Event, Some(&stmt.what), written(&stmt.name));
				out.read(entry.clone());
				out.write(entry);
			}
			AlterStatement::Function(stmt) => {
				let entry = Resource::Catalog {
					kind: CatalogKind::Function,
					table: None,
					name: Some(stmt.name.as_str().to_owned()),
				};
				out.read(entry.clone());
				out.write(entry);
			}
			AlterStatement::Param(stmt) => {
				let entry = Resource::Catalog {
					kind: CatalogKind::Param,
					table: None,
					name: Some(stmt.name.as_str().to_owned()),
				};
				out.read(entry.clone());
				out.write(entry);
			}
			AlterStatement::System(_)
			| AlterStatement::Namespace(_)
			| AlterStatement::Database(_) => out.absorb(Footprint::unknown()),
			_ => {
				out.read(catalog(CatalogKind::Other, None, None));
				out.write(catalog(CatalogKind::Other, None, None));
			}
		},
		// A rebuild rewrites an index's entries from the rows.
		LogicalDdlStatement::Rebuild(_) => {
			out.read(Resource::any_data());
			out.write(Resource::any_data());
			out.write(catalog(CatalogKind::Index, None, None));
		}
	}
}

/// What the catalog says a statement with footprint `fp` triggers, added to
/// it: a table read whose reads evaluate a permission predicate or a computed
/// field reads what those may, a table written that the catalog does not
/// define is defined by the write, a table written that runs events or holds views over it runs
/// what the footprint cannot see, and a delete from a table other tables reference
/// runs their repairs, which may run statements. `catalog` answers for the
/// tables as the program being planned leaves them.
pub(crate) async fn expand(fp: &mut Footprint, catalog: &dyn CatalogView) {
	// A table whose reads evaluate what the catalog holds — a permission
	// predicate, a computed field — reads whatever that reads.
	let read: Vec<TableName> = fp
		.reads
		.iter()
		.filter_map(|r| match r {
			Resource::Data {
				table: Some(table),
				..
			} => Some(table.clone()),
			_ => None,
		})
		.collect();
	for table in read {
		if catalog.reads_through(&table).await {
			fp.read(Resource::Unknown);
			break;
		}
	}
	let mut triggers = false;
	let written: Vec<Option<TableName>> = fp
		.writes
		.iter()
		.filter_map(|w| match w {
			Resource::Data {
				table,
				..
			} => Some(table.clone()),
			_ => None,
		})
		.collect();
	for table in written {
		match table {
			Some(table) => {
				// A write defines the table it writes to when there is none.
				if !catalog.defines(&table).await {
					fp.write(Resource::table_catalog(table.clone()));
				}
				let (events, views) = catalog.triggers(&table).await;
				triggers |= events;
				if views {
					// A view's table is written by the view's maintenance.
					fp.write(Resource::any_data());
				}
			}
			None => triggers |= catalog.any_triggers().await,
		}
	}
	if triggers {
		fp.absorb(Footprint::unknown());
	}
}

/// The catalog a footprint's expansion reads.
pub(crate) trait CatalogView: Send + Sync {
	/// Whether `table` runs events when written, and whether views are
	/// defined over it. Answers `(true, true)` when it cannot tell.
	fn triggers<'a>(&'a self, table: &'a TableName) -> BoxFuture<'a, (bool, bool)>;
	/// Whether reading `table` evaluates a body the catalog holds: a select
	/// permission predicate, a computed field, a field's select permission.
	/// Answers `true` when it cannot tell.
	fn reads_through<'a>(&'a self, table: &'a TableName) -> BoxFuture<'a, bool>;
	/// Whether the catalog defines `table`. Answers `false` when it cannot
	/// tell.
	fn defines<'a>(&'a self, table: &'a TableName) -> BoxFuture<'a, bool>;
	/// Whether any table runs events, or any reference repair runs a
	/// statement, when written. Answers `true` when it cannot tell.
	fn any_triggers(&self) -> BoxFuture<'_, bool>;
}

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

#[cfg(test)]
mod tests {
	use super::*;

	fn data(table: &str) -> Resource {
		Resource::table(TableName::new(table))
	}

	#[test]
	fn records_of_different_tables_do_not_overlap() {
		assert!(data("a").overlaps(&data("a")));
		assert!(!data("a").overlaps(&data("b")));
		assert!(Resource::any_data().overlaps(&data("b")));
		assert!(Resource::Unknown.overlaps(&Resource::table_catalog(TableName::new("x"))));
	}

	#[test]
	fn a_table_definition_covers_the_definitions_on_it() {
		let field = Resource::Catalog {
			kind: CatalogKind::Field,
			table: Some(TableName::new("t")),
			name: Some("a".into()),
		};
		let other_field = Resource::Catalog {
			kind: CatalogKind::Field,
			table: Some(TableName::new("t")),
			name: Some("b".into()),
		};
		let function = Resource::Catalog {
			kind: CatalogKind::Function,
			table: None,
			name: Some("f".into()),
		};
		assert!(Resource::table_catalog(TableName::new("t")).overlaps(&field));
		assert!(!Resource::table_catalog(TableName::new("u")).overlaps(&field));
		assert!(!field.overlaps(&other_field));
		assert!(!field.overlaps(&function));
		assert!(!Resource::table_catalog(TableName::new("t")).overlaps(&function));
		assert!(!data("t").overlaps(&field));
	}

	/// Each arm of a statement `IF` has its own footprint: a write in one arm
	/// is not a write of another, and each arm reads the conditions before it.
	#[test]
	fn each_arm_of_an_if_has_its_own_footprint() {
		let registry = FunctionRegistry::with_builtins();
		let LogicalOp::If(ifs) = super::super::lower_test(
			"IF (SELECT * FROM c) { UPDATE a SET x = 1 } ELSE { SELECT * FROM b }",
		) else {
			panic!("a statement IF");
		};
		let arms = of_arms(&ifs, &registry);
		assert_eq!(arms.len(), 2);
		assert!(arms[0].writes_over(&data("a")));
		assert!(!arms[1].writes_over(&data("a")), "the ELSE arm writes nothing");
		for arm in &arms {
			assert!(
				arm.reads.iter().any(|r| r.overlaps(&data("c"))),
				"both arms read the condition"
			);
		}
		assert!(!arms[0].reads.iter().any(|r| r.overlaps(&data("b"))));
		let whole = of(&LogicalOp::If(ifs), &registry);
		assert!(whole.writes_over(&data("a")));
		assert!(whole.reads.iter().any(|r| r.overlaps(&data("b"))));
	}

	#[test]
	fn a_write_conflicts_with_what_reads_or_writes_its_resources() {
		let write_a = Footprint {
			reads: vec![],
			writes: vec![data("a")],
		};
		let read_a = Footprint {
			reads: vec![data("a")],
			writes: vec![],
		};
		let write_b = Footprint {
			reads: vec![],
			writes: vec![data("b")],
		};
		assert!(write_a.conflicts(&read_a));
		assert!(write_a.conflicts(&write_a));
		assert!(!write_a.conflicts(&write_b));
		assert!(!read_a.conflicts(&write_a));
		assert!(Footprint::unknown().conflicts(&read_a));
		assert!(!write_b.conflicts(&read_a));
	}
}
