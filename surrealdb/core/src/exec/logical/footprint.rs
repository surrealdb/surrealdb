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
//! [`Resource::Unknown`], which overlaps everything. What a statement
//! triggers is part of its footprint: a write reads its table's catalog. A
//! table's events, views and reference repairs, and custom function bodies and
//! closures invoked by value, run code the footprint cannot see, so it widens
//! to `Unknown` unless the catalog shows there is nothing to run ([`expand`]).

#![deny(clippy::wildcard_enum_match_arm)]

use surrealdb_strand::TableName;

use crate::exec::function::FunctionRegistry;
use crate::expr::logical::{
	EffectSet, LogicalDdlStatement, LogicalExpr, LogicalIfOp, LogicalOp, LogicalWrite,
	LogicalWriteSource, LogicalWriteStatement, SourceRef,
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
			if let (Some(a), Some(b)) = (a, b) {
				a == b
			} else {
				true
			}
		}
		if matches!(self, Resource::Unknown) || matches!(other, Resource::Unknown) {
			return true;
		}
		if let (
			Resource::Data {
				table: ta,
			},
			Resource::Data {
				table: tb,
			},
		) = (self, other)
		{
			return same(ta, tb);
		}
		if let (
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
		) = (self, other)
		{
			if !same(ta, tb) {
				return false;
			}
			// A table's definition covers every definition on it.
			let tabled = |kind: &CatalogKind| {
				matches!(
					kind,
					CatalogKind::Field
						| CatalogKind::Index
						| CatalogKind::Event
						| CatalogKind::View
				)
			};
			return if matches!(ka, CatalogKind::Table) && (*kb == CatalogKind::Table || tabled(kb))
				|| matches!(kb, CatalogKind::Table) && (*ka == CatalogKind::Table || tabled(ka))
			{
				ka != kb || same(na, nb)
			} else {
				ka == kb && same(na, nb)
			};
		}
		false
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
#[cfg_attr(
	not(test),
	allow(
		dead_code,
		reason = "the footprint of a whole plan; the engine asks for a write's by reference (`of_write`)"
	)
)]
pub(crate) fn of(op: &LogicalOp, registry: &FunctionRegistry) -> Footprint {
	let mut out = Footprint::default();
	walk(op, registry, &mut out);
	out
}

/// [`of`] for the write `write` as a plan on its own, without building the plan around it.
pub(crate) fn of_write(write: &LogicalWrite, registry: &FunctionRegistry) -> Footprint {
	let mut out = Footprint::default();
	walk_write(write, registry, &mut out);
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
	if let LogicalOp::Source(source) = op {
		source_ref(&source.source, out);
	} else if let LogicalOp::Write(write) = op {
		write_footprint(write, out);
	} else if let LogicalOp::Ddl(ddl) = op {
		ddl_footprint(&ddl.statement, out);
	} else if let LogicalOp::Fetch(_) = op {
		// A fetch follows links to records of any table.
		out.read(Resource::any_data());
	} else if let LogicalOp::Info(_) = op {
		out.read(Resource::Catalog {
			kind: CatalogKind::Other,
			table: None,
			name: None,
		});
	}
	let _ = op.visit_scalars(&mut |scalar| {
		expr_footprint(scalar, registry, out);
		Ok::<(), std::convert::Infallible>(())
	});
	for child in op.children() {
		walk(child, registry, out);
	}
}

/// What the write `write` touches in itself: the tables it names, and what its statement clears.
fn write_footprint(write: &LogicalWrite, out: &mut Footprint) {
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

/// [`walk`] of the `Write` node of `write`: its own footprint, that of the scalars it holds, and
/// that of the plan that feeds it rows.
fn walk_write(write: &LogicalWrite, registry: &FunctionRegistry, out: &mut Footprint) {
	write_footprint(write, out);
	write.for_each_scalar(&mut |scalar| expr_footprint(scalar, registry, out));
	if let LogicalWriteSource::Rows(input) = &write.source {
		walk(input, registry, out);
	}
}

/// The tables a write writes; `None` where a target is evaluated.
pub(crate) fn write_targets(source: &LogicalWriteSource) -> Option<Vec<TableName>> {
	match source {
		LogicalWriteSource::Targets(targets) => {
			let mut out = Vec::new();
			for target in targets {
				if let LogicalExpr::Table(table) = target {
					out.push(table.clone());
				} else if let LogicalExpr::RecordId(rid) = target {
					out.push(rid.table.clone());
				} else {
					return None;
				}
			}
			Some(out)
		}
		LogicalWriteSource::Inserts(inserts) => {
			if let Some(LogicalExpr::Table(table)) = &inserts.into {
				Some(vec![table.clone()])
			} else {
				None
			}
		}
		LogicalWriteSource::Rows(rows) => {
			let mut tables = Vec::new();
			let mut named = true;
			let _ = rows.visit(&mut |op| {
				if let LogicalOp::Source(source) = op {
					if let SourceRef::Table(table) = &source.source {
						tables.push(table.clone());
					} else if let SourceRef::RecordId(rid) = &source.source {
						tables.push(rid.table.clone());
					} else {
						named = false;
					}
				} else if let LogicalOp::Values(values) = op {
					// A list of records written out, or a parameter's list a pass
					// replaced with one: it names the tables of its elements, and
					// any value it does not name leaves the tables open.
					named &= named_tables(&values.expr, &mut tables);
				}
				std::ops::ControlFlow::<()>::Continue(())
			});
			named.then_some(tables)
		}
	}
}

/// Pushes the table `expr`, or each element of it where it is a literal list, names, as a table
/// or a record id; whether every one is named so.
fn named_tables(expr: &LogicalExpr, tables: &mut Vec<TableName>) -> bool {
	let mut push = |expr: &LogicalExpr| {
		if let LogicalExpr::Table(table) = expr {
			tables.push(table.clone());
			true
		} else if let LogicalExpr::RecordId(rid) = expr {
			tables.push(rid.table.clone());
			true
		} else {
			false
		}
	};
	if let LogicalExpr::Array(items) = expr {
		items.iter().all(push)
	} else {
		push(expr)
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
					// The body is stored in the catalog and is not visible here, so the
					// footprint widens to unknown.
					out.absorb(Footprint::unknown());
				}
				Function::Script(_)
				| Function::Model(_)
				| Function::Module(..)
				| Function::Silo {
					..
				} => out.absorb(Footprint::unknown()),
			},
			// A value that is invoked, or a lookup over edges, runs or reads
			// things this footprint cannot name, so it widens to unknown.
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
			// A path into a value, a lookup or a method may follow a record link
			// whose table is not known here, so it reads records of any table.
			LogicalExpr::Mock(_)
			| LogicalExpr::Doc
			| LogicalExpr::Closure(_)
			| LogicalExpr::Access {
				..
			}
			| LogicalExpr::Index {
				..
			}
			| LogicalExpr::First(_)
			| LogicalExpr::Last(_)
			| LogicalExpr::Each(_)
			| LogicalExpr::Flatten {
				..
			}
			| LogicalExpr::Filter {
				..
			}
			| LogicalExpr::Method {
				..
			}
			| LogicalExpr::Destructure {
				..
			}
			| LogicalExpr::Optional {
				..
			}
			| LogicalExpr::Lookup {
				..
			}
			| LogicalExpr::Recurse(_)
			| LogicalExpr::RepeatRecurse(_)
			| LogicalExpr::Broadcast {
				..
			}
			| LogicalExpr::SetField {
				..
			} => out.read(Resource::any_data()),
		}
		std::ops::ControlFlow::<()>::Continue(())
	});
}

/// The name an identifier or string literal gives by being written.
pub(crate) fn written(expr: &Expr) -> Option<String> {
	if let Expr::Literal(crate::expr::Literal::String(s)) = expr {
		Some(s.as_str().to_owned())
	} else {
		crate::ddl::scalar::written_name(expr)
	}
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
				// A view is filled from its sources into its table.
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
			DefineStatement::Analyzer(_)
			| DefineStatement::Model(_)
			| DefineStatement::Config(_)
			| DefineStatement::Api(_)
			| DefineStatement::Bucket(_)
			| DefineStatement::Type(_)
			| DefineStatement::Sequence(_)
			| DefineStatement::Module(_) => {
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
			RemoveStatement::Analyzer(_)
			| RemoveStatement::Access(_)
			| RemoveStatement::User(_)
			| RemoveStatement::Model(_)
			| RemoveStatement::Api(_)
			| RemoveStatement::Bucket(_)
			| RemoveStatement::Type(_)
			| RemoveStatement::Sequence(_)
			| RemoveStatement::Module(_)
			| RemoveStatement::Config(_) => {
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
			AlterStatement::Api(_)
			| AlterStatement::Sequence(_)
			| AlterStatement::Bucket(_)
			| AlterStatement::Type(_)
			| AlterStatement::Config(_)
			| AlterStatement::Analyzer(_)
			| AlterStatement::User(_)
			| AlterStatement::Access(_)
			| AlterStatement::Module(_) => {
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

	/// The footprint of a write taken by reference is that of the node that holds it, whatever the
	/// source and clauses.
	#[test]
	fn a_borrowed_write_has_the_footprint_of_its_node() {
		let registry = FunctionRegistry::with_builtins();
		for sql in [
			"CREATE a SET x = (SELECT * FROM b)",
			"UPDATE a:1 SET y = 2 WHERE (SELECT * FROM c) = 1",
			"DELETE a WHERE x = 1",
			"INSERT INTO a [{ x: (SELECT * FROM d) }]",
			"INSERT INTO a { id: 1 } ON DUPLICATE KEY UPDATE x = (SELECT * FROM e)",
			"UPSERT a CONTENT { x: 1 }",
			"RELATE a:1->r->b:2 SET x = 1",
			"CREATE (SELECT VALUE id FROM f) SET x = 1",
		] {
			let op = super::super::lower_test(sql);
			let LogicalOp::Write(write) = &op else {
				panic!("{sql} lowers to a write");
			};
			assert_eq!(of_write(write, &registry), of(&op, &registry), "{sql}");
		}
	}
}
