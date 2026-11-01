//! The indexes and constraints of the user's tables, as the `pg_index`, `pg_constraint` and
//! `pg_class` views list them.
//!
//! Every table has a primary key. It is the unique index the SQL DDL names `<table>_<columns>_pk`
//! when there is one, and the record key `id` otherwise. A unique index named
//! `<table>_<columns>_uniq` is a `UNIQUE` constraint; any other index is a plain index. Every
//! foreign key references the `id` of its target table.
//!
//! Object identifiers are derived from the position of the table in the list the views are given
//! and from the order of its indexes and foreign keys, so every view that lists the same tables
//! agrees on them.

use std::borrow::Cow;
use std::collections::HashSet;

use super::{ReferentialAction, TableSchema};
use crate::lower::ddl::sequence_name;

/// The oid of the `i`-th user table, as `pg_class` lists it.
pub(crate) const fn oid_of_table(i: usize) -> i64 {
	16384 + i as i64
}

const INDEX_OID_BASE: i64 = 1_000_000;
const CONSTRAINT_OID_BASE: i64 = 2_000_000;

/// The 1-based `attnum` of `column` in `table`.
fn attnum(table: &TableSchema, column: &str) -> Option<i64> {
	table.columns.iter().position(|c| c.name == column).map(|i| i as i64 + 1)
}

/// An index relation.
pub(crate) struct IndexEntry {
	pub oid: i64,
	pub table_oid: i64,
	pub name: String,
	pub attnums: Vec<i64>,
	pub unique: bool,
	pub primary: bool,
}

/// A constraint of a table.
pub(crate) struct ConstraintEntry {
	pub oid: i64,
	pub name: String,
	/// `p`, `u` or `f`.
	pub kind: char,
	pub table_oid: i64,
	/// The index that enforces it; for a foreign key, the primary key index it references.
	pub index_oid: i64,
	pub attnums: Vec<i64>,
	pub referenced_table_oid: i64,
	pub referenced_attnums: Vec<i64>,
	pub on_delete: ReferentialAction,
	/// What `pg_get_constraintdef` answers.
	pub definition: String,
}

/// How a unique index of `table` is read.
#[derive(PartialEq, Eq)]
enum Role {
	PrimaryKey,
	Unique,
	Plain,
}

fn role(table: &TableSchema, index: &super::IndexInfo) -> Role {
	let prefix = table.name.replace('.', "_");
	let stem = index.columns.join("_");
	if !index.unique {
		Role::Plain
	} else if index.name == format!("{prefix}_{stem}_pk") {
		Role::PrimaryKey
	} else if index.name == format!("{prefix}_{stem}_uniq") {
		Role::Unique
	} else {
		Role::Plain
	}
}

/// The indexes of `tables`, the primary key index of each table first.
///
/// An index over a column the table does not list is not listed.
pub(crate) fn user_indexes(tables: &[TableSchema]) -> Vec<IndexEntry> {
	let mut entries = Vec::new();
	let mut next = INDEX_OID_BASE;
	for (position, table) in tables.iter().enumerate() {
		let table_oid = oid_of_table(position);
		let mut push = |name: String, attnums: Vec<i64>, unique: bool, primary: bool| {
			entries.push(IndexEntry {
				oid: next,
				table_oid,
				name,
				attnums,
				unique,
				primary,
			});
			next += 1;
		};
		let primary = table.indexes.iter().find(|index| role(table, index) == Role::PrimaryKey);
		match primary {
			Some(index) => {
				let attnums: Option<Vec<i64>> =
					index.columns.iter().map(|c| attnum(table, c)).collect();
				if let Some(attnums) = attnums {
					push(format!("{}_pkey", table.name), attnums, true, true);
				}
			}
			None => {
				if let Some(id) = attnum(table, "id") {
					push(format!("{}_pkey", table.name), vec![id], true, true);
				}
			}
		}
		for index in &table.indexes {
			if role(table, index) == Role::PrimaryKey {
				continue;
			}
			let attnums: Option<Vec<i64>> =
				index.columns.iter().map(|c| attnum(table, c)).collect();
			if let Some(attnums) = attnums {
				push(index.name.clone(), attnums, index.unique, false);
			}
		}
	}
	entries
}

/// What `pg_get_indexdef` answers for each index of `tables`, by the oid of the index:
/// `CREATE [UNIQUE] INDEX name ON public.table USING btree (columns)`.
pub(crate) fn index_definitions(tables: &[TableSchema]) -> Vec<(i64, String)> {
	user_indexes(tables)
		.into_iter()
		.filter_map(|index| {
			let table = tables.get(usize::try_from(index.table_oid - oid_of_table(0)).ok()?)?;
			Some((
				index.oid,
				format!(
					"CREATE {}INDEX {} ON public.{} USING btree ({})",
					if index.unique {
						"UNIQUE "
					} else {
						""
					},
					quote_ident(&index.name),
					quote_ident(&table.name),
					column_list(table, &index.attnums)
				),
			))
		})
		.collect()
}

/// The identifier as `pg_get_constraintdef` writes it: quoted unless it is a plain lowercase name.
pub(crate) fn quote_ident(name: &str) -> String {
	let plain = name.chars().next().is_some_and(|c| c.is_ascii_lowercase() || c == '_')
		&& name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
	if plain {
		name.to_owned()
	} else {
		format!("\"{}\"", name.replace('"', "\"\""))
	}
}

fn column_list(table: &TableSchema, attnums: &[i64]) -> String {
	attnums
		.iter()
		.filter_map(|n| table.columns.get(usize::try_from(*n - 1).ok()?))
		.map(|c| quote_ident(&c.name))
		.collect::<Vec<_>>()
		.join(", ")
}

/// The constraints of `tables`: each table's primary key, its `UNIQUE` constraints, and its
/// foreign keys, in that order. A foreign key whose target table is not listed is not listed.
pub(crate) fn user_constraints(tables: &[TableSchema]) -> Vec<ConstraintEntry> {
	let indexes = user_indexes(tables);
	let mut entries = Vec::new();
	let mut next = CONSTRAINT_OID_BASE;
	for (position, table) in tables.iter().enumerate() {
		let table_oid = oid_of_table(position);
		for index in indexes.iter().filter(|i| i.table_oid == table_oid && i.unique) {
			if !index.primary
				&& !table
					.indexes
					.iter()
					.any(|i| i.name == index.name && role(table, i) == Role::Unique)
			{
				continue;
			}
			let (kind, label) = if index.primary {
				('p', "PRIMARY KEY")
			} else {
				('u', "UNIQUE")
			};
			entries.push(ConstraintEntry {
				oid: next,
				name: index.name.clone(),
				kind,
				table_oid,
				index_oid: index.oid,
				attnums: index.attnums.clone(),
				referenced_table_oid: 0,
				referenced_attnums: Vec::new(),
				on_delete: ReferentialAction::NoAction,
				definition: format!("{label} ({})", column_list(table, &index.attnums)),
			});
			next += 1;
		}
		for key in &table.foreign_keys {
			let Some(column) = attnum(table, &key.column) else {
				continue;
			};
			let Some((target_position, target)) =
				tables.iter().enumerate().find(|(_, t)| t.name == key.target_table)
			else {
				continue;
			};
			let Some(target_id) = attnum(target, "id") else {
				continue;
			};
			let target_oid = oid_of_table(target_position);
			let target_index = indexes
				.iter()
				.find(|i| i.table_oid == target_oid && i.primary)
				.map_or(0, |i| i.oid);
			let mut definition = format!(
				"FOREIGN KEY ({}) REFERENCES {}(id)",
				quote_ident(&key.column),
				quote_ident(&target.name)
			);
			if key.on_delete != ReferentialAction::NoAction {
				definition.push_str(" ON DELETE ");
				definition.push_str(key.on_delete.sql());
			}
			entries.push(ConstraintEntry {
				oid: next,
				name: format!("{}_{}_fkey", table.name, key.column),
				kind: 'f',
				table_oid,
				index_oid: target_index,
				attnums: vec![column],
				referenced_table_oid: target_oid,
				referenced_attnums: vec![target_id],
				on_delete: key.on_delete,
				definition,
			});
			next += 1;
		}
	}
	entries
}

/// What a constraint name of a table stands for, as [`user_constraints`] names the constraints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintTarget {
	/// A `UNIQUE` constraint, or the primary key when a `<table>_<columns>_pk` index backs it: the
	/// index to remove.
	Index {
		name: String,
		primary: bool,
	},
	/// The primary key of a table with no index behind it: the record id.
	RecordId,
	/// A foreign key: the column that carries the `REFERENCE`.
	ForeignKey {
		column: String,
	},
}

/// The constraint of `table` named `name`, or `None` when the table has no such constraint.
///
/// The names are those of the `pg_constraint` view: `<table>_pkey` for the primary key,
/// `<table>_<column>_fkey` for a foreign key, and the name of the index of a `UNIQUE` constraint.
/// A plain index, and a unique index that no `UNIQUE` constraint of the SQL DDL defines, is not a
/// constraint.
pub fn constraint_target(table: &TableSchema, name: &str) -> Option<ConstraintTarget> {
	if name == format!("{}_pkey", table.name) {
		return Some(match table.indexes.iter().find(|i| role(table, i) == Role::PrimaryKey) {
			Some(index) => ConstraintTarget::Index {
				name: index.name.clone(),
				primary: true,
			},
			None => ConstraintTarget::RecordId,
		});
	}
	if let Some(index) =
		table.indexes.iter().find(|i| i.name == name && role(table, i) == Role::Unique)
	{
		return Some(ConstraintTarget::Index {
			name: index.name.clone(),
			primary: false,
		});
	}
	table.foreign_keys.iter().find(|key| format!("{}_{}_fkey", table.name, key.column) == name).map(
		|key| ConstraintTarget::ForeignKey {
			column: key.column.clone(),
		},
	)
}

/// A serial or auto-increment column and the sequence that numbers it.
pub struct SerialColumn {
	/// The oid of the table, as `pg_class` lists it.
	pub table_oid: i64,
	pub table: String,
	pub schema: String,
	pub column: String,
	/// The 1-based `attnum` of the column.
	pub attnum: i64,
	/// The sequence name.
	pub sequence: String,
}

/// `tables`, with each sequence the SQL DDL defined for a generated column listed under the name
/// PostgreSQL gives it, `<table>_<column>_seq`. A name another sequence already has takes the
/// smallest number that makes it unique (`a_b_c_seq`, `a_b_c_seq1`), as PostgreSQL does; the
/// columns are taken in the order of their table's name and then their position, so the names a
/// request sees depend on the tables alone. A sequence the DDL did not define for the column, such
/// as one named in a user's `DEFAULT`, keeps its own name, and no generated name takes it.
///
/// The tables are borrowed when no column has such a sequence. Applying this to its own result
/// changes nothing.
pub(crate) fn with_visible_sequence_names(tables: &[TableSchema]) -> Cow<'_, [TableSchema]> {
	let defined = |table: &TableSchema, column: &super::ColumnInfo| {
		column.sequence.as_deref() == Some(sequence_name(&table.name, &column.name).as_str())
	};
	let mut generated: Vec<(usize, usize)> = Vec::new();
	let mut taken: HashSet<&str> = HashSet::new();
	for (t, table) in tables.iter().enumerate() {
		for (c, column) in table.columns.iter().enumerate() {
			if defined(table, column) {
				generated.push((t, c));
			} else if let Some(sequence) = column.sequence.as_deref() {
				taken.insert(sequence);
			}
		}
	}
	if generated.is_empty() {
		return Cow::Borrowed(tables);
	}
	generated
		.sort_by(|(t1, c1), (t2, c2)| tables[*t1].name.cmp(&tables[*t2].name).then(c1.cmp(c2)));
	let mut named = tables.to_vec();
	let mut used: HashSet<String> = taken.into_iter().map(str::to_owned).collect();
	for (t, c) in generated {
		let base =
			format!("{}_{}_seq", tables[t].name.replace('.', "_"), tables[t].columns[c].name);
		let mut name = base.clone();
		let mut number = 0;
		while !used.insert(name.clone()) {
			number += 1;
			name = format!("{base}{number}");
		}
		named[t].columns[c].sequence = Some(name);
	}
	Cow::Owned(named)
}

/// The columns of `tables` that a sequence numbers, under the sequence names PostgreSQL clients
/// see ([`with_visible_sequence_names`]).
pub fn serial_columns(tables: &[TableSchema]) -> Vec<SerialColumn> {
	visible_serial_columns(&with_visible_sequence_names(tables))
}

/// The columns of `tables` that a sequence numbers, under the names the tables list.
pub(crate) fn visible_serial_columns(tables: &[TableSchema]) -> Vec<SerialColumn> {
	let mut columns = Vec::new();
	for (position, table) in tables.iter().enumerate() {
		for (index, column) in table.columns.iter().enumerate() {
			let Some(sequence) = &column.sequence else {
				continue;
			};
			columns.push(SerialColumn {
				table_oid: oid_of_table(position),
				table: table.name.clone(),
				schema: table.schema.clone().unwrap_or_else(|| "public".to_owned()),
				column: column.name.clone(),
				attnum: index as i64 + 1,
				sequence: sequence.clone(),
			});
		}
	}
	columns
}

/// The oid of the default B-tree operator class of a column of SQL type `data_type`, and its name.
pub(crate) fn default_opclass(data_type: Option<&str>) -> (i64, &'static str) {
	match data_type {
		Some("bigint") => (3124, "int8_ops"),
		Some("integer") => (1978, "int4_ops"),
		Some("smallint") => (1979, "int2_ops"),
		Some("boolean") => (424, "bool_ops"),
		Some("double precision") => (3122, "float8_ops"),
		Some("real") => (3121, "float4_ops"),
		Some("numeric") => (1988, "numeric_ops"),
		_ => (3126, "text_ops"),
	}
}

/// The `(oid, definition)` of every constraint of `tables`, as `pg_get_constraintdef` answers.
pub fn constraint_definitions(tables: &[TableSchema]) -> Vec<(i64, String)> {
	user_constraints(tables).into_iter().map(|c| (c.oid, c.definition)).collect()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{ColumnInfo, ForeignKeyInfo, IndexInfo};

	fn table(name: &str, columns: &[&str]) -> TableSchema {
		TableSchema::new(
			name,
			columns.iter().enumerate().map(|(i, c)| ColumnInfo::new(*c, i)).collect(),
		)
	}

	fn index(name: &str, columns: &[&str], unique: bool) -> IndexInfo {
		IndexInfo {
			name: name.to_owned(),
			columns: columns.iter().map(|c| (*c).to_owned()).collect(),
			unique,
		}
	}

	#[test]
	fn every_table_has_a_primary_key_on_id_unless_the_ddl_names_another() {
		let tables = [
			table("a", &["id", "x"]),
			table("b", &["id", "x", "y"]).with_indexes(vec![index("b_x_y_pk", &["x", "y"], true)]),
		];
		let constraints = user_constraints(&tables);
		let summary: Vec<_> =
			constraints.iter().map(|c| (c.name.as_str(), c.kind, c.definition.as_str())).collect();
		assert_eq!(
			summary,
			vec![("a_pkey", 'p', "PRIMARY KEY (id)"), ("b_pkey", 'p', "PRIMARY KEY (x, y)"),]
		);
		assert_eq!(constraints[1].attnums, vec![2, 3]);
	}

	#[test]
	fn unique_constraints_and_plain_indexes_are_told_apart_by_name() {
		let tables = [table("t", &["id", "email", "name"]).with_indexes(vec![
			index("t_email_uniq", &["email"], true),
			index("t_name_idx", &["name"], false),
			index("by_name", &["name"], true),
			index("ghost", &["nope"], false),
		])];
		let constraints = user_constraints(&tables);
		let names: Vec<_> = constraints.iter().map(|c| c.name.as_str()).collect();
		assert_eq!(names, ["t_pkey", "t_email_uniq"]);
		let indexes = user_indexes(&tables);
		let listed: Vec<_> = indexes.iter().map(|i| (i.name.as_str(), i.unique)).collect();
		assert_eq!(
			listed,
			[("t_pkey", true), ("t_email_uniq", true), ("t_name_idx", false), ("by_name", true)]
		);
	}

	#[test]
	fn a_foreign_key_references_the_id_of_its_target() {
		let tables = [
			table("User", &["id", "name"]),
			table("Post", &["id", "authorId"]).with_foreign_keys(vec![
				ForeignKeyInfo {
					column: "authorId".to_owned(),
					target_table: "User".to_owned(),
					on_delete: ReferentialAction::Cascade,
				},
				ForeignKeyInfo {
					column: "authorId".to_owned(),
					target_table: "Missing".to_owned(),
					on_delete: ReferentialAction::NoAction,
				},
			]),
		];
		let constraints = user_constraints(&tables);
		let key = constraints.iter().find(|c| c.kind == 'f').unwrap();
		assert_eq!(key.name, "Post_authorId_fkey");
		assert_eq!(key.attnums, vec![2]);
		assert_eq!(key.referenced_table_oid, oid_of_table(0));
		assert_eq!(key.referenced_attnums, vec![1]);
		assert_eq!(
			key.definition,
			"FOREIGN KEY (\"authorId\") REFERENCES \"User\"(id) ON DELETE CASCADE"
		);
		assert_eq!(constraints.iter().filter(|c| c.kind == 'f').count(), 1);
	}
}
