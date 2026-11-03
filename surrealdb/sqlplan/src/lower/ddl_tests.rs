// A catch-all arm in these tests reports the value a lowering produced instead of the one
// expected; naming every other variant would add nothing to that report.
#![allow(clippy::wildcard_enum_match_arm)]

use surrealdb_expr::expr::reference::ReferenceDeleteStrategy;
use surrealdb_expr::expr::statements::alter::{AlterKind, AlterStatement};
use surrealdb_expr::expr::statements::define::{DefineDefault, DefineKind, DefineStatement};
use surrealdb_expr::expr::statements::remove::{RemoveStatement, SqlTypeObject};
use surrealdb_expr::expr::{Expr, Kind};
use surrealdb_types::ToSql;

use super::ddl::{
	lower_mysql_ddl_definitions, lower_pgsql_ddl_definitions, map_mysql_type, map_pgsql_type,
};

#[test]
fn test_pgsql_type_map() {
	let parse = |sql: &str| -> surrealdb_pgsql::ast::stmt::Statement {
		surrealdb_pgsql::parse_statement(sql).expect("parse pgsql")
	};

	let stmt = parse(
		"CREATE TABLE t (
		a INT,
		b SMALLINT,
		c BIGINT,
		d SERIAL,
		e REAL,
		f DOUBLE PRECISION,
		g NUMERIC,
		h TEXT,
		i VARCHAR(255),
		j CHAR(10),
		k BOOL,
		l DATE,
		m TIME,
		n TIMESTAMP,
		o TIMESTAMPTZ,
		p UUID,
		q JSON,
		r JSONB,
		s BYTEA,
		t INT[],
		u TEXT[]
	);",
	);

	if let surrealdb_pgsql::ast::stmt::Statement::CreateTable(ct) = stmt {
		assert_eq!(map_pgsql_type(&ct.columns[0].data_type), Kind::I32);
		assert_eq!(map_pgsql_type(&ct.columns[1].data_type), Kind::I16);
		assert_eq!(map_pgsql_type(&ct.columns[2].data_type), Kind::Int);
		assert_eq!(map_pgsql_type(&ct.columns[3].data_type), Kind::I32);
		assert_eq!(map_pgsql_type(&ct.columns[4].data_type), Kind::F32);
		assert_eq!(map_pgsql_type(&ct.columns[5].data_type), Kind::Float);
		// A `numeric` without a precision holds any number of digits.
		assert_eq!(map_pgsql_type(&ct.columns[6].data_type), Kind::BigDecimal);
		assert_eq!(map_pgsql_type(&ct.columns[7].data_type), Kind::String);
		assert_eq!(map_pgsql_type(&ct.columns[8].data_type), Kind::String);
		assert_eq!(map_pgsql_type(&ct.columns[9].data_type), Kind::String);
		assert_eq!(map_pgsql_type(&ct.columns[10].data_type), Kind::Bool);
		assert_eq!(map_pgsql_type(&ct.columns[11].data_type), Kind::Date);
		assert_eq!(map_pgsql_type(&ct.columns[12].data_type), Kind::Time);
		assert_eq!(map_pgsql_type(&ct.columns[13].data_type), Kind::Timestamp);
		assert_eq!(map_pgsql_type(&ct.columns[14].data_type), Kind::Datetime);
		assert_eq!(map_pgsql_type(&ct.columns[15].data_type), Kind::Uuid);
		assert_eq!(map_pgsql_type(&ct.columns[16].data_type), Kind::Any);
		assert_eq!(map_pgsql_type(&ct.columns[17].data_type), Kind::Any);
		assert_eq!(map_pgsql_type(&ct.columns[18].data_type), Kind::Bytes);
		assert_eq!(
			map_pgsql_type(&ct.columns[19].data_type),
			Kind::Array(Box::new(Kind::I32), None)
		);
		assert_eq!(
			map_pgsql_type(&ct.columns[20].data_type),
			Kind::Array(Box::new(Kind::String), None)
		);
	} else {
		panic!("Expected CreateTable");
	}
}

#[test]
fn test_mysql_type_map() {
	assert_eq!(map_mysql_type("int"), Kind::I32);
	assert_eq!(map_mysql_type("integer"), Kind::I32);
	assert_eq!(map_mysql_type("int(10) UNSIGNED"), Kind::U32);
	assert_eq!(map_mysql_type("tinyint(3) ZEROFILL"), Kind::U8);
	assert_eq!(map_mysql_type("int zerofill"), Kind::U32);
	assert_eq!(map_mysql_type("tinyint(1)"), Kind::I8);
	assert_eq!(map_mysql_type("tinyint UNSIGNED"), Kind::U8);
	assert_eq!(map_mysql_type("smallint"), Kind::I16);
	assert_eq!(map_mysql_type("smallint(5) UNSIGNED"), Kind::U16);
	assert_eq!(map_mysql_type("mediumint"), Kind::I32);
	assert_eq!(map_mysql_type("mediumint UNSIGNED"), Kind::U32);
	assert_eq!(map_mysql_type("bigint"), Kind::Int);
	assert_eq!(map_mysql_type("bigint unsigned"), Kind::U64);
	assert_eq!(map_mysql_type("float"), Kind::F32);
	assert_eq!(map_mysql_type("float(24)"), Kind::F32);
	assert_eq!(map_mysql_type("float(25)"), Kind::Float);
	assert_eq!(map_mysql_type("FLOAT(53) UNSIGNED"), Kind::Float);
	assert_eq!(map_mysql_type("float(7,4)"), Kind::F32);
	assert_eq!(map_mysql_type("double"), Kind::Float);
	assert_eq!(map_mysql_type("double precision"), Kind::Float);
	assert_eq!(map_mysql_type("decimal(10,2)"), Kind::Decimal);
	assert_eq!(map_mysql_type("decimal"), Kind::Decimal);
	assert_eq!(map_mysql_type("DECIMAL(28,10)"), Kind::Decimal);
	assert_eq!(map_mysql_type("DECIMAL(29,10)"), Kind::BigDecimal);
	assert_eq!(map_mysql_type("decimal(65,30)"), Kind::BigDecimal);
	assert_eq!(map_mysql_type("numeric(40)"), Kind::BigDecimal);
	assert_eq!(map_mysql_type("fixed(65,0) unsigned"), Kind::BigDecimal);
	assert_eq!(map_mysql_type("varchar(100)"), Kind::String);
	assert_eq!(map_mysql_type("char(2)"), Kind::String);
	assert_eq!(map_mysql_type("text"), Kind::String);
	assert_eq!(map_mysql_type("longtext"), Kind::String);
	assert_eq!(map_mysql_type("bool"), Kind::Bool);
	assert_eq!(map_mysql_type("boolean"), Kind::Bool);
	assert_eq!(map_mysql_type("date"), Kind::Date);
	assert_eq!(map_mysql_type("time"), Kind::Interval);
	assert_eq!(map_mysql_type("time(3)"), Kind::Interval);
	assert_eq!(map_mysql_type("datetime"), Kind::Timestamp);
	assert_eq!(map_mysql_type("datetime(6)"), Kind::Timestamp);
	assert_eq!(map_mysql_type("timestamp"), Kind::Datetime);
	assert_eq!(map_mysql_type("year"), Kind::Either(vec![Kind::U16, Kind::String]));
	assert_eq!(map_mysql_type("json"), Kind::Any);
	assert_eq!(map_mysql_type("blob"), Kind::Bytes);
	assert_eq!(map_mysql_type("bit(8)"), Kind::U64);
	assert_eq!(map_mysql_type("bit"), Kind::U64);
	assert_eq!(map_mysql_type("bit(64)"), Kind::U64);
	assert_eq!(map_mysql_type("enum"), Kind::String);
	assert_eq!(map_mysql_type("set()"), Kind::String);
}

#[test]
fn test_mysql_type_map_unsigned_bigint_serial_and_aliases() {
	assert_eq!(map_mysql_type("bigint UNSIGNED"), Kind::U64);
	assert_eq!(map_mysql_type("BIGINT(20) unsigned"), Kind::U64);
	assert_eq!(map_mysql_type("int8 unsigned"), Kind::U64);
	assert_eq!(map_mysql_type("bigint"), Kind::Int);
	assert_eq!(map_mysql_type("int8"), Kind::Int);
	assert_eq!(map_mysql_type("serial"), Kind::U64);
	assert_eq!(map_mysql_type("int1"), Kind::I8);
	assert_eq!(map_mysql_type("int1 unsigned"), Kind::U8);
	assert_eq!(map_mysql_type("int2"), Kind::I16);
	assert_eq!(map_mysql_type("int3"), Kind::I32);
	assert_eq!(map_mysql_type("int3 unsigned"), Kind::U32);
	assert_eq!(map_mysql_type("int4"), Kind::I32);
	assert_eq!(map_mysql_type("middleint"), Kind::I32);
	assert_eq!(map_mysql_type("middleint unsigned"), Kind::U32);
	assert_eq!(map_mysql_type("float4"), Kind::F32);
	assert_eq!(map_mysql_type("float8"), Kind::Float);
	assert_eq!(map_mysql_type("double precision"), Kind::Float);
}

#[test]
fn test_mysql_type_map_string_and_binary_aliases() {
	assert_eq!(map_mysql_type("nchar(5)"), Kind::String);
	assert_eq!(map_mysql_type("NVARCHAR(5)"), Kind::String);
	assert_eq!(map_mysql_type("national char(5)"), Kind::String);
	assert_eq!(map_mysql_type("NATIONAL VARCHAR(5)"), Kind::String);
	assert_eq!(map_mysql_type("national character varying(5)"), Kind::String);
	assert_eq!(map_mysql_type("character varying(5)"), Kind::String);
	assert_eq!(map_mysql_type("long"), Kind::String);
	assert_eq!(map_mysql_type("long varchar"), Kind::String);
	assert_eq!(map_mysql_type("long varbinary"), Kind::Bytes);
}

#[test]
fn test_mysql_type_map_vector() {
	assert_eq!(map_mysql_type("vector(768)"), Kind::Array(Box::new(Kind::Float), Some(768)));
	assert_eq!(map_mysql_type("VECTOR(3)"), Kind::Array(Box::new(Kind::Float), Some(3)));
	assert_eq!(map_mysql_type("vector(0)"), Kind::Array(Box::new(Kind::Float), None));
}

#[test]
fn test_pgsql_text_shaped_types_map_to_string() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE TABLE t (
		a INET,
		b CIDR,
		c MACADDR,
		d MACADDR8,
		e BIT(8),
		f BIT VARYING(4),
		g VARBIT,
		h XML,
		i TSVECTOR,
		j TSQUERY,
		k JSONPATH,
		l TID,
		m PG_SNAPSHOT,
		n TXID_SNAPSHOT
	);",
	)
	.expect("parse pgsql");
	let surrealdb_pgsql::ast::stmt::Statement::CreateTable(ct) = stmt else {
		panic!("Expected CreateTable");
	};
	for column in &ct.columns {
		assert_eq!(map_pgsql_type(&column.data_type), Kind::String, "{}", column.name);
	}
}

#[test]
fn test_pgsql_text_type_names_map_to_string() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE TABLE t (a name, b \"char\", c citext, d VARCHAR(10));",
	)
	.expect("parse pgsql");
	let surrealdb_pgsql::ast::stmt::Statement::CreateTable(ct) = stmt else {
		panic!("Expected CreateTable");
	};
	for column in &ct.columns {
		assert_eq!(map_pgsql_type(&column.data_type), Kind::String);
	}
}

#[test]
fn test_lower_mysql_serial_column_is_unsigned_unique_and_sequenced() {
	let stmt = surrealdb_mysql::parse_statement("CREATE TABLE orders (id SERIAL, note TEXT);")
		.expect("parse");

	let exprs = lower_mysql_ddl_definitions(&stmt).expect("lower");
	// 1 DefineTable + 1 DefineSequence (id) + 2 DefineField + 1 DefineIndex (id UNIQUE)
	assert_eq!(exprs.len(), 5);

	let id_field = exprs.iter().find_map(|expr| match expr {
		Expr::Define(d) => match &**d {
			DefineStatement::Field(f) if f.name.to_sql() == "id" => Some(f),
			_ => None,
		},
		_ => None,
	});
	let id_field = id_field.expect("a field definition for id");
	// The record id key is a signed 64-bit integer; the declared SQL type keeps the unsigned type.
	assert_eq!(id_field.field_kind, Some(Kind::Int));
	let sql_type = id_field.sql_type.as_ref().expect("a declared SQL type");
	assert!(sql_type.text.to_ascii_lowercase().contains("serial"), "{}", sql_type.text);
	assert!(id_field.assert.is_some(), "SERIAL is NOT NULL");
	assert!(
		!matches!(id_field.default, DefineDefault::None),
		"SERIAL is AUTO_INCREMENT, so the column has a sequence default"
	);
}

#[test]
fn test_lower_pgsql_create_table_simple() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email VARCHAR(100) UNIQUE, age INT DEFAULT 18);"
	).expect("parse");

	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	// 1 DefineTable + 4 DefineField + 2 DefineIndex (id PK + email UNIQUE) = 7
	assert_eq!(exprs.len(), 7);

	let Expr::Define(d) = &exprs[0] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Table(t) = &**d else {
		panic!("Expected DefineTable");
	};
	assert_eq!(t.full, true);
	assert_eq!(t.kind, DefineKind::Default);

	// Field id
	let Expr::Define(d) = &exprs[1] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Field(f) = &**d else {
		panic!("Expected DefineField id");
	};
	assert_eq!(f.field_kind, Some(Kind::I32));

	// Field name (NOT NULL)
	let Expr::Define(d) = &exprs[2] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Field(f) = &**d else {
		panic!("Expected DefineField name");
	};
	assert_eq!(f.field_kind, Some(Kind::String));
	assert!(f.assert.is_some());

	// Field email
	let Expr::Define(d) = &exprs[3] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Field(f) = &**d else {
		panic!("Expected DefineField email");
	};
	assert_eq!(f.field_kind, Some(Kind::either(vec![Kind::None, Kind::Null, Kind::String])));

	// Field age with default 18
	let Expr::Define(d) = &exprs[4] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Field(f) = &**d else {
		panic!("Expected DefineField age");
	};
	assert_eq!(f.field_kind, Some(Kind::either(vec![Kind::None, Kind::Null, Kind::I32])));
	assert!(matches!(f.default, surrealdb_expr::expr::statements::define::DefineDefault::Set(_)));

	// Index 1: id primary key index
	let Expr::Define(d) = &exprs[5] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Index(idx) = &**d else {
		panic!("Expected DefineIndex for primary key");
	};
	assert_eq!(
		idx.index,
		surrealdb_expr::expr::index_kind::Index::Uniq {
			nulls_not_distinct: false
		}
	);

	// Index 2: email unique index
	let Expr::Define(d) = &exprs[6] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Index(idx) = &**d else {
		panic!("Expected DefineIndex for unique");
	};
	assert_eq!(
		idx.index,
		surrealdb_expr::expr::index_kind::Index::Uniq {
			nulls_not_distinct: false
		}
	);
}

#[test]
fn test_lower_pgsql_create_table_composite_keys() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE TABLE IF NOT EXISTS order_items (
			order_id INT,
			item_id INT,
			quantity INT NOT NULL,
			PRIMARY KEY (order_id, item_id),
			UNIQUE (order_id, quantity)
		);",
	)
	.expect("parse");

	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	// 1 DefineTable + 3 DefineField + 2 TableConstraint indexes = 6
	assert_eq!(exprs.len(), 6);

	// DefineTable with IF NOT EXISTS
	let Expr::Define(d) = &exprs[0] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Table(t) = &**d else {
		panic!("Expected DefineTable");
	};
	assert_eq!(t.kind, DefineKind::IfNotExists);
}

#[test]
fn test_lower_pgsql_create_index() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users (email);",
	)
	.expect("parse");

	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	assert_eq!(exprs.len(), 1);

	let Expr::Define(d) = &exprs[0] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Index(idx) = &**d else {
		panic!("Expected DefineIndex");
	};
	assert_eq!(
		idx.index,
		surrealdb_expr::expr::index_kind::Index::Uniq {
			nulls_not_distinct: false
		}
	);
	assert_eq!(idx.kind, DefineKind::IfNotExists);
	assert_eq!(idx.cols.len(), 1);

	let multi_idx = surrealdb_pgsql::parse_statement(
		"CREATE INDEX idx_orders_customer_status ON orders (customer_id, status);",
	)
	.expect("parse");

	let exprs_multi = lower_pgsql_ddl_definitions(&multi_idx).expect("lower");
	assert_eq!(exprs_multi.len(), 1);
	let Expr::Define(d) = &exprs_multi[0] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Index(idx) = &**d else {
		panic!("Expected DefineIndex");
	};
	assert_eq!(idx.index, surrealdb_expr::expr::index_kind::Index::Idx);
	assert_eq!(idx.cols.len(), 2);
}

#[test]
fn test_lower_pgsql_create_view() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE OR REPLACE VIEW active_users AS SELECT id, name FROM users WHERE active = true;",
	)
	.expect("parse");

	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	assert_eq!(exprs.len(), 1);

	let Expr::Define(d) = &exprs[0] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Table(t) = &**d else {
		panic!("Expected DefineTable for view");
	};
	assert_eq!(t.kind, DefineKind::Overwrite);
	assert!(t.view.is_some());
	let v = t.view.as_ref().unwrap();
	assert_eq!(v.what.len(), 1);
	assert!(v.cond.is_some());
}

#[test]
fn test_lower_pgsql_alter_table() {
	let stmt = surrealdb_pgsql::parse_statement("ALTER TABLE users ADD COLUMN phone VARCHAR(20);")
		.expect("parse");
	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	assert_eq!(exprs.len(), 1);
	assert!(matches!(&exprs[0], Expr::Define(d) if matches!(&**d, DefineStatement::Field(_))));

	let drop_col =
		surrealdb_pgsql::parse_statement("ALTER TABLE users DROP COLUMN IF EXISTS phone;")
			.expect("parse");
	let exprs_drop = lower_pgsql_ddl_definitions(&drop_col).expect("lower");
	assert_eq!(exprs_drop.len(), 1);
	assert!(matches!(&exprs_drop[0], Expr::Remove(r) if matches!(&**r, RemoveStatement::Field(_))));

	let alter_type =
		surrealdb_pgsql::parse_statement("ALTER TABLE users ALTER COLUMN age TYPE BIGINT;")
			.expect("parse");
	let exprs_type = lower_pgsql_ddl_definitions(&alter_type).expect("lower");
	assert_eq!(exprs_type.len(), 1);

	let set_def =
		surrealdb_pgsql::parse_statement("ALTER TABLE users ALTER COLUMN age SET DEFAULT 21;")
			.expect("parse");
	let exprs_def = lower_pgsql_ddl_definitions(&set_def).expect("lower");
	assert_eq!(exprs_def.len(), 1);

	let drop_def =
		surrealdb_pgsql::parse_statement("ALTER TABLE users ALTER COLUMN age DROP DEFAULT;")
			.expect("parse");
	let exprs_drop_def = lower_pgsql_ddl_definitions(&drop_def).expect("lower");
	assert_eq!(exprs_drop_def.len(), 1);

	let set_nn =
		surrealdb_pgsql::parse_statement("ALTER TABLE users ALTER COLUMN age SET NOT NULL;")
			.expect("parse");
	let exprs_nn = lower_pgsql_ddl_definitions(&set_nn).expect("lower");
	assert_eq!(exprs_nn.len(), 1);

	let drop_nn =
		surrealdb_pgsql::parse_statement("ALTER TABLE users ALTER COLUMN age DROP NOT NULL;")
			.expect("parse");
	let exprs_drop_nn = lower_pgsql_ddl_definitions(&drop_nn).expect("lower");
	assert_eq!(exprs_drop_nn.len(), 1);
}

#[test]
fn test_lower_pgsql_drop() {
	let stmt =
		surrealdb_pgsql::parse_statement("DROP TABLE IF EXISTS users, profiles;").expect("parse");
	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	assert_eq!(exprs.len(), 2);
	for e in &exprs {
		assert!(
			matches!(e, Expr::Remove(r) if matches!(&**r, RemoveStatement::Table(t) if t.if_exists))
		);
	}

	let drop_idx =
		surrealdb_pgsql::parse_statement("DROP INDEX IF EXISTS idx_users_email;").expect("parse");
	let exprs_idx = lower_pgsql_ddl_definitions(&drop_idx).expect("lower");
	assert_eq!(exprs_idx.len(), 1);
	assert!(
		matches!(&exprs_idx[0], Expr::Remove(r) if matches!(&**r, RemoveStatement::Index(i) if i.if_exists))
	);

	let drop_view = surrealdb_pgsql::parse_statement("DROP VIEW active_users;").expect("parse");
	let exprs_view = lower_pgsql_ddl_definitions(&drop_view).expect("lower");
	assert_eq!(exprs_view.len(), 1);
	assert!(matches!(&exprs_view[0], Expr::Remove(r) if matches!(&**r, RemoveStatement::Table(_))));
}

#[test]
fn test_lower_mysql_create_table() {
	let stmt = surrealdb_mysql::parse_statement(
		"CREATE TABLE IF NOT EXISTS products (
			id INT AUTO_INCREMENT PRIMARY KEY,
			sku VARCHAR(50) NOT NULL UNIQUE,
			name VARCHAR(255) NOT NULL,
			price DECIMAL(10,2) NOT NULL DEFAULT 0.0,
			description TEXT,
			is_active BOOL DEFAULT true,
			created_at DATETIME
		);",
	)
	.expect("parse");

	let exprs = lower_mysql_ddl_definitions(&stmt).expect("lower");
	// 1 sequence restart (id) + 1 DefineTable + 1 DefineSequence (id) + 7 DefineField
	// + 2 DefineIndex (id PK + sku UNIQUE)
	assert_eq!(exprs.len(), 12);
	assert!(
		matches!(&exprs[0], Expr::IfElse(_)),
		"the restart of the numbering comes before the table's definition"
	);

	let Expr::Define(d) = &exprs[1] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Table(t) = &**d else {
		panic!("Expected DefineTable");
	};
	assert_eq!(t.full, true);
	assert_eq!(t.kind, DefineKind::IfNotExists);
}

#[test]
fn test_lower_mysql_create_table_with_constraints() {
	let stmt = surrealdb_mysql::parse_statement(
		"CREATE TABLE order_details (
			order_id INT,
			item_id INT,
			qty INT NOT NULL,
			PRIMARY KEY (order_id, item_id),
			UNIQUE KEY uq_order_qty (order_id, qty)
		);",
	)
	.expect("parse");

	let exprs = lower_mysql_ddl_definitions(&stmt).expect("lower");
	// 1 DefineTable + 3 DefineField + 2 TableConstraint indexes = 6
	assert_eq!(exprs.len(), 6);
}

#[test]
fn test_lower_mysql_drop_table() {
	let stmt =
		surrealdb_mysql::parse_statement("DROP TABLE IF EXISTS orders, items;").expect("parse");

	let exprs = lower_mysql_ddl_definitions(&stmt).expect("lower");
	assert_eq!(exprs.len(), 2);
	for e in &exprs {
		assert!(
			matches!(e, Expr::Remove(r) if matches!(&**r, RemoveStatement::Table(t) if t.if_exists))
		);
	}
}

#[test]
fn test_lower_pgsql_foreign_key_column_constraint() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE TABLE posts (
			id SERIAL PRIMARY KEY,
			author_id INT REFERENCES users(id),
			content TEXT
		);",
	)
	.expect("parse");

	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	// 1 DefineTable + 1 DefineSequence (id) + 3 DefineField + 1 PK Index = 6
	assert_eq!(exprs.len(), 6);

	// Find the author_id field
	let author_field = exprs
		.iter()
		.find(|e| {
			if let Expr::Define(d) = e
				&& let DefineStatement::Field(f) = &**d
			{
				f.name.to_sql() == "author_id"
			} else {
				false
			}
		})
		.expect("found author_id field");

	if let Expr::Define(d) = author_field
		&& let DefineStatement::Field(f) = &**d
	{
		assert!(f.reference.is_some(), "author_id should have a reference constraint");
		// The column is a link to the referenced table, nullable as the column is.
		assert_eq!(
			f.field_kind.as_ref().map(ToSql::to_sql).as_deref(),
			Some("none | record<users>")
		);
		let assert = f.assert.as_ref().expect("the link is asserted to name a row").to_sql();
		assert!(assert.contains("record::exists($value)"), "{assert}");
		assert!(assert.contains("= NONE"), "{assert}");
	}
}

#[test]
fn test_lower_pgsql_not_null_foreign_key_is_a_required_link() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE TABLE posts (id INT PRIMARY KEY, author_id INT NOT NULL REFERENCES users)",
	)
	.expect("parse");
	let sql: Vec<String> =
		lower_pgsql_ddl_definitions(&stmt).expect("lower").iter().map(ToSql::to_sql).collect();
	let field = sql.iter().find(|s| s.contains("FIELD author_id")).expect("the field");
	assert!(field.contains("TYPE record<users> "), "{field}");
	assert!(field.contains("REFERENCE ON DELETE REJECT"), "{field}");
	assert!(!field.contains("$value != NONE"), "{field}");
	assert!(field.contains(r#"violates foreign key constraint "posts_author_id_fkey""#), "{field}");
}

#[test]
fn test_lower_pgsql_foreign_key_to_its_own_table_accepts_the_row_itself() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE TABLE node (id INT PRIMARY KEY, parent INT REFERENCES node(id))",
	)
	.expect("parse");
	let sql: Vec<String> =
		lower_pgsql_ddl_definitions(&stmt).expect("lower").iter().map(ToSql::to_sql).collect();
	let field = sql.iter().find(|s| s.contains("FIELD parent")).expect("the field");
	assert!(field.contains("$value = id"), "{field}");
}

#[test]
fn test_lower_pgsql_foreign_key_refusals() {
	for (sql, reason) in [
		(
			"CREATE TABLE p (id INT PRIMARY KEY, a INT, b INT, FOREIGN KEY (a, b) REFERENCES u(id, id))",
			"several columns",
		),
		(
			"CREATE TABLE p (id INT PRIMARY KEY, a INT REFERENCES u(name))",
			"id column of the target",
		),
		(
			"CREATE TABLE p (id INT PRIMARY KEY, a INT, FOREIGN KEY (a) REFERENCES u(name))",
			"id column of the target",
		),
		("CREATE TABLE p (id INT REFERENCES u(id))", "foreign key on the id column"),
	] {
		let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
		let err = lower_pgsql_ddl_definitions(&stmt).unwrap_err();
		assert_eq!(err.pg_sqlstate(), "0A000", "{sql}: {err}");
		assert!(err.to_string().contains(reason), "{sql}: {err}");
	}
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE TABLE p (id INT PRIMARY KEY, a INT, FOREIGN KEY (zz) REFERENCES u(id))",
	)
	.expect("parse");
	assert_eq!(lower_pgsql_ddl_definitions(&stmt).unwrap_err().pg_sqlstate(), "42703");
}

#[test]
fn test_lower_pgsql_foreign_key_table_constraint() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE TABLE comments (
			id SERIAL PRIMARY KEY,
			post_id INT,
			user_id INT,
			body TEXT,
			FOREIGN KEY (post_id) REFERENCES posts(id),
			FOREIGN KEY (user_id) REFERENCES users(id)
		);",
	)
	.expect("parse");

	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	// 1 DefineTable + 1 DefineSequence (id) + 4 DefineField + 1 PK Index: the foreign keys are
	// part of the definitions of their columns.
	assert_eq!(exprs.len(), 7);
	for column in ["post_id", "user_id"] {
		let field = exprs
			.iter()
			.find_map(|e| match e {
				Expr::Define(d) => match &**d {
					DefineStatement::Field(f) if f.name.to_sql() == column => Some(f),
					_ => None,
				},
				_ => None,
			})
			.expect("the column is defined");
		assert!(field.reference.is_some(), "{column}");
		assert!(
			field.field_kind.as_ref().is_some_and(|k| k.to_sql().starts_with("none | record<")),
			"{column}"
		);
	}
}

#[test]
fn test_lower_pgsql_complex_defaults() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE TABLE configs (
			id INT PRIMARY KEY,
			is_enabled BOOL DEFAULT true,
			counter INT DEFAULT 0,
			rate FLOAT DEFAULT 1.5,
			tag TEXT DEFAULT 'general'
		);",
	)
	.expect("parse");

	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	assert_eq!(exprs.len(), 7);

	for expr in &exprs {
		if let Expr::Define(d) = expr
			&& let DefineStatement::Field(f) = &**d
		{
			match f.name.to_sql().as_str() {
				"is_enabled" | "counter" | "rate" | "tag" => {
					assert!(matches!(
						f.default,
						surrealdb_expr::expr::statements::define::DefineDefault::Set(_)
					));
				}
				_ => {}
			}
		}
	}
}

#[test]
fn test_lower_pgsql_add_column_is_nullable_unless_not_null() {
	let field_kind = |sql: &str| {
		let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
		let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
		let Expr::Define(d) = &exprs[0] else {
			panic!("Expected Expr::Define");
		};
		let DefineStatement::Field(f) = &**d else {
			panic!("Expected DefineField");
		};
		f.field_kind.clone()
	};
	assert_eq!(
		field_kind("ALTER TABLE users ADD COLUMN phone TEXT;"),
		Some(Kind::either(vec![Kind::None, Kind::Null, Kind::String]))
	);
	assert_eq!(field_kind("ALTER TABLE users ADD COLUMN phone TEXT NOT NULL;"), Some(Kind::String));
}

#[test]
fn test_lower_pgsql_alter_table_multiple_actions() {
	let stmt = surrealdb_pgsql::parse_statement(
		"ALTER TABLE accounts
			ADD COLUMN status VARCHAR(20) DEFAULT 'pending',
			DROP COLUMN IF EXISTS old_status,
			ALTER COLUMN balance TYPE NUMERIC,
			ALTER COLUMN balance SET DEFAULT 0.00,
			ALTER COLUMN user_id SET NOT NULL;",
	)
	.expect("parse");

	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	assert_eq!(exprs.len(), 5);

	// Action 1: Add column status (DefineField)
	assert!(matches!(&exprs[0], Expr::Define(d) if matches!(&**d, DefineStatement::Field(_))));
	// Action 2: Drop column old_status (RemoveField)
	assert!(matches!(&exprs[1], Expr::Remove(r) if matches!(&**r, RemoveStatement::Field(_))));
	// Action 3: Alter balance type (AlterField: the rest of the field is kept)
	assert!(matches!(&exprs[2], Expr::Alter(_)));
	// Action 4: Set balance default (AlterField: the rest of the field is kept)
	assert!(matches!(&exprs[3], Expr::Alter(_)));
	// Action 5: Set user_id NOT NULL (AlterField)
	assert!(matches!(&exprs[4], Expr::Alter(_)));
}

#[test]
fn test_lower_pgsql_alter_table_constraints() {
	let add_pk = surrealdb_pgsql::parse_statement(
		"ALTER TABLE memberships ADD PRIMARY KEY (user_id, team_id);",
	)
	.expect("parse");
	let exprs_pk = lower_pgsql_ddl_definitions(&add_pk).expect("lower");
	assert_eq!(exprs_pk.len(), 1);
	let Expr::Define(d) = &exprs_pk[0] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Index(idx) = &**d else {
		panic!("Expected DefineIndex");
	};
	assert_eq!(
		idx.index,
		surrealdb_expr::expr::index_kind::Index::Uniq {
			nulls_not_distinct: false
		}
	);
	assert_eq!(idx.cols.len(), 2);

	let add_uq =
		surrealdb_pgsql::parse_statement("ALTER TABLE memberships ADD UNIQUE (user_id, role);")
			.expect("parse");
	let exprs_uq = lower_pgsql_ddl_definitions(&add_uq).expect("lower");
	assert_eq!(exprs_uq.len(), 1);
	let Expr::Define(d) = &exprs_uq[0] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Index(idx) = &**d else {
		panic!("Expected DefineIndex");
	};
	assert_eq!(
		idx.index,
		surrealdb_expr::expr::index_kind::Index::Uniq {
			nulls_not_distinct: false
		}
	);
	assert_eq!(idx.cols.len(), 2);

	let drop_con = surrealdb_pgsql::parse_statement(
		"ALTER TABLE memberships DROP CONSTRAINT IF EXISTS uq_user_role;",
	)
	.expect("parse");
	let exprs_drop_con = lower_pgsql_ddl_definitions(&drop_con).expect("lower");
	assert_eq!(exprs_drop_con.len(), 1);
	assert!(
		matches!(&exprs_drop_con[0], Expr::Remove(r) if matches!(&**r, RemoveStatement::Index(_)))
	);
}

#[test]
fn test_lower_pgsql_create_index_concurrently() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE INDEX CONCURRENTLY idx_audit_created ON audit_logs (created_at);",
	)
	.expect("parse");

	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	assert_eq!(exprs.len(), 1);
	let Expr::Define(d) = &exprs[0] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Index(idx) = &**d else {
		panic!("Expected DefineIndex");
	};
	assert!(idx.concurrently);
	assert_eq!(idx.index, surrealdb_expr::expr::index_kind::Index::Idx);
}

#[test]
fn test_lower_pgsql_create_view_wildcard() {
	let stmt = surrealdb_pgsql::parse_statement("CREATE VIEW v_all_users AS SELECT * FROM users;")
		.expect("parse");

	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	assert_eq!(exprs.len(), 1);
	let Expr::Define(d) = &exprs[0] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Table(t) = &**d else {
		panic!("Expected DefineTable");
	};
	assert!(t.view.is_some());
	let v = t.view.as_ref().unwrap();
	assert_eq!(v.expr, surrealdb_expr::expr::Fields::all());
}

#[test]
fn test_lower_mysql_table_all_datatypes() {
	let stmt = surrealdb_mysql::parse_statement(
		"CREATE TABLE everything (
			c_tiny TINYINT NOT NULL,
			c_small SMALLINT,
			c_med MEDIUMINT,
			c_int INT,
			c_big BIGINT,
			c_float FLOAT,
			c_double DOUBLE,
			c_dec DECIMAL(12,4),
			c_char CHAR(10),
			c_vchar VARCHAR(255),
			c_text TEXT,
			c_ltext LONGTEXT,
			c_bool BOOLEAN,
			c_date DATE,
			c_time TIME,
			c_datetime DATETIME,
			c_ts TIMESTAMP,
			c_year YEAR,
			c_json JSON,
			c_blob BLOB,
			c_bit BIT(1),
			c_enum ENUM('yes', 'no'),
			c_set SET('a', 'b', 'c')
		);",
	)
	.expect("parse");

	let exprs = lower_mysql_ddl_definitions(&stmt).expect("lower");
	// 1 DefineTable + 23 DefineField = 24
	assert_eq!(exprs.len(), 24);
}

#[test]
fn test_lower_mysql_foreign_keys() {
	let stmt = surrealdb_mysql::parse_statement(
		"CREATE TABLE order_entries (
			entry_id INT PRIMARY KEY,
			order_id INT,
			product_id INT,
			CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders (id),
			CONSTRAINT fk_prod FOREIGN KEY (product_id) REFERENCES products (id)
		);",
	)
	.expect("parse");

	let exprs = lower_mysql_ddl_definitions(&stmt).expect("lower");
	// 1 DefineTable + 3 DefineField + 1 PK Index + 2 FK fields = 7
	assert_eq!(exprs.len(), 7);
}

#[test]
fn test_lower_mysql_drop_multiple_tables() {
	let stmt =
		surrealdb_mysql::parse_statement("DROP TABLE IF EXISTS t1, t2, t3, t4;").expect("parse");

	let exprs = lower_mysql_ddl_definitions(&stmt).expect("lower");
	assert_eq!(exprs.len(), 4);
	for (i, expr) in exprs.iter().enumerate() {
		let Expr::Remove(r) = expr else {
			panic!("Expected Expr::Remove");
		};
		let RemoveStatement::Table(t) = &**r else {
			panic!("Expected RemoveTable");
		};
		assert!(t.if_exists);
		assert_eq!(t.name.to_sql(), format!("t{}", i + 1));
	}
}

#[test]
fn test_lower_mysql_database_ddl() {
	let create_stmt =
		surrealdb_mysql::parse_statement("CREATE DATABASE IF NOT EXISTS my_db;").expect("parse");
	let exprs = lower_mysql_ddl_definitions(&create_stmt).expect("lower create db");
	assert_eq!(exprs.len(), 1);
	let Expr::Define(d) = &exprs[0] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Database(db) = &**d else {
		panic!("Expected DefineDatabase");
	};
	assert_eq!(db.name.to_sql(), "my_db");
	assert_eq!(db.kind, DefineKind::IfNotExists);

	let drop_stmt =
		surrealdb_mysql::parse_statement("DROP DATABASE IF EXISTS my_db;").expect("parse");
	let exprs = lower_mysql_ddl_definitions(&drop_stmt).expect("lower drop db");
	assert_eq!(exprs.len(), 1);
	let Expr::Remove(r) = &exprs[0] else {
		panic!("Expected Expr::Remove");
	};
	let RemoveStatement::Database(db) = &**r else {
		panic!("Expected RemoveDatabase");
	};
	assert_eq!(db.name.to_sql(), "my_db");
	assert!(db.if_exists);
}

#[test]
fn test_lower_mysql_index_ddl() {
	let create_idx =
		surrealdb_mysql::parse_statement("CREATE UNIQUE INDEX idx_email ON users (email);")
			.expect("parse");
	let exprs = lower_mysql_ddl_definitions(&create_idx).expect("lower create index");
	assert_eq!(exprs.len(), 1);
	let Expr::Define(d) = &exprs[0] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Index(idx) = &**d else {
		panic!("Expected DefineIndex");
	};
	assert_eq!(idx.name.to_sql(), "idx_email");
	assert_eq!(idx.what.to_sql(), "users");
	assert_eq!(idx.cols.len(), 1);

	let drop_idx =
		surrealdb_mysql::parse_statement("DROP INDEX idx_email ON users;").expect("parse");
	let exprs = lower_mysql_ddl_definitions(&drop_idx).expect("lower drop index");
	assert_eq!(exprs.len(), 1);
	let Expr::Remove(r) = &exprs[0] else {
		panic!("Expected Expr::Remove");
	};
	let RemoveStatement::Index(idx) = &**r else {
		panic!("Expected RemoveIndex");
	};
	assert_eq!(idx.name.to_sql(), "idx_email");
	assert_eq!(idx.what.to_sql(), "users");
}

#[test]
fn test_lower_mysql_view_ddl() {
	let create_view = surrealdb_mysql::parse_statement(
		"CREATE OR REPLACE VIEW active_users AS SELECT id, name FROM users WHERE active = 1;",
	)
	.expect("parse");
	let exprs = lower_mysql_ddl_definitions(&create_view).expect("lower create view");
	assert_eq!(exprs.len(), 1);
	let Expr::Define(d) = &exprs[0] else {
		panic!("Expected Expr::Define");
	};
	let DefineStatement::Table(tbl) = &**d else {
		panic!("Expected DefineTable for view");
	};
	assert_eq!(tbl.name.to_sql(), "active_users");
	assert_eq!(tbl.kind, DefineKind::Overwrite);
	assert!(tbl.view.is_some());

	let drop_view =
		surrealdb_mysql::parse_statement("DROP VIEW IF EXISTS active_users, old_users;")
			.expect("parse");
	let exprs = lower_mysql_ddl_definitions(&drop_view).expect("lower drop view");
	assert_eq!(exprs.len(), 2);
	let Expr::Remove(r) = &exprs[0] else {
		panic!("Expected Expr::Remove");
	};
	let RemoveStatement::Table(tbl) = &**r else {
		panic!("Expected RemoveTable");
	};
	assert_eq!(tbl.name.to_sql(), "active_users");
	assert!(tbl.if_exists);
}

#[test]
fn test_lower_mysql_alter_table() {
	let alter = surrealdb_mysql::parse_statement(
		"ALTER TABLE users ADD COLUMN age INT, DROP COLUMN legacy, RENAME TO customers;",
	)
	.expect("parse");
	let exprs = lower_mysql_ddl_definitions(&alter).expect("lower alter table");
	assert_eq!(exprs.len(), 3);
	// 1: DefineField (ADD age)
	assert!(matches!(&exprs[0], Expr::Define(d) if matches!(&**d, DefineStatement::Field(_))));
	// 2: RemoveField (DROP legacy)
	assert!(matches!(&exprs[1], Expr::Remove(r) if matches!(&**r, RemoveStatement::Field(_))));
	// 3: DefineTable (RENAME customers)
	assert!(matches!(&exprs[2], Expr::Define(d) if matches!(&**d, DefineStatement::Table(_))));
}

/// The `(name, flexible)` of every field `exprs` defines.
fn field_flexibility(exprs: &[Expr]) -> Vec<(String, bool)> {
	exprs
		.iter()
		.filter_map(|expr| match expr {
			Expr::Define(define) => match &**define {
				DefineStatement::Field(field) => Some((field.name.to_sql(), field.flexible)),
				DefineStatement::Namespace(_)
				| DefineStatement::Database(_)
				| DefineStatement::Function(_)
				| DefineStatement::Analyzer(_)
				| DefineStatement::Param(_)
				| DefineStatement::Table(_)
				| DefineStatement::Event(_)
				| DefineStatement::Index(_)
				| DefineStatement::User(_)
				| DefineStatement::Model(_)
				| DefineStatement::Access(_)
				| DefineStatement::Config(_)
				| DefineStatement::Api(_)
				| DefineStatement::Bucket(_)
				| DefineStatement::Sequence(_)
				| DefineStatement::Type(_)
				| DefineStatement::Module(_) => None,
			},
			Expr::Literal(_)
			| Expr::Param(_)
			| Expr::Idiom(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Block(_)
			| Expr::Constant(_)
			| Expr::Prefix {
				..
			}
			| Expr::Postfix {
				..
			}
			| Expr::Binary {
				..
			}
			| Expr::FunctionCall(_)
			| Expr::Closure(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Return(_)
			| Expr::Throw(_)
			| Expr::IfElse(_)
			| Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Upsert(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Alter(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Sleep(_)
			| Expr::Explain {
				..
			} => None,
		})
		.collect()
}

#[test]
fn json_columns_of_a_created_table_hold_any_json_value_and_are_not_flexible() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE TABLE docs (id INT PRIMARY KEY, a JSONB NOT NULL, b JSON, c JSONB[], d TEXT, e TEXT[]);",
	)
	.expect("parse");
	assert_eq!(
		field_flexibility(&lower_pgsql_ddl_definitions(&stmt).expect("lower")),
		[
			("id".to_string(), false),
			("a".to_string(), false),
			("b".to_string(), false),
			("c".to_string(), false),
			("d".to_string(), false),
			("e".to_string(), false),
		]
	);
	let stmt =
		surrealdb_mysql::parse_statement("CREATE TABLE docs (id INT PRIMARY KEY, a JSON, b TEXT);")
			.expect("parse");
	assert_eq!(
		field_flexibility(&lower_mysql_ddl_definitions(&stmt).expect("lower")),
		[("id".to_string(), false), ("a".to_string(), false), ("b".to_string(), false)]
	);
}

/// The SurrealQL of every statement `exprs` holds.
/// The name and declared `SQL TYPE` text of every field the statement defines, in order.
fn declared_sql_types(exprs: &[Expr]) -> Vec<(String, Option<String>)> {
	exprs
		.iter()
		.filter_map(|expr| match expr {
			Expr::Define(define) => {
				let DefineStatement::Field(field) = &**define else {
					return None;
				};
				Some((field.name.to_sql(), field.sql_type.as_ref().map(|t| t.text.clone())))
			}
			Expr::Alter(alter) => {
				let AlterStatement::Field(field) = &**alter else {
					return None;
				};
				let AlterKind::Set(sql_type) = &field.sql_type else {
					return None;
				};
				Some((field.name.to_sql(), Some(sql_type.text.clone())))
			}
			_ => None,
		})
		.collect()
}

fn declared(name: &str, text: &str) -> (String, Option<String>) {
	(name.to_string(), Some(text.to_string()))
}

#[test]
fn pgsql_columns_carry_their_canonical_declared_type() {
	let create = surrealdb_pgsql::parse_statement(
		"CREATE TABLE t (
			a INT,
			b VARCHAR(255),
			c NUMERIC(10, 2),
			d TIMESTAMP(3) WITH TIME ZONE,
			e INT[][],
			f CHARACTER VARYING(7)
		);",
	)
	.expect("parse");
	let exprs = lower_pgsql_ddl_definitions(&create).expect("lower");
	assert_eq!(
		declared_sql_types(&exprs),
		vec![
			declared("a", "int"),
			declared("b", "varchar(255)"),
			declared("c", "numeric(10,2)"),
			declared("d", "timestamp(3) with time zone"),
			declared("e", "int[][]"),
			declared("f", "character varying(7)"),
		]
	);

	let add = surrealdb_pgsql::parse_statement("ALTER TABLE t ADD COLUMN g NUMERIC(5,1);")
		.expect("parse");
	let exprs = lower_pgsql_ddl_definitions(&add).expect("lower");
	assert_eq!(declared_sql_types(&exprs), vec![declared("g", "numeric(5,1)")]);

	let retype = surrealdb_pgsql::parse_statement("ALTER TABLE t ALTER COLUMN b TYPE VARCHAR(40);")
		.expect("parse");
	let exprs = lower_pgsql_ddl_definitions(&retype).expect("lower");
	assert_eq!(declared_sql_types(&exprs), vec![declared("b", "varchar(40)")]);
}

#[test]
fn mysql_columns_carry_their_canonical_declared_type() {
	let create = surrealdb_mysql::parse_statement(
		"CREATE TABLE m (
			a VARCHAR(20),
			b INT UNSIGNED,
			c DECIMAL(10,2),
			d ENUM('x,y', 'z')
		);",
	)
	.expect("parse");
	let exprs = lower_mysql_ddl_definitions(&create).expect("lower");
	assert_eq!(
		declared_sql_types(&exprs),
		vec![
			declared("a", "varchar(20)"),
			declared("b", "int unsigned"),
			declared("c", "decimal(10,2)"),
			declared("d", "enum('x,y','z')"),
		]
	);

	let modify = surrealdb_mysql::parse_statement("ALTER TABLE m MODIFY COLUMN a VARCHAR(30);")
		.expect("parse");
	let exprs = lower_mysql_ddl_definitions(&modify).expect("lower");
	assert_eq!(declared_sql_types(&exprs), vec![declared("a", "varchar(30)")]);
}

#[test]
fn a_declared_type_that_cannot_be_spelt_is_refused() {
	assert!(super::ddl::mysql_declared_type("varchar(").is_err());
	assert!(super::ddl::mysql_declared_type("   ").is_err());
}

fn statements_sql(exprs: &[Expr]) -> Vec<String> {
	exprs.iter().map(|expr| expr.to_sql()).collect()
}

#[test]
fn serial_and_auto_increment_columns_draw_from_a_sequence_of_their_own() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE TABLE gen (id SERIAL PRIMARY KEY, n BIGSERIAL, m INT, d SERIAL DEFAULT 7);",
	)
	.expect("parse");
	let sql = statements_sql(&lower_pgsql_ddl_definitions(&stmt).expect("lower"));
	let defines = |name: &str| sql.iter().filter(|s| s.contains(name)).cloned().collect::<Vec<_>>();
	for column in ["id", "n"] {
		let sequence = format!("gen.{column}");
		let lines = defines(&sequence);
		assert_eq!(lines.len(), 2, "{lines:?}");
		assert!(lines[0].starts_with("DEFINE SEQUENCE OVERWRITE"), "{lines:?}");
		assert!(lines[0].contains("START 1"), "{lines:?}");
		assert!(lines[1].starts_with("DEFINE FIELD"), "{lines:?}");
		assert!(lines[1].contains(&format!("::nextval('{sequence}')")), "{lines:?}");
		assert!(!lines[1].contains("option<"), "a serial column is NOT NULL: {lines:?}");
	}
	assert!(defines("gen.m").is_empty());
	assert!(defines("gen.d").is_empty(), "an explicit DEFAULT is kept");

	let stmt = surrealdb_mysql::parse_statement(
		"CREATE TABLE gen (id INT AUTO_INCREMENT PRIMARY KEY, name TEXT);",
	)
	.expect("parse");
	let sql = statements_sql(&lower_mysql_ddl_definitions(&stmt).expect("lower"));
	assert_eq!(sql.iter().filter(|s| s.contains("gen.id")).count(), 2, "{sql:?}");
	assert!(!sql.iter().any(|s| s.contains("gen.name")), "{sql:?}");
}

fn delete_strategy_of(sql: &str, column: &str) -> ReferenceDeleteStrategy {
	let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	exprs
		.iter()
		.rev()
		.find_map(|e| {
			let Expr::Define(d) = e else {
				return None;
			};
			let DefineStatement::Field(f) = &**d else {
				return None;
			};
			(f.name.to_sql() == column)
				.then(|| f.reference.as_ref().map(|r| r.on_delete.clone()))?
		})
		.expect("the column has a reference")
}

#[test]
fn test_lower_pgsql_foreign_key_on_delete_actions() {
	for (action, expected) in [
		("CASCADE", ReferenceDeleteStrategy::Cascade),
		("SET NULL", ReferenceDeleteStrategy::Unset),
		("NO ACTION", ReferenceDeleteStrategy::Reject),
		("RESTRICT", ReferenceDeleteStrategy::Reject),
	] {
		let column = format!(
			"CREATE TABLE posts (id INT PRIMARY KEY, author_id INT REFERENCES users(id) ON DELETE \
			 {action} ON UPDATE CASCADE)"
		);
		assert_eq!(delete_strategy_of(&column, "author_id"), expected, "{action}");
		let table = format!(
			"CREATE TABLE posts (id INT PRIMARY KEY, author_id INT, FOREIGN KEY (author_id) \
			 REFERENCES users(id) ON UPDATE NO ACTION ON DELETE {action})"
		);
		assert_eq!(delete_strategy_of(&table, "author_id"), expected, "{action}");
	}
	let default = "CREATE TABLE posts (id INT PRIMARY KEY, author_id INT REFERENCES users(id))";
	assert_eq!(delete_strategy_of(default, "author_id"), ReferenceDeleteStrategy::Reject);
}

#[test]
fn on_delete_set_default_is_refused() {
	for sql in [
		"CREATE TABLE posts (id INT PRIMARY KEY, author_id INT REFERENCES users(id) ON DELETE SET DEFAULT)",
		"CREATE TABLE posts (id INT PRIMARY KEY, author_id INT, FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE SET DEFAULT)",
		"ALTER TABLE posts ADD COLUMN author_id INT REFERENCES users(id) ON DELETE SET DEFAULT",
	] {
		let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
		let err = lower_pgsql_ddl_definitions(&stmt).unwrap_err();
		assert_eq!(err.pg_sqlstate(), "0A000", "{sql}: {err}");
	}
}

mod constraints {
	use surrealdb_expr::expr::{Expr, Kind};
	use surrealdb_types::ToSql;

	use super::super::ddl::{lower_pgsql_ddl_definitions_with, lower_pgsql_ddl_with};
	use crate::{
		BinderCatalog, ColumnAssert, ColumnInfo, ColumnShape, EmptyCatalog, ForeignKeyInfo,
		IndexInfo, MemoryCatalog, PlanError, ReferentialAction, StagedCatalog, TableSchema,
	};

	fn index(name: &str, columns: &[&str]) -> IndexInfo {
		IndexInfo {
			name: name.to_owned(),
			columns: columns.iter().map(|c| (*c).to_owned()).collect(),
			unique: true,
		}
	}

	fn columns(names: &[&str]) -> Vec<ColumnInfo> {
		names.iter().enumerate().map(|(i, n)| ColumnInfo::new(*n, i)).collect()
	}

	/// `post` has a primary key index, a unique constraint, a plain unique index and a foreign
	/// key; `plain` has only the record id.
	fn catalog() -> MemoryCatalog {
		let post = TableSchema::new("post", columns(&["id", "email", "slug", "author"]))
			.with_schema("public")
			.with_indexes(vec![
				index("post_id_pk", &["id"]),
				index("post_email_uniq", &["email"]),
				index("by_slug", &["slug"]),
			])
			.with_foreign_keys(vec![ForeignKeyInfo {
				column: "author".to_owned(),
				target_table: "user".to_owned(),
				on_delete: ReferentialAction::NoAction,
			}]);
		let plain = TableSchema::new("plain", columns(&["id", "n"])).with_schema("public");
		MemoryCatalog::new().with_listed_tables(vec![post, plain])
	}

	fn lower(catalog: &MemoryCatalog, sql: &str) -> Result<Vec<String>, PlanError> {
		let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
		lower_pgsql_ddl_definitions_with(&stmt, catalog)
			.map(|exprs| exprs.iter().map(Expr::to_sql).collect())
	}

	#[test]
	fn drop_constraint_of_a_unique_constraint_removes_its_index() {
		let sql = lower(&catalog(), "ALTER TABLE post DROP CONSTRAINT post_email_uniq").unwrap();
		assert_eq!(sql, ["REMOVE INDEX post_email_uniq ON post"]);
	}

	#[test]
	fn drop_constraint_of_a_foreign_key_drops_the_reference_and_keeps_the_type() {
		let sql = lower(&catalog(), "ALTER TABLE post DROP CONSTRAINT post_author_fkey").unwrap();
		assert_eq!(sql, ["ALTER FIELD author ON post DROP ASSERT DROP REFERENCE"]);
	}

	#[test]
	fn drop_constraint_of_the_primary_key_removes_the_pk_index_or_is_refused() {
		let sql = lower(&catalog(), "ALTER TABLE post DROP CONSTRAINT post_pkey").unwrap();
		assert_eq!(sql, ["REMOVE INDEX post_id_pk ON post"]);
		let err = lower(&catalog(), "ALTER TABLE plain DROP CONSTRAINT plain_pkey").unwrap_err();
		assert_eq!(err.pg_sqlstate(), "0A000", "{err}");
		assert!(err.to_string().contains("record id"), "{err}");
	}

	#[test]
	fn drop_constraint_of_an_unknown_name_is_undefined_unless_if_exists() {
		for name in ["nope", "by_slug", "post_id_pk"] {
			let err =
				lower(&catalog(), &format!("ALTER TABLE post DROP CONSTRAINT {name}")).unwrap_err();
			assert_eq!(err.pg_sqlstate(), "42704", "{name}: {err}");
			assert_eq!(
				err.to_string(),
				format!("constraint \"{name}\" of relation \"post\" does not exist")
			);
			let sql =
				lower(&catalog(), &format!("ALTER TABLE post DROP CONSTRAINT IF EXISTS {name}"))
					.unwrap();
			assert!(sql.is_empty(), "{name}: {sql:?}");
		}
	}

	#[test]
	fn drop_constraint_of_a_table_the_catalog_does_not_list_names_an_index() {
		let sql = lower(&catalog(), "ALTER TABLE other DROP CONSTRAINT IF EXISTS x").unwrap();
		assert_eq!(sql, ["REMOVE INDEX IF EXISTS x ON other"]);
		let stmt = surrealdb_pgsql::parse_statement("ALTER TABLE post DROP CONSTRAINT x").unwrap();
		let exprs = lower_pgsql_ddl_with(&stmt, &EmptyCatalog).unwrap();
		assert_eq!(exprs[0].to_sql(), "REMOVE INDEX x ON post");
	}

	/// The steps of the one statement that `sql` lowers to.
	fn steps(catalog: &dyn BinderCatalog, sql: &str) -> Result<Vec<String>, PlanError> {
		let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
		let exprs = lower_pgsql_ddl_with(&stmt, catalog)?;
		let [Expr::Block(block)] = exprs.as_slice() else {
			panic!("one block was expected: {exprs:?}");
		};
		Ok(block.0.iter().map(Expr::to_sql).collect())
	}

	fn shaped(table: &str, column: &str, kind: Kind, assert: ColumnAssert) -> MemoryCatalog {
		MemoryCatalog::new().with_column_shape(
			table,
			column,
			ColumnShape {
				kind: Some(kind),
				assert,
				has_default: false,
			},
		)
	}

	fn nullable(kind: Kind) -> Kind {
		Kind::either(vec![Kind::None, Kind::Null, kind])
	}

	#[test]
	fn add_foreign_key_references_an_existing_link_column() {
		let catalog = MemoryCatalog::new().with_link("post", "author", "user");
		let sql = steps(
			&catalog,
			"ALTER TABLE post ADD CONSTRAINT fk FOREIGN KEY (author) REFERENCES \"user\"(id) ON DELETE CASCADE",
		)
		.unwrap();
		// The rows are checked, then the column is given its reference and assertion.
		assert_eq!(sql.len(), 2, "{sql:?}");
		assert!(sql[0].starts_with("IF array::len((SELECT VALUE id FROM post WHERE"), "{sql:?}");
		assert!(sql[0].contains("THROW"), "{sql:?}");
		assert!(sql[1].starts_with("ALTER FIELD author ON post"), "{sql:?}");
		assert!(sql[1].contains("REFERENCE ON DELETE CASCADE"), "{sql:?}");
		assert!(sql[1].contains("ASSERT $value = NONE OR"), "{sql:?}");
		assert!(!sql[1].contains("TYPE"), "{sql:?}");
	}

	#[test]
	fn add_foreign_key_on_a_scalar_column_converts_the_column_and_its_rows() {
		let catalog = shaped("post", "author", nullable(Kind::Int), ColumnAssert::None);
		let sql = steps(
			&catalog,
			"ALTER TABLE post ADD CONSTRAINT fk FOREIGN KEY (author) REFERENCES \"user\"(id) ON DELETE SET NULL",
		)
		.unwrap();
		assert_eq!(sql.len(), 5, "{sql:#?}");
		// 1. A value that names no row fails the statement before anything changes.
		assert!(sql[0].starts_with("IF array::len((SELECT VALUE id FROM post WHERE"), "{sql:#?}");
		assert!(sql[0].contains("record::exists"), "{sql:#?}");
		assert!(
			sql[0].contains(r#"violates foreign key constraint "post_author_fkey""#),
			"{sql:#?}"
		);
		// 2. The type takes links as well as the column's own values.
		assert_eq!(sql[1], "ALTER FIELD author ON post TYPE none | null | int | record<user>");
		// 3. The rows hold links, and `NULL` is absent.
		assert!(
			sql[2].starts_with("UPDATE post SET author = (IF author = NULL THEN NONE ELSE"),
			"{sql:#?}"
		);
		assert!(sql[2].contains("type::record('user', author)"), "{sql:#?}");
		assert!(sql[2].ends_with("WHERE author != NONE RETURN NONE"), "{sql:#?}");
		// 4. The type narrows to the link, keeping what the rows hold.
		assert!(sql[3].starts_with("ALTER FIELD author ON post TYPE none | record<user> MIGRATE EXISTING ROWS AS post_author_fkey USING"), "{sql:#?}");
		assert!(sql[3].ends_with("FALLBACK NONE"), "{sql:#?}");
		// 5. The reference and the assertion.
		assert!(
			sql[4].starts_with("ALTER FIELD author ON post ASSERT $value = NONE OR"),
			"{sql:#?}"
		);
		assert!(sql[4].contains("REFERENCE ON DELETE UNSET"), "{sql:#?}");
	}

	#[test]
	fn add_foreign_key_on_a_not_null_column_makes_a_required_link() {
		for assert in [ColumnAssert::NotNull, ColumnAssert::None] {
			let kind = if assert == ColumnAssert::NotNull {
				Kind::Int
			} else {
				nullable(Kind::Int)
			};
			let catalog = shaped("post", "author", kind.clone(), assert);
			let sql = steps(
				&catalog,
				"ALTER TABLE post ADD FOREIGN KEY (author) REFERENCES \"user\"(id)",
			)
			.unwrap();
			assert_eq!(sql.len(), 5, "{sql:#?}");
			// The assertion reads `$value`, so the engine refuses to keep it over a new type.
			assert!(!sql[1].contains("ASSERT $value != NONE"), "{sql:#?}");
			assert!(sql[2].contains("type::record('user', author)"), "{sql:#?}");
			let required = assert == ColumnAssert::NotNull;
			assert_eq!(sql[3].contains("TYPE record<user>"), required, "{sql:#?}");
			assert_eq!(sql[3].contains("FALLBACK user:0"), required, "{sql:#?}");
		}
	}

	#[test]
	fn add_foreign_key_on_a_column_that_cannot_become_a_link_is_refused() {
		let refused = |catalog: MemoryCatalog, reason: &str| {
			let err = lower(
				&catalog,
				"ALTER TABLE post ADD FOREIGN KEY (author) REFERENCES \"user\"(id)",
			)
			.unwrap_err();
			assert_eq!(err.pg_sqlstate(), "0A000", "{err}");
			assert!(err.to_string().contains(reason), "{err}");
		};
		refused(shaped("post", "author", Kind::Bool, ColumnAssert::None), "integer, text or uuid");
		refused(shaped("post", "author", Kind::Int, ColumnAssert::Other), "assertion of its own");
		refused(
			MemoryCatalog::new().with_column_shape(
				"post",
				"author",
				ColumnShape {
					kind: Some(Kind::Int),
					assert: ColumnAssert::None,
					has_default: true,
				},
			),
			"default",
		);
		let err = lower(
			&MemoryCatalog::new(),
			"ALTER TABLE post ADD FOREIGN KEY (author) REFERENCES \"user\"(id)",
		)
		.unwrap_err();
		assert_eq!(err.pg_sqlstate(), "42703", "{err}");
	}

	#[test]
	fn add_foreign_key_to_the_wrong_table_or_column_is_refused() {
		let catalog = MemoryCatalog::new().with_link("post", "author", "user");
		let err = lower(&catalog, "ALTER TABLE post ADD FOREIGN KEY (author) REFERENCES team(id)")
			.unwrap_err();
		assert_eq!(err.pg_sqlstate(), "0A000", "{err}");
		let err =
			lower(&catalog, "ALTER TABLE post ADD FOREIGN KEY (author) REFERENCES \"user\"(name)")
				.unwrap_err();
		assert_eq!(err.pg_sqlstate(), "0A000", "{err}");
		let err = lower(
			&catalog,
			"ALTER TABLE post ADD FOREIGN KEY (author, editor) REFERENCES \"user\"(id, id)",
		)
		.unwrap_err();
		assert_eq!(err.pg_sqlstate(), "0A000", "{err}");
	}

	#[test]
	fn a_table_created_if_not_exists_keeps_the_declaration_of_the_table_that_exists() {
		let create = surrealdb_pgsql::parse_statement(
			"CREATE TABLE IF NOT EXISTS post (id integer PRIMARY KEY, author integer NOT NULL, note text)",
		)
		.unwrap();
		let alter = "ALTER TABLE post ADD FOREIGN KEY (note) REFERENCES \"user\"(id)";
		// The table exists, and its column is a link already.
		let existing = MemoryCatalog::new().with_link("post", "author", "user");
		let mut staged = StagedCatalog::new(&existing);
		staged.stage(&lower_pgsql_ddl_with(&create, &staged).unwrap());
		assert_eq!(staged.link_table(None, "post", "author"), Some("user".to_owned()));
		// The table is new: the statement declares the column.
		let mut staged = StagedCatalog::new(&EmptyCatalog);
		staged.stage(&lower_pgsql_ddl_with(&create, &staged).unwrap());
		assert_eq!(staged.link_table(None, "post", "author"), None);
		assert!(staged.column_shape(None, "post", "author").is_some());
		assert_eq!(steps(&staged, alter).unwrap().len(), 5);
		staged.stage(
			&lower_pgsql_ddl_with(&surrealdb_pgsql::parse_statement(alter).unwrap(), &staged)
				.unwrap(),
		);
		assert_eq!(staged.link_table(None, "post", "note"), Some("user".to_owned()));
	}

	#[test]
	fn add_foreign_key_over_a_column_of_an_earlier_statement_sees_its_declaration() {
		let create = surrealdb_pgsql::parse_statement(
			"CREATE TABLE post (id integer PRIMARY KEY, author integer NOT NULL)",
		)
		.unwrap();
		let alter = surrealdb_pgsql::parse_statement(
			"ALTER TABLE post ADD FOREIGN KEY (author) REFERENCES \"user\"(id)",
		)
		.unwrap();
		let mut staged = StagedCatalog::new(&EmptyCatalog);
		staged.stage(&lower_pgsql_ddl_with(&create, &staged).unwrap());
		let sql =
			steps(&staged, "ALTER TABLE post ADD FOREIGN KEY (author) REFERENCES \"user\"(id)")
				.unwrap();
		assert_eq!(sql.len(), 5, "{sql:#?}");
		assert!(sql[3].contains("TYPE record<user>"), "{sql:#?}");
		// The conversion is itself staged: the column is a link now.
		staged.stage(&lower_pgsql_ddl_with(&alter, &staged).unwrap());
		assert_eq!(staged.link_table(None, "post", "author"), Some("user".to_owned()));
		// A table created again forgets the columns of the one before it.
		staged.stage(&lower_pgsql_ddl_with(&create, &staged).unwrap());
		assert_eq!(staged.link_table(None, "post", "author"), None);
	}

	/// `post` and `user` listed, `post.author` already a foreign key to `user`.
	fn listed_with_user() -> MemoryCatalog {
		let tables = vec![
			TableSchema::new("post", columns(&["id", "author", "editor"]))
				.with_schema("public")
				.with_foreign_keys(vec![ForeignKeyInfo {
					column: "author".to_owned(),
					target_table: "user".to_owned(),
					on_delete: ReferentialAction::NoAction,
				}]),
			TableSchema::new("user", columns(&["id"])).with_schema("public"),
		];
		MemoryCatalog::new().with_listed_tables(tables)
	}

	#[test]
	fn add_foreign_key_to_a_table_that_is_not_listed_is_undefined() {
		let err = lower(
			&listed_with_user(),
			"ALTER TABLE post ADD FOREIGN KEY (editor) REFERENCES missing(id)",
		)
		.unwrap_err();
		assert_eq!(err.pg_sqlstate(), "42P01", "{err}");
	}

	#[test]
	fn add_foreign_key_to_a_column_that_has_one_is_refused() {
		let err = lower(
			&listed_with_user(),
			"ALTER TABLE post ADD FOREIGN KEY (author) REFERENCES \"user\"(id)",
		)
		.unwrap_err();
		assert_eq!(err.pg_sqlstate(), "0A000", "{err}");
		assert!(err.to_string().contains("already has a foreign key"), "{err}");
	}

	#[test]
	fn a_column_added_by_the_statement_can_take_its_foreign_key() {
		let sql = lower(
			&MemoryCatalog::new(),
			"ALTER TABLE child ADD COLUMN parent_id INT, \
			 ADD FOREIGN KEY (parent_id) REFERENCES parent(id)",
		)
		.unwrap();
		assert!(sql.len() >= 2, "{sql:#?}");
	}

	#[test]
	fn create_table_foreign_keys_must_name_a_listed_table() {
		let catalog = listed_with_user();
		let err = lower(
			&catalog,
			"CREATE TABLE note (id INT PRIMARY KEY, author INT REFERENCES missing(id))",
		)
		.unwrap_err();
		assert_eq!(err.pg_sqlstate(), "42P01", "{err}");
		for sql in [
			"CREATE TABLE note (id INT PRIMARY KEY, author INT REFERENCES \"user\"(id))",
			"CREATE TABLE node (id INT PRIMARY KEY, parent INT REFERENCES node(id))",
		] {
			assert!(lower(&catalog, sql).is_ok(), "{sql}");
		}
		// A catalog that lists no table accepts every target.
		assert!(
			lower(
				&MemoryCatalog::new(),
				"CREATE TABLE note (id INT PRIMARY KEY, author INT REFERENCES missing(id))"
			)
			.is_ok()
		);
	}

	#[test]
	fn add_column_foreign_keys_must_name_a_listed_table() {
		let err = lower(
			&listed_with_user(),
			"ALTER TABLE post ADD COLUMN reviewer INT REFERENCES missing(id)",
		)
		.unwrap_err();
		assert_eq!(err.pg_sqlstate(), "42P01", "{err}");
		assert!(
			lower(
				&listed_with_user(),
				"ALTER TABLE post ADD COLUMN reviewer INT REFERENCES \"user\"(id)"
			)
			.is_ok()
		);
	}

	#[test]
	fn a_default_set_on_a_link_column_names_the_row_it_links_to() {
		let catalog = MemoryCatalog::new().with_link("post", "author", "user");
		let sql = lower(&catalog, "ALTER TABLE post ALTER COLUMN author SET DEFAULT 1").unwrap();
		assert_eq!(sql.len(), 1);
		assert!(sql[0].contains("user"), "{sql:?}");
		let plain =
			lower(&MemoryCatalog::new(), "ALTER TABLE post ALTER COLUMN n SET DEFAULT 1").unwrap();
		assert!(!plain[0].contains("user"), "{plain:?}");
	}

	#[test]
	fn the_listing_of_an_existing_table_takes_the_constraints_the_request_adds() {
		let base = catalog();
		let mut staged = StagedCatalog::new(&base);
		let alter = surrealdb_pgsql::parse_statement(
			"ALTER TABLE plain ADD COLUMN owner INT REFERENCES post(id)",
		)
		.unwrap();
		staged.stage(&lower_pgsql_ddl_with(&alter, &staged).unwrap());
		let plain = staged.all_tables().into_iter().find(|t| t.name == "plain").unwrap();
		assert_eq!(plain.foreign_keys.len(), 1, "{plain:?}");
		assert_eq!(plain.foreign_keys[0].column, "owner");
		// What the catalog lists for another table is unchanged.
		let post = staged.all_tables().into_iter().find(|t| t.name == "post").unwrap();
		assert_eq!(post.foreign_keys.len(), 1);
	}

	#[test]
	fn an_existing_column_altered_in_a_request_keeps_its_declared_type() {
		let declared = crate::declared_type::DeclaredSqlType::parse(
			surrealdb_expr::expr::statements::define::SqlTypeDialect::PostgreSql,
			"char(5)",
		)
		.unwrap();
		let mut base = shaped("fixed", "code", nullable(Kind::String), ColumnAssert::None)
			.with_column_type("fixed", "code", crate::SqlType::Bpchar);
		base.add_table(TableSchema::new(
			"fixed",
			vec![
				ColumnInfo::new("id", 0),
				ColumnInfo::new("code", 1).with_declared(declared.clone()),
			],
		));
		let mut staged = StagedCatalog::new(&base);
		let alter =
			surrealdb_pgsql::parse_statement("ALTER TABLE fixed ALTER COLUMN code SET NOT NULL")
				.unwrap();
		staged.stage(&lower_pgsql_ddl_with(&alter, &staged).unwrap());
		assert_eq!(staged.column_sql_type(None, "fixed", "code"), Some(declared.clone()));
		assert_eq!(staged.column_type(None, "fixed", "code"), crate::SqlType::Bpchar);
		let fixed = staged.all_tables().into_iter().find(|t| t.name == "fixed").unwrap();
		assert_eq!(fixed.columns[1].declared, Some(declared));
		assert!(!fixed.columns[1].is_nullable);
	}

	#[test]
	fn a_column_dropped_in_a_request_no_longer_names_its_domain() {
		let domain = crate::catalog::DomainType {
			name: "pos".to_owned(),
			id: 1,
			base: Kind::I32,
			base_type: "integer".to_owned(),
			not_null: false,
			default: None,
			checks: Vec::new(),
		};
		let mut base =
			shaped("held", "x", nullable(Kind::Named("pos".to_owned())), ColumnAssert::None)
				.with_domains(vec![domain]);
		base.add_table(TableSchema::new(
			"held",
			vec![ColumnInfo::new("id", 0), ColumnInfo::new("x", 1).with_domain("pos")],
		));
		let stage = |sqls: &[&str]| {
			let mut staged = StagedCatalog::new(&base);
			for sql in sqls {
				let stmt = surrealdb_pgsql::parse_statement(sql).unwrap();
				staged.stage(&lower_pgsql_ddl_with(&stmt, &staged).unwrap());
			}
			staged.domain_type("pos").is_some()
		};
		// A column that names the domain keeps it: the removal is refused when it runs.
		assert!(stage(&["DROP DOMAIN pos"]));
		// Once the column is dropped, nothing names the domain, and the removal takes it.
		assert!(!stage(&["ALTER TABLE held DROP COLUMN x", "DROP DOMAIN pos"]));
		let mut staged = StagedCatalog::new(&base);
		let drop = surrealdb_pgsql::parse_statement("ALTER TABLE held DROP COLUMN x").unwrap();
		staged.stage(&lower_pgsql_ddl_with(&drop, &staged).unwrap());
		assert!(staged.column_shape(None, "held", "x").is_none());
		assert_eq!(staged.column_type(None, "held", "x"), crate::SqlType::Unknown);
		assert_eq!(staged.column_sql_type(None, "held", "x"), None);
	}

	#[test]
	fn the_columns_of_an_existing_table_follow_the_request_that_adds_and_drops_them() {
		let base = catalog();
		let mut staged = StagedCatalog::new(&base);
		for sql in ["ALTER TABLE plain ADD COLUMN extra TEXT", "ALTER TABLE plain DROP COLUMN n"] {
			let stmt = surrealdb_pgsql::parse_statement(sql).unwrap();
			staged.stage(&lower_pgsql_ddl_with(&stmt, &staged).unwrap());
		}
		let plain = staged.all_tables().into_iter().find(|t| t.name == "plain").unwrap();
		assert_eq!(plain.column_names(), ["id", "extra"]);
		assert_eq!(plain.columns[1].ordinal, 1);
		assert_eq!(plain.columns[1].data_type.as_deref(), Some("text"));
	}

	#[test]
	fn dropping_not_null_widens_a_scalar_column_that_refused_null() {
		let catalog = shaped("t", "n", Kind::Int, ColumnAssert::None);
		let sql = lower(&catalog, "ALTER TABLE t ALTER COLUMN n DROP NOT NULL").unwrap();
		assert_eq!(sql.len(), 1);
		assert!(sql[0].contains("TYPE none | null | int"), "{sql:?}");
		let optional = shaped("t", "n", nullable(Kind::Int), ColumnAssert::NotNull);
		let sql = lower(&optional, "ALTER TABLE t ALTER COLUMN n DROP NOT NULL").unwrap();
		assert!(!sql[0].contains("TYPE"), "{sql:?}");
	}

	#[test]
	fn a_foreign_key_that_would_not_name_the_id_or_would_repeat_is_refused() {
		let others = MemoryCatalog::new().with_listed_tables(vec![
			TableSchema::new("parent", columns(&["id", "code"]))
				.with_schema("public")
				.with_indexes(vec![index("parent_code_pk", &["code"])]),
			TableSchema::new("child", columns(&["id", "p"])).with_schema("public"),
		]);
		for sql in [
			"ALTER TABLE child ADD FOREIGN KEY (p) REFERENCES parent",
			"ALTER TABLE child ADD FOREIGN KEY (p) REFERENCES parent(id) ON DELETE SET DEFAULT",
		] {
			let err = lower(&others, sql).unwrap_err();
			assert_eq!(err.pg_sqlstate(), "0A000", "{sql}: {err}");
		}
		let stmt = surrealdb_pgsql::parse_statement(
			"CREATE TABLE t (id INT PRIMARY KEY, x INT REFERENCES a(id) REFERENCES b(id))",
		)
		.unwrap();
		let err = lower_pgsql_ddl_definitions_with(&stmt, &MemoryCatalog::new()).unwrap_err();
		assert_eq!(err.pg_sqlstate(), "0A000", "{err}");
	}

	#[test]
	fn a_self_reference_to_a_table_with_another_primary_key_is_refused() {
		let stmt = surrealdb_pgsql::parse_statement(
			"CREATE TABLE node (code INT PRIMARY KEY, parent INT REFERENCES node)",
		)
		.unwrap();
		let err = lower_pgsql_ddl_definitions_with(&stmt, &listed_with_user()).unwrap_err();
		assert_eq!(err.pg_sqlstate(), "0A000", "{err}");
	}

	#[test]
	fn conditional_creations_stage_what_the_catalog_proves_absent() {
		let base = listed_with_user();
		let mut staged = StagedCatalog::new(&base);
		for sql in [
			"CREATE TABLE IF NOT EXISTS fresh (id INT PRIMARY KEY)",
			"CREATE TABLE IF NOT EXISTS post (id INT PRIMARY KEY, other TEXT)",
		] {
			let stmt = surrealdb_pgsql::parse_statement(sql).unwrap();
			staged.stage(&lower_pgsql_ddl_with(&stmt, &staged).unwrap());
		}
		let names: Vec<String> = staged.all_tables().into_iter().map(|t| t.name).collect();
		assert!(names.contains(&"fresh".to_owned()), "{names:?}");
		let post = staged.all_tables().into_iter().find(|t| t.name == "post").unwrap();
		// The existing table keeps its columns; a column the conditional definition adds joins
		// them.
		assert_eq!(post.column_names()[..3], ["id", "author", "editor"]);
	}

	#[test]
	fn an_altered_foreign_key_reaches_the_listing_of_the_next_statement() {
		let base = listed_with_user();
		let mut staged = StagedCatalog::new(&base);
		let drop =
			surrealdb_pgsql::parse_statement("ALTER TABLE post DROP CONSTRAINT post_author_fkey")
				.unwrap();
		staged.stage(&lower_pgsql_ddl_with(&drop, &staged).unwrap());
		let post = staged.all_tables().into_iter().find(|t| t.name == "post").unwrap();
		assert!(post.foreign_keys.is_empty(), "{:?}", post.foreign_keys);
	}

	#[test]
	fn the_listing_of_a_table_the_request_creates_carries_its_constraints() {
		let create = surrealdb_pgsql::parse_statement(
			"CREATE TABLE fresh (id serial PRIMARY KEY, email TEXT UNIQUE, author INT REFERENCES \"user\"(id) ON DELETE CASCADE)",
		)
		.unwrap();
		let base = listed_with_user();
		let mut staged = StagedCatalog::new(&base);
		staged.stage(&lower_pgsql_ddl_with(&create, &staged).unwrap());
		let fresh = staged.all_tables().into_iter().find(|t| t.name == "fresh").unwrap();
		assert_eq!(
			fresh.foreign_keys,
			[ForeignKeyInfo {
				column: "author".to_owned(),
				target_table: "user".to_owned(),
				on_delete: ReferentialAction::Cascade,
			}]
		);
		assert!(fresh.indexes.iter().any(|index| index.unique && index.columns == ["email"]));
		assert!(fresh.columns[0].sequence.is_some(), "{:?}", fresh.columns);
	}

	#[test]
	fn the_listings_follow_the_tables_the_request_creates_and_removes() {
		let create =
			surrealdb_pgsql::parse_statement("CREATE TABLE fresh (id INT, n INT)").unwrap();
		let drop = surrealdb_pgsql::parse_statement("DROP TABLE plain").unwrap();
		let base = catalog();
		let mut staged = StagedCatalog::new(&base);
		staged.stage(&lower_pgsql_ddl_with(&create, &staged).unwrap());
		staged.stage(&lower_pgsql_ddl_with(&drop, &staged).unwrap());
		let names: Vec<String> = staged.all_tables().into_iter().map(|t| t.name).collect();
		assert_eq!(names, ["post", "fresh"]);
		let fresh = staged.all_tables().into_iter().find(|t| t.name == "fresh").unwrap();
		assert_eq!(fresh.column_names(), ["id", "n"]);
	}

	#[test]
	fn an_alteration_of_the_assertion_keeps_the_rest_of_the_column_for_the_next_statement() {
		let base = shaped("post", "author", Kind::Int, ColumnAssert::None);
		let mut staged = StagedCatalog::new(&base);
		let alter =
			surrealdb_pgsql::parse_statement("ALTER TABLE post ALTER COLUMN author SET NOT NULL")
				.unwrap();
		staged.stage(&lower_pgsql_ddl_with(&alter, &staged).unwrap());
		let shape = staged.column_shape(None, "post", "author").unwrap();
		assert_eq!(shape.assert, ColumnAssert::NotNull);
		assert_eq!(shape.kind, Some(Kind::Int));
	}
}
#[test]
fn test_pgsql_time_zone_suffix_names_the_type() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE TABLE t (a TIMESTAMP(3) WITH TIME ZONE, b TIME WITHOUT TIME ZONE, c TIMESTAMP WITH TIME ZONE[])",
	)
	.expect("parse pgsql");
	let surrealdb_pgsql::ast::stmt::Statement::CreateTable(ct) = stmt else {
		panic!("Expected CreateTable");
	};
	assert_eq!(map_pgsql_type(&ct.columns[0].data_type), Kind::Datetime);
	assert_eq!(map_pgsql_type(&ct.columns[1].data_type), Kind::Time);
	assert_eq!(
		map_pgsql_type(&ct.columns[2].data_type),
		Kind::Array(Box::new(Kind::Datetime), None)
	);
	assert!(
		surrealdb_pgsql::parse_statement("CREATE TABLE t (a TIMESTAMP WITH TIMEZONE)").is_err()
	);
}

#[test]
fn test_pgsql_temporal_type_map() {
	let kind_of = |ty: &str| -> Kind {
		let stmt = surrealdb_pgsql::parse_statement(&format!("CREATE TABLE t (a {ty})"))
			.expect("parse pgsql");
		let surrealdb_pgsql::ast::stmt::Statement::CreateTable(ct) = stmt else {
			panic!("Expected CreateTable");
		};
		map_pgsql_type(&ct.columns[0].data_type)
	};
	for (ty, want) in [
		("date", Kind::Date),
		("time", Kind::Time),
		("time without time zone", Kind::Time),
		("timetz", Kind::Time),
		("time with time zone", Kind::Time),
		("timestamp", Kind::Timestamp),
		("timestamp without time zone", Kind::Timestamp),
		("timestamp(3)", Kind::Timestamp),
		("timestamptz", Kind::Datetime),
		("timestamp with time zone", Kind::Datetime),
		("interval", Kind::Interval),
		("interval[]", Kind::Array(Box::new(Kind::Interval), None)),
	] {
		assert_eq!(kind_of(ty), want, "{ty}");
	}
}

#[test]
fn test_pgsql_sized_type_map() {
	let kind_of = |ty: &str| -> Kind {
		let stmt = surrealdb_pgsql::parse_statement(&format!("CREATE TABLE t (a {ty})"))
			.expect("parse pgsql");
		let surrealdb_pgsql::ast::stmt::Statement::CreateTable(ct) = stmt else {
			panic!("Expected CreateTable");
		};
		map_pgsql_type(&ct.columns[0].data_type)
	};
	for (ty, want) in [
		("smallint", Kind::I16),
		("int2", Kind::I16),
		("smallserial", Kind::I16),
		("serial2", Kind::I16),
		("integer", Kind::I32),
		("int", Kind::I32),
		("int4", Kind::I32),
		("serial", Kind::I32),
		("serial4", Kind::I32),
		("bigint", Kind::Int),
		("int8", Kind::Int),
		("bigserial", Kind::Int),
		("serial8", Kind::Int),
		("real", Kind::F32),
		("float4", Kind::F32),
		("float8", Kind::Float),
		("double precision", Kind::Float),
		("float", Kind::Float),
		("float(24)", Kind::F32),
		("float(53)", Kind::Float),
		("integer[]", Kind::Array(Box::new(Kind::I32), None)),
		("real[]", Kind::Array(Box::new(Kind::F32), None)),
	] {
		assert_eq!(kind_of(ty), want, "{ty}");
	}
}

#[test]
fn test_pgsql_multidimensional_array_map() {
	let kind_of = |ty: &str| -> Kind {
		let stmt = surrealdb_pgsql::parse_statement(&format!("CREATE TABLE t (a {ty})"))
			.expect("parse pgsql");
		let surrealdb_pgsql::ast::stmt::Statement::CreateTable(ct) = stmt else {
			panic!("Expected CreateTable");
		};
		map_pgsql_type(&ct.columns[0].data_type)
	};
	let array = |kind: Kind| Kind::Array(Box::new(kind), None);
	// One array level per dimension, and a bound in the brackets does not change the kind.
	assert_eq!(kind_of("int[][]"), array(array(Kind::I32)));
	assert_eq!(kind_of("int[3][]"), array(array(Kind::I32)));
	assert_eq!(kind_of("int[2][3][]"), array(array(array(Kind::I32))));
	assert_eq!(kind_of("text[][]"), array(array(Kind::String)));
	assert_eq!(kind_of("int[]"), array(Kind::I32));
	assert_eq!(kind_of("int[3]"), array(Kind::I32));
	assert_eq!(kind_of("int"), Kind::I32);
}

#[test]
fn a_column_with_two_foreign_keys_is_refused() {
	for sql in [
		"CREATE TABLE t (id INT PRIMARY KEY, x INT REFERENCES a(id), FOREIGN KEY (x) REFERENCES b(id))",
		"CREATE TABLE t (id INT PRIMARY KEY, x INT, FOREIGN KEY (x) REFERENCES a(id), FOREIGN KEY (x) REFERENCES b(id))",
	] {
		let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
		let err = lower_pgsql_ddl_definitions(&stmt).unwrap_err();
		assert_eq!(err.pg_sqlstate(), "0A000", "{sql}: {err}");
	}
}

#[test]
fn lowers_enum_type_statements() {
	use surrealdb_expr::expr::statements::AlterStatement;
	use surrealdb_expr::expr::statements::alter::{AlterTypeAction, EnumPosition};
	use surrealdb_expr::expr::statements::define::union_labels;

	let create =
		surrealdb_pgsql::parse_statement("CREATE TYPE mood AS ENUM ('sad', 'ok')").expect("parse");
	let exprs = lower_pgsql_ddl_definitions(&create).expect("lower create type");
	let define = defined_type(&exprs);
	assert_eq!(define.name.as_str(), "mood");
	assert_eq!(union_labels(&define.body), Some(vec!["sad".to_owned(), "ok".to_owned()]));

	let alter = surrealdb_pgsql::parse_statement("ALTER TYPE mood ADD VALUE 'meh' BEFORE 'ok'")
		.expect("parse");
	let exprs = lower_pgsql_ddl_definitions(&alter).expect("lower alter type");
	let [Expr::Alter(alter)] = exprs.as_slice() else {
		panic!("expected one alteration, got {exprs:?}");
	};
	let AlterStatement::Type(alter) = &**alter else {
		panic!("expected ALTER TYPE");
	};
	assert_eq!(
		alter.actions,
		[AlterTypeAction::AddValue {
			value: "meh".to_owned(),
			position: Some(EnumPosition::Before("ok".to_owned())),
			if_not_exists: false,
		}]
	);

	let drop = surrealdb_pgsql::parse_statement("DROP TYPE IF EXISTS mood").expect("parse");
	let exprs = lower_pgsql_ddl_definitions(&drop).expect("lower drop type");
	assert!(matches!(
		exprs.as_slice(),
		[Expr::Remove(r)] if matches!(&**r, RemoveStatement::Type(e) if e.if_exists)
	));
}

/// The type a lowering defines, from its only definition.
fn defined_type(exprs: &[Expr]) -> &surrealdb_expr::expr::statements::define::DefineTypeStatement {
	exprs
		.iter()
		.find_map(|expr| match expr {
			Expr::Define(define) => match &**define {
				DefineStatement::Type(ty) => Some(ty),
				_ => None,
			},
			_ => None,
		})
		.unwrap_or_else(|| panic!("no type defined in {exprs:?}"))
}

#[test]
fn lowers_create_domain_to_a_type_definition() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE DOMAIN posint AS integer NOT NULL DEFAULT 1 CHECK (value > 0) CONSTRAINT small CHECK (VALUE < 100) CHECK (value <> 7)",
	)
	.expect("parse");
	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	let ty = defined_type(&exprs);
	assert_eq!(ty.name.as_str(), "posint");
	assert_eq!(ty.kind, DefineKind::Default);
	assert_eq!(ty.body, Kind::I32);
	assert_eq!(ty.default, Some(Expr::Literal(surrealdb_expr::expr::Literal::Integer(1))));
	assert_eq!(ty.sql_type.as_deref(), Some("integer"));
	let names: Vec<_> = ty.asserts.iter().map(|c| c.name.as_deref()).collect();
	// A check declared with no name takes PostgreSQL's `<domain>_check`, numbered once taken.
	assert_eq!(names, [Some("posint_check"), Some("small"), Some("posint_check1")]);
	assert!(ty.asserts[1].check.to_sql().contains("$value < 100"));

	// A domain that is not NOT NULL admits NONE and NULL.
	let stmt = surrealdb_pgsql::parse_statement("CREATE DOMAIN maybe AS text").expect("parse");
	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	assert_eq!(defined_type(&exprs).body, Kind::either(vec![Kind::None, Kind::Null, Kind::String]));
}

#[test]
fn a_domain_constant_default_is_stored_as_the_value_its_base_type_is_assigned() {
	for (sql, default) in [
		("CREATE DOMAIN d AS text DEFAULT 1", "'1'"),
		("CREATE DOMAIN d AS text DEFAULT true", "'true'"),
		("CREATE DOMAIN d AS integer DEFAULT 2.5", "3"),
		("CREATE DOMAIN d AS integer DEFAULT -2.5", "-3"),
		("CREATE DOMAIN d AS smallint DEFAULT '7'", "7"),
		("CREATE DOMAIN d AS boolean DEFAULT 'yes'", "true"),
		("CREATE DOMAIN d AS numeric(5,2) DEFAULT -1.235", "-1.24dec"),
		("CREATE DOMAIN d AS double precision DEFAULT 1", "1f"),
	] {
		let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
		let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
		let lowered = defined_type(&exprs).default.as_ref().map(ToSql::to_sql);
		assert_eq!(lowered.as_deref(), Some(default), "{sql}");
	}
	for (sql, code, message) in [
		("CREATE DOMAIN d AS smallint DEFAULT 100000", "22003", "smallint out of range"),
		("CREATE DOMAIN d AS integer DEFAULT -3000000000", "22003", "integer out of range"),
		(
			"CREATE DOMAIN d AS smallint DEFAULT '100000'",
			"22003",
			"value \"100000\" is out of range for type smallint",
		),
		("CREATE DOMAIN d AS numeric(3,1) DEFAULT 100", "22003", "numeric field overflow"),
		("CREATE DOMAIN d AS real DEFAULT 1e100", "22003", "value out of range: overflow"),
		(
			"CREATE DOMAIN d AS boolean DEFAULT -1",
			"42804",
			"column \"d\" is of type boolean but default expression is of type integer",
		),
	] {
		let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
		let err = lower_pgsql_ddl_definitions(&stmt).unwrap_err();
		assert_eq!((err.pg_sqlstate(), err.to_string().as_str()), (code, message), "{sql}");
	}
}

#[test]
fn a_domain_expression_default_of_another_type_family_is_refused() {
	for (sql, found) in [
		("CREATE DOMAIN d AS integer DEFAULT now()", "timestamp with time zone"),
		("CREATE DOMAIN d AS boolean DEFAULT 1 + 2", "bigint"),
		("CREATE DOMAIN d AS timestamptz DEFAULT 1 + 2", "bigint"),
	] {
		let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
		let err = lower_pgsql_ddl_definitions(&stmt).unwrap_err();
		assert_eq!(err.pg_sqlstate(), "42804", "{sql}");
		assert!(
			err.to_string().ends_with(&format!("default expression is of type {found}")),
			"{sql}: {err}"
		);
	}
	for sql in [
		"CREATE DOMAIN d AS timestamptz DEFAULT now()",
		"CREATE DOMAIN d AS text DEFAULT now()",
		"CREATE DOMAIN d AS numeric DEFAULT 1 + 2",
		"CREATE DOMAIN d AS date DEFAULT now()",
	] {
		let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
		lower_pgsql_ddl_definitions(&stmt).expect(sql);
	}
}

#[test]
fn refuses_a_domain_over_a_user_type_and_a_composite_attribute_of_a_domain() {
	let catalog = crate::catalog::MemoryCatalog::new().with_domains(vec![posint_domain()]);
	for (sql, code) in [
		("CREATE DOMAIN d AS posint", "0A000"),
		("CREATE TYPE pair AS (a posint, b text)", "0A000"),
		("CREATE TYPE pair AS (a int, a text)", "42701"),
		("DROP DOMAIN posint CASCADE", "0A000"),
	] {
		let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
		let err = super::ddl::lower_pgsql_ddl_definitions_with(&stmt, &catalog).unwrap_err();
		assert_eq!(err.pg_sqlstate(), code, "{sql}: {err}");
	}
}

#[test]
fn lowers_create_type_of_a_composite_to_an_object_of_nullable_attributes() {
	use std::collections::BTreeMap;

	use surrealdb_expr::expr::KindLiteral;
	use surrealdb_strand::Strand;

	let stmt = surrealdb_pgsql::parse_statement("CREATE TYPE address AS (street text, zip int)")
		.expect("parse");
	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	let ty = defined_type(&exprs);
	assert_eq!(
		ty.body,
		Kind::Literal(KindLiteral::Object(BTreeMap::from([
			(Strand::from("street"), Kind::either(vec![Kind::None, Kind::Null, Kind::String])),
			(Strand::from("zip"), Kind::either(vec![Kind::None, Kind::Null, Kind::I32])),
		])))
	);
	// The declared types keep the attributes' order, which an object does not.
	let sql_type = ty.sql_type.as_deref().expect("sql type");
	assert!(sql_type.starts_with("(street "), "{sql_type}");
}

#[test]
fn alter_domain_redefines_the_type() {
	let lower = |sql: &str| {
		let catalog = crate::catalog::MemoryCatalog::new().with_domains(vec![posint_domain()]);
		let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
		super::ddl::lower_pgsql_ddl_definitions_with(&stmt, &catalog)
	};

	// A new check overwrites the definition, and no column is restated.
	let exprs =
		lower("ALTER DOMAIN posint ADD CONSTRAINT not_seven CHECK (VALUE <> 7)").expect("lower");
	assert_eq!(exprs.len(), 1, "{exprs:?}");
	let ty = defined_type(&exprs);
	assert_eq!(ty.kind, DefineKind::Overwrite);
	assert_eq!(ty.asserts.len(), 2);

	let exprs = lower("ALTER DOMAIN posint SET DEFAULT 3").expect("lower");
	assert_eq!(
		defined_type(&exprs).default,
		Some(Expr::Literal(surrealdb_expr::expr::Literal::Integer(3)))
	);

	let exprs = lower("ALTER DOMAIN posint DROP NOT NULL").expect("lower");
	assert_eq!(defined_type(&exprs).body, Kind::either(vec![Kind::None, Kind::Null, Kind::I32]));

	// Errors are PostgreSQL's.
	for (sql, code) in [
		("ALTER DOMAIN nope SET NOT NULL", "42704"),
		("ALTER DOMAIN posint DROP CONSTRAINT nope", "42704"),
		("ALTER DOMAIN posint ADD CONSTRAINT positive CHECK (VALUE > 1)", "42710"),
	] {
		let err = lower(sql).unwrap_err();
		assert_eq!(err.pg_sqlstate(), code, "{sql}: {err}");
	}
	assert!(lower("ALTER DOMAIN posint DROP CONSTRAINT IF EXISTS nope").expect("lower").is_empty());
}

#[test]
fn drop_domain_removes_the_type() {
	let stmt = surrealdb_pgsql::parse_statement("DROP DOMAIN IF EXISTS posint").expect("parse");
	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	assert!(matches!(
		exprs.as_slice(),
		[Expr::Remove(r)] if matches!(&**r, RemoveStatement::Type(t)
			if t.if_exists && t.name.as_str() == "posint" && t.object == Some(SqlTypeObject::Domain))
	));
}

#[test]
fn a_column_of_an_enum_type_names_the_type() {
	let catalog = crate::MemoryCatalog::new().with_enums(vec![crate::EnumType::declared(
		"mood",
		0,
		&["sad".to_owned(), "ok".to_owned()],
	)]);
	let create = surrealdb_pgsql::parse_statement("CREATE TABLE p (id int, m mood NOT NULL)")
		.expect("parse");
	let exprs = super::ddl::lower_pgsql_ddl_definitions_with(&create, &catalog).expect("lower");
	assert_eq!(field_named(&exprs, "m").field_kind, Some(Kind::Named("mood".to_owned())));
}

#[test]
fn a_column_of_a_multidimensional_enum_array_holds_the_enum_at_every_level() {
	let catalog = crate::MemoryCatalog::new().with_enums(vec![crate::EnumType::declared(
		"mood",
		0,
		&["sad".to_owned(), "ok".to_owned()],
	)]);
	let create =
		surrealdb_pgsql::parse_statement("CREATE TABLE p (id int, m mood[][])").expect("parse");
	let exprs = super::ddl::lower_pgsql_ddl_definitions_with(&create, &catalog).expect("lower");
	let kind = field_named(&exprs, "m").field_kind.clone();
	let element = Kind::Named("mood".to_owned());
	let nested = Kind::Array(Box::new(Kind::Array(Box::new(element), None)), None);
	let Some(Kind::Either(members)) = kind else {
		panic!("expected an optional kind, found {kind:?}");
	};
	assert!(members.contains(&nested), "{members:?}");
}

#[test]
fn mysql_enum_and_set_columns_keep_their_members() {
	let labels = vec!["a".to_owned(), "it's".to_owned()];
	assert_eq!(map_mysql_type("enum('a', 'it''s')"), Kind::Enum(labels.clone()));
	assert_eq!(map_mysql_type("set('a', 'it''s')"), Kind::Set(Box::new(Kind::Enum(labels)), None));
	let members = (0..65).map(|i| format!("'m{i}'")).collect::<Vec<_>>().join(", ");
	assert!(super::ddl::check_mysql_set_limit(&format!("set({members})")).is_err());
	assert!(super::ddl::check_mysql_set_limit("set('a', 'b')").is_ok());
}

/// The field definition `name` in `exprs`, the statements a lowering runs.
fn field_named<'a>(
	exprs: &'a [Expr],
	name: &str,
) -> &'a surrealdb_expr::expr::statements::define::DefineFieldStatement {
	exprs
		.iter()
		.find_map(|expr| match expr {
			Expr::Define(define) => match &**define {
				DefineStatement::Field(field) if field.name.to_sql() == name => Some(&**field),
				_ => None,
			},
			_ => None,
		})
		.unwrap_or_else(|| panic!("no field {name} in {exprs:?}"))
}

/// The domain `posint`: an integer that refuses NULL, defaults to 1, and must be positive.
fn posint_domain() -> crate::catalog::DomainType {
	crate::catalog::DomainType {
		name: "posint".to_owned(),
		id: 1,
		base: Kind::I32,
		base_type: "integer".to_owned(),
		not_null: true,
		default: Some(Expr::Literal(surrealdb_expr::expr::Literal::Integer(1))),
		checks: vec![crate::catalog::DomainCheck {
			name: Some("positive".to_owned()),
			check: super::ddl::lower_pgsql_domain_check(
				&surrealdb_pgsql::parse_expr("VALUE > 0").expect("parse"),
			)
			.expect("lower"),
		}],
	}
}

#[test]
fn lowers_a_domain_check_over_the_bound_value() {
	let check = super::ddl::lower_pgsql_domain_check(
		&surrealdb_pgsql::parse_expr("value > 0").expect("parse"),
	)
	.expect("lower");
	assert!(check.to_sql().contains("$value > 0"), "{}", check.to_sql());
}

#[test]
fn a_domain_column_names_the_domain() {
	let catalog = crate::catalog::MemoryCatalog::new().with_domains(vec![posint_domain()]);
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE TABLE t (id INT PRIMARY KEY, n posint, m posint DEFAULT 5)",
	)
	.expect("parse");
	let exprs = super::ddl::lower_pgsql_ddl_definitions_with(&stmt, &catalog).expect("lower");

	// The type holds the domain's constraints and default, so the column states none of them.
	let n = field_named(&exprs, "n");
	assert_eq!(n.field_kind, Some(Kind::Named("posint".to_owned())));
	assert_eq!(n.assert, None);
	assert_eq!(n.default, DefineDefault::None);

	// A column's own DEFAULT is its own.
	let m = field_named(&exprs, "m");
	assert_eq!(
		m.default,
		DefineDefault::Set(Expr::Literal(surrealdb_expr::expr::Literal::Integer(5)))
	);

	// ALTER TABLE ADD COLUMN names the domain too, and a column's own NOT NULL is its assertion.
	let stmt = surrealdb_pgsql::parse_statement("ALTER TABLE t ADD COLUMN k posint NOT NULL")
		.expect("parse");
	let exprs = super::ddl::lower_pgsql_ddl_definitions_with(&stmt, &catalog).expect("lower");
	let k = field_named(&exprs, "k");
	assert_eq!(k.field_kind, Some(Kind::Named("posint".to_owned())));
}

#[test]
fn a_composite_column_names_the_composite_type() {
	let catalog =
		crate::catalog::MemoryCatalog::new().with_composites(vec![crate::catalog::CompositeType {
			name: "address".to_owned(),
			id: 1,
			fields: vec![("street".to_owned(), Kind::String), ("zip".to_owned(), Kind::I32)],
			declared: Vec::new(),
		}]);
	let stmt =
		surrealdb_pgsql::parse_statement("CREATE TABLE t (id INT PRIMARY KEY, a address NOT NULL)")
			.expect("parse");
	let exprs = super::ddl::lower_pgsql_ddl_definitions_with(&stmt, &catalog).expect("lower");
	assert_eq!(field_named(&exprs, "a").field_kind, Some(Kind::Named("address".to_owned())));
}

#[test]
fn refuses_domain_arrays_and_foreign_keys() {
	let catalog = crate::catalog::MemoryCatalog::new().with_domains(vec![posint_domain()]);
	for sql in [
		"CREATE TABLE t (id INT PRIMARY KEY, n posint[])",
		"CREATE TABLE t (id INT PRIMARY KEY, n posint REFERENCES u(id))",
		"ALTER TABLE t ADD COLUMN n posint[]",
	] {
		let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
		let err = super::ddl::lower_pgsql_ddl_definitions_with(&stmt, &catalog).unwrap_err();
		assert_eq!(err.pg_sqlstate(), "0A000", "{sql}: {err}");
	}
}

#[test]
fn drop_type_removes_the_type_of_any_kind() {
	let catalog =
		crate::catalog::MemoryCatalog::new().with_composites(vec![crate::catalog::CompositeType {
			name: "address".to_owned(),
			id: 1,
			fields: vec![("street".to_owned(), Kind::String)],
			declared: Vec::new(),
		}]);
	for sql in ["DROP TYPE address", "DROP TYPE mood"] {
		let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
		let exprs = super::ddl::lower_pgsql_ddl_definitions_with(&stmt, &catalog).expect("lower");
		assert!(
			matches!(exprs.as_slice(), [Expr::Remove(r)] if matches!(&**r, RemoveStatement::Type(t)
				if t.object == Some(SqlTypeObject::Type))),
			"{sql}: {exprs:?}"
		);
	}
}

#[test]
fn test_pgsql_number_shaped_type_map() {
	let kind_of = |type_name: &str| -> Kind {
		let sql = format!("CREATE TABLE t (c {type_name})");
		let stmt = surrealdb_pgsql::parse_statement(&sql).expect("parse pgsql");
		let surrealdb_pgsql::ast::stmt::Statement::CreateTable(ct) = stmt else {
			panic!("expected CREATE TABLE for {type_name}");
		};
		map_pgsql_type(&ct.columns[0].data_type)
	};
	assert_eq!(kind_of("money"), Kind::Decimal);
	assert_eq!(kind_of("numeric"), Kind::BigDecimal);
	assert_eq!(kind_of("numeric(28)"), Kind::Decimal);
	assert_eq!(kind_of("numeric(28,28)"), Kind::Decimal);
	assert_eq!(kind_of("numeric(29,2)"), Kind::BigDecimal);
	assert_eq!(kind_of("decimal(40,10)"), Kind::BigDecimal);
	assert_eq!(kind_of("numeric(40,10)[]"), Kind::Array(Box::new(Kind::BigDecimal), None));
	for name in [
		"oid",
		"regclass",
		"regproc",
		"regprocedure",
		"regoper",
		"regoperator",
		"regtype",
		"regnamespace",
		"regrole",
		"regconfig",
		"regdictionary",
		"regcollation",
		"xid",
		"cid",
	] {
		assert_eq!(kind_of(name), Kind::U32, "{name}");
	}
	assert_eq!(kind_of("xid8"), Kind::U64);
	assert_eq!(kind_of("pg_lsn"), Kind::U64);
}

#[test]
fn test_lower_mysql_serial_column_other_than_the_key_is_u64() {
	let stmt =
		surrealdb_mysql::parse_statement("CREATE TABLE orders (id INT, n SERIAL);").expect("parse");
	let exprs = lower_mysql_ddl_definitions(&stmt).expect("lower");
	let n_field = exprs.iter().find_map(|expr| match expr {
		Expr::Define(d) => match &**d {
			DefineStatement::Field(f) if f.name.to_sql() == "n" => Some(f),
			_ => None,
		},
		_ => None,
	});
	assert_eq!(n_field.expect("a field definition for n").field_kind, Some(Kind::U64));
}

mod drop_schema {
	use surrealdb_expr::expr::Expr;
	use surrealdb_types::ToSql;

	use super::super::ddl::{lower_pgsql_ddl_definitions_with, lower_pgsql_ddl_with};
	use crate::{BinderCatalog, ColumnInfo, MemoryCatalog, PlanError, StagedCatalog, TableSchema};

	/// A database holding `post` and `plain`, listed out of name order.
	fn catalog() -> MemoryCatalog {
		let table = |name: &str| {
			TableSchema::new(name, vec![ColumnInfo::new("id", 0)]).with_schema("public")
		};
		MemoryCatalog::new().with_listed_tables(vec![table("post"), table("plain")])
	}

	/// A catalog that lists no table and leaves its refusals to the schema, as the recorder the
	/// datastore lowers with first does.
	struct Deferring;

	impl BinderCatalog for Deferring {
		fn get_table(&self, _schema: Option<&str>, _name: &str) -> Option<TableSchema> {
			None
		}

		fn defers_schema_checks(&self) -> bool {
			true
		}
	}

	fn lower(catalog: &dyn BinderCatalog, sql: &str) -> Result<Vec<String>, PlanError> {
		let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
		lower_pgsql_ddl_definitions_with(&stmt, catalog)
			.map(|exprs| exprs.iter().map(Expr::to_sql).collect())
	}

	#[test]
	fn cascade_removes_every_table_in_name_order() {
		for sql in ["DROP SCHEMA public CASCADE", "DROP SCHEMA IF EXISTS public CASCADE"] {
			assert_eq!(
				lower(&catalog(), sql).unwrap(),
				["REMOVE TABLE IF EXISTS plain", "REMOVE TABLE IF EXISTS post"],
				"{sql}"
			);
		}
	}

	#[test]
	fn a_view_is_removed_before_the_tables_it_reads() {
		let table = |name: &str, over: &[&str]| {
			TableSchema::new(name, vec![ColumnInfo::new("id", 0)])
				.with_schema("public")
				.with_view_over(over.iter().map(|source| (*source).to_owned()).collect())
		};
		let catalog = MemoryCatalog::new().with_listed_tables(vec![
			table("a_base", &[]),
			table("m_view", &["a_base"]),
			table("z_view_of_view", &["m_view"]),
			table("b_plain", &[]),
		]);
		assert_eq!(
			lower(&catalog, "DROP SCHEMA public CASCADE").unwrap(),
			[
				"REMOVE TABLE IF EXISTS b_plain",
				"REMOVE TABLE IF EXISTS z_view_of_view",
				"REMOVE TABLE IF EXISTS m_view",
				"REMOVE TABLE IF EXISTS a_base",
			]
		);
	}

	#[test]
	fn a_view_created_in_the_same_request_is_removed_before_its_source() {
		let base = MemoryCatalog::new().with_listed_tables(vec![
			TableSchema::new("a_base", vec![ColumnInfo::new("id", 0)]).with_schema("public"),
		]);
		let mut staged = StagedCatalog::new(&base);
		let create =
			surrealdb_pgsql::parse_statement("CREATE VIEW z_view AS SELECT * FROM a_base").unwrap();
		staged.stage(&lower_pgsql_ddl_with(&create, &staged).unwrap());
		assert_eq!(
			lower(&staged, "DROP SCHEMA public CASCADE").unwrap(),
			["REMOVE TABLE IF EXISTS z_view", "REMOVE TABLE IF EXISTS a_base"]
		);
	}

	#[test]
	fn cascade_removes_the_sequences_the_tables_draw_from() {
		let table = |name: &str, sequence: Option<&str>| {
			let mut id = ColumnInfo::new("id", 0);
			id.sequence = sequence.map(str::to_owned);
			TableSchema::new(name, vec![id]).with_schema("public")
		};
		let catalog = MemoryCatalog::new().with_listed_tables(vec![
			table("post", Some("post_id_seq")),
			table("plain", None),
			table("author", Some("author_id_seq")),
		]);
		assert_eq!(
			lower(&catalog, "DROP SCHEMA public CASCADE").unwrap(),
			[
				"REMOVE TABLE IF EXISTS author",
				"REMOVE TABLE IF EXISTS plain",
				"REMOVE TABLE IF EXISTS post",
				"REMOVE SEQUENCE IF EXISTS author_id_seq",
				"REMOVE SEQUENCE IF EXISTS post_id_seq",
			]
		);
	}

	#[test]
	fn views_over_many_tables_are_removed_before_their_sources() {
		const PAIRS: usize = 3_000;
		let table = |name: String, over: Vec<String>| {
			TableSchema::new(name, vec![ColumnInfo::new("id", 0)])
				.with_schema("public")
				.with_view_over(over)
		};
		// Sources sort before the views that read them, so a removal that went by name alone would
		// take every source first. Each view also reads the previous source, and a source is listed
		// twice.
		let mut listed = Vec::new();
		for at in 0..PAIRS {
			let mut over = vec![format!("a_{at:05}")];
			if at > 0 {
				over.push(format!("a_{:05}", at - 1));
			}
			listed.push(table(format!("v_{at:05}"), over));
			listed.push(table(format!("a_{at:05}"), Vec::new()));
		}
		listed.push(table("a_00000".to_owned(), Vec::new()));
		let catalog = MemoryCatalog::new().with_listed_tables(listed);
		let removed = lower(&catalog, "DROP SCHEMA public CASCADE").unwrap();
		assert_eq!(removed.len(), 2 * PAIRS);
		let removed_at = |name: &str| {
			removed.iter().position(|sql| sql == &format!("REMOVE TABLE IF EXISTS {name}")).unwrap()
		};
		for at in 0..PAIRS {
			assert!(removed_at(&format!("v_{at:05}")) < removed_at(&format!("a_{at:05}")), "{at}");
			if at > 0 {
				assert!(
					removed_at(&format!("v_{at:05}")) < removed_at(&format!("a_{:05}", at - 1)),
					"{at}"
				);
			}
		}
		// Of the tables nothing reads, the first name goes first, and a source is ready as soon as
		// the last view reading it is gone.
		assert_eq!(
			removed[..3],
			[
				"REMOVE TABLE IF EXISTS v_00000",
				"REMOVE TABLE IF EXISTS v_00001",
				"REMOVE TABLE IF EXISTS a_00000"
			]
		);
	}

	#[test]
	fn a_view_over_a_table_the_database_does_not_list_is_still_removed() {
		let table = |name: &str, over: &[&str]| {
			TableSchema::new(name, vec![ColumnInfo::new("id", 0)])
				.with_schema("public")
				.with_view_over(over.iter().map(|source| (*source).to_owned()).collect())
		};
		let catalog = MemoryCatalog::new()
			.with_listed_tables(vec![table("view", &["elsewhere", "base"]), table("base", &[])]);
		assert_eq!(
			lower(&catalog, "DROP SCHEMA public CASCADE").unwrap(),
			["REMOVE TABLE IF EXISTS view", "REMOVE TABLE IF EXISTS base"]
		);
	}

	#[test]
	fn restrict_is_refused_while_the_database_holds_a_table() {
		let err = lower(&catalog(), "DROP SCHEMA public").unwrap_err();
		assert_eq!(err.pg_sqlstate(), "0A000", "{err}");
		assert!(err.to_string().contains("CASCADE"), "{err}");
		assert!(lower(&MemoryCatalog::new(), "DROP SCHEMA public").unwrap().is_empty());
	}

	#[test]
	fn a_database_without_tables_has_nothing_to_remove() {
		assert!(lower(&MemoryCatalog::new(), "DROP SCHEMA public CASCADE").unwrap().is_empty());
	}

	#[test]
	fn a_name_that_is_not_public_is_no_schema() {
		let err = lower(&catalog(), "DROP SCHEMA nosuch CASCADE").unwrap_err();
		assert_eq!(err.pg_sqlstate(), "42P01", "{err}");
		assert!(lower(&catalog(), "DROP SCHEMA IF EXISTS nosuch CASCADE").unwrap().is_empty());
	}

	#[test]
	fn the_system_schemas_are_refused_even_under_if_exists() {
		for name in ["pg_catalog", "information_schema", "pg_toast"] {
			for sql in [
				format!("DROP SCHEMA {name} CASCADE"),
				format!("DROP SCHEMA IF EXISTS {name} CASCADE"),
			] {
				let err = lower(&catalog(), &sql).unwrap_err();
				assert_eq!(err.pg_sqlstate(), "0A000", "{sql}: {err}");
			}
		}
	}

	#[test]
	fn a_catalog_that_defers_to_the_schema_is_not_refused() {
		assert!(lower(&Deferring, "DROP SCHEMA public").unwrap().is_empty());
		assert!(lower(&Deferring, "DROP SCHEMA public CASCADE").unwrap().is_empty());
	}
}

#[test]
fn a_repeated_enum_label_or_domain_constraint_name_is_refused() {
	let create = surrealdb_pgsql::parse_statement("CREATE TYPE mood AS ENUM ('ok', 'sad', 'ok')")
		.expect("parse");
	let err = lower_pgsql_ddl_definitions(&create).expect_err("a repeated label");
	assert_eq!(err, crate::PlanError::DuplicateEnumLabel("ok".to_owned()));
	assert_eq!(err.pg_sqlstate(), "42710");

	let create = surrealdb_pgsql::parse_statement(
		"CREATE DOMAIN d AS integer CONSTRAINT c CHECK (VALUE > 0) CONSTRAINT c CHECK (VALUE < 9)",
	)
	.expect("parse");
	let err = lower_pgsql_ddl_definitions(&create).expect_err("a repeated constraint name");
	assert_eq!(
		err,
		crate::PlanError::DuplicateDomainConstraint {
			constraint: "c".to_owned(),
			domain: "d".to_owned(),
		}
	);
}

#[test]
fn an_enum_with_no_labels_is_refused() {
	let create = surrealdb_pgsql::parse_statement("CREATE TYPE e AS ENUM ()").expect("parse");
	let err = lower_pgsql_ddl_definitions(&create).expect_err("an empty enum");
	assert_eq!(err.pg_sqlstate(), "0A000", "{err}");
}
