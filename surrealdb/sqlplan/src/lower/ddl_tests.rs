use surrealdb_expr::expr::statements::define::{DefineKind, DefineStatement};
use surrealdb_expr::expr::statements::remove::RemoveStatement;
use surrealdb_expr::expr::{Expr, Kind};
use surrealdb_types::ToSql;

use super::ddl::{lower_mysql_ddl, lower_pgsql_ddl, map_mysql_type, map_pgsql_type};

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
		assert_eq!(map_pgsql_type(&ct.columns[0].data_type), Kind::Int);
		assert_eq!(map_pgsql_type(&ct.columns[1].data_type), Kind::Int);
		assert_eq!(map_pgsql_type(&ct.columns[2].data_type), Kind::Int);
		assert_eq!(map_pgsql_type(&ct.columns[3].data_type), Kind::Int);
		assert_eq!(map_pgsql_type(&ct.columns[4].data_type), Kind::Float);
		assert_eq!(map_pgsql_type(&ct.columns[5].data_type), Kind::Float);
		assert_eq!(map_pgsql_type(&ct.columns[6].data_type), Kind::Decimal);
		assert_eq!(map_pgsql_type(&ct.columns[7].data_type), Kind::String);
		assert_eq!(map_pgsql_type(&ct.columns[8].data_type), Kind::String);
		assert_eq!(map_pgsql_type(&ct.columns[9].data_type), Kind::String);
		assert_eq!(map_pgsql_type(&ct.columns[10].data_type), Kind::Bool);
		assert_eq!(map_pgsql_type(&ct.columns[11].data_type), Kind::Datetime);
		assert_eq!(map_pgsql_type(&ct.columns[12].data_type), Kind::Datetime);
		assert_eq!(map_pgsql_type(&ct.columns[13].data_type), Kind::Datetime);
		assert_eq!(map_pgsql_type(&ct.columns[14].data_type), Kind::Datetime);
		assert_eq!(map_pgsql_type(&ct.columns[15].data_type), Kind::Uuid);
		assert_eq!(map_pgsql_type(&ct.columns[16].data_type), Kind::Object);
		assert_eq!(map_pgsql_type(&ct.columns[17].data_type), Kind::Object);
		assert_eq!(map_pgsql_type(&ct.columns[18].data_type), Kind::Bytes);
		assert_eq!(
			map_pgsql_type(&ct.columns[19].data_type),
			Kind::Array(Box::new(Kind::Int), None)
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
	assert_eq!(map_mysql_type("int"), Kind::Int);
	assert_eq!(map_mysql_type("tinyint(1)"), Kind::Int);
	assert_eq!(map_mysql_type("smallint"), Kind::Int);
	assert_eq!(map_mysql_type("mediumint"), Kind::Int);
	assert_eq!(map_mysql_type("bigint unsigned"), Kind::Int);
	assert_eq!(map_mysql_type("float"), Kind::Float);
	assert_eq!(map_mysql_type("double"), Kind::Float);
	assert_eq!(map_mysql_type("decimal(10,2)"), Kind::Decimal);
	assert_eq!(map_mysql_type("varchar(100)"), Kind::String);
	assert_eq!(map_mysql_type("char(2)"), Kind::String);
	assert_eq!(map_mysql_type("text"), Kind::String);
	assert_eq!(map_mysql_type("longtext"), Kind::String);
	assert_eq!(map_mysql_type("bool"), Kind::Bool);
	assert_eq!(map_mysql_type("boolean"), Kind::Bool);
	assert_eq!(map_mysql_type("date"), Kind::Datetime);
	assert_eq!(map_mysql_type("datetime"), Kind::Datetime);
	assert_eq!(map_mysql_type("timestamp"), Kind::Datetime);
	assert_eq!(map_mysql_type("json"), Kind::Object);
	assert_eq!(map_mysql_type("blob"), Kind::Bytes);
	assert_eq!(map_mysql_type("bit(8)"), Kind::Bytes);
	assert_eq!(map_mysql_type("enum('a','b')"), Kind::String);
	assert_eq!(map_mysql_type("set('x','y')"), Kind::String);
}

#[test]
fn test_lower_pgsql_create_table_simple() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email VARCHAR(100) UNIQUE, age INT DEFAULT 18);"
	).expect("parse");

	let exprs = lower_pgsql_ddl(&stmt).expect("lower");
	// 1 DefineTable + 4 DefineField + 2 DefineIndex (id PK + email UNIQUE) = 7
	assert_eq!(exprs.len(), 7);

	match &exprs[0] {
		Expr::Define(d) => match &**d {
			DefineStatement::Table(t) => {
				assert_eq!(t.full, true);
				assert_eq!(t.kind, DefineKind::Default);
			}
			_ => panic!("Expected DefineTable"),
		},
		_ => panic!("Expected Expr::Define"),
	}

	// Field id
	match &exprs[1] {
		Expr::Define(d) => match &**d {
			DefineStatement::Field(f) => {
				assert_eq!(f.field_kind, Some(Kind::Int));
			}
			_ => panic!("Expected DefineField id"),
		},
		_ => panic!("Expected Expr::Define"),
	}

	// Field name (NOT NULL)
	match &exprs[2] {
		Expr::Define(d) => match &**d {
			DefineStatement::Field(f) => {
				assert_eq!(f.field_kind, Some(Kind::String));
				assert!(f.assert.is_some());
			}
			_ => panic!("Expected DefineField name"),
		},
		_ => panic!("Expected Expr::Define"),
	}

	// Field email
	match &exprs[3] {
		Expr::Define(d) => match &**d {
			DefineStatement::Field(f) => {
				assert_eq!(
					f.field_kind,
					Some(Kind::either(vec![Kind::None, Kind::Null, Kind::String]))
				);
			}
			_ => panic!("Expected DefineField email"),
		},
		_ => panic!("Expected Expr::Define"),
	}

	// Field age with default 18
	match &exprs[4] {
		Expr::Define(d) => match &**d {
			DefineStatement::Field(f) => {
				assert_eq!(
					f.field_kind,
					Some(Kind::either(vec![Kind::None, Kind::Null, Kind::Int]))
				);
				assert!(matches!(
					f.default,
					surrealdb_expr::expr::statements::define::DefineDefault::Set(_)
				));
			}
			_ => panic!("Expected DefineField age"),
		},
		_ => panic!("Expected Expr::Define"),
	}

	// Index 1: id primary key index
	match &exprs[5] {
		Expr::Define(d) => match &**d {
			DefineStatement::Index(idx) => {
				assert_eq!(idx.index, surrealdb_expr::expr::index_kind::Index::Uniq);
			}
			_ => panic!("Expected DefineIndex for primary key"),
		},
		_ => panic!("Expected Expr::Define"),
	}

	// Index 2: email unique index
	match &exprs[6] {
		Expr::Define(d) => match &**d {
			DefineStatement::Index(idx) => {
				assert_eq!(idx.index, surrealdb_expr::expr::index_kind::Index::Uniq);
			}
			_ => panic!("Expected DefineIndex for unique"),
		},
		_ => panic!("Expected Expr::Define"),
	}
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

	let exprs = lower_pgsql_ddl(&stmt).expect("lower");
	// 1 DefineTable + 3 DefineField + 2 TableConstraint indexes = 6
	assert_eq!(exprs.len(), 6);

	// DefineTable with IF NOT EXISTS
	match &exprs[0] {
		Expr::Define(d) => match &**d {
			DefineStatement::Table(t) => {
				assert_eq!(t.kind, DefineKind::IfNotExists);
			}
			_ => panic!("Expected DefineTable"),
		},
		_ => panic!("Expected Expr::Define"),
	}
}

#[test]
fn test_lower_pgsql_create_index() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users (email);",
	)
	.expect("parse");

	let exprs = lower_pgsql_ddl(&stmt).expect("lower");
	assert_eq!(exprs.len(), 1);

	match &exprs[0] {
		Expr::Define(d) => match &**d {
			DefineStatement::Index(idx) => {
				assert_eq!(idx.index, surrealdb_expr::expr::index_kind::Index::Uniq);
				assert_eq!(idx.kind, DefineKind::IfNotExists);
				assert_eq!(idx.cols.len(), 1);
			}
			_ => panic!("Expected DefineIndex"),
		},
		_ => panic!("Expected Expr::Define"),
	}

	let multi_idx = surrealdb_pgsql::parse_statement(
		"CREATE INDEX idx_orders_customer_status ON orders (customer_id, status);",
	)
	.expect("parse");

	let exprs_multi = lower_pgsql_ddl(&multi_idx).expect("lower");
	assert_eq!(exprs_multi.len(), 1);
	match &exprs_multi[0] {
		Expr::Define(d) => match &**d {
			DefineStatement::Index(idx) => {
				assert_eq!(idx.index, surrealdb_expr::expr::index_kind::Index::Idx);
				assert_eq!(idx.cols.len(), 2);
			}
			_ => panic!("Expected DefineIndex"),
		},
		_ => panic!("Expected Expr::Define"),
	}
}

#[test]
fn test_lower_pgsql_create_view() {
	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE OR REPLACE VIEW active_users AS SELECT id, name FROM users WHERE active = true;",
	)
	.expect("parse");

	let exprs = lower_pgsql_ddl(&stmt).expect("lower");
	assert_eq!(exprs.len(), 1);

	match &exprs[0] {
		Expr::Define(d) => match &**d {
			DefineStatement::Table(t) => {
				assert_eq!(t.kind, DefineKind::Overwrite);
				assert!(t.view.is_some());
				let v = t.view.as_ref().unwrap();
				assert_eq!(v.what.len(), 1);
				assert!(v.cond.is_some());
			}
			_ => panic!("Expected DefineTable for view"),
		},
		_ => panic!("Expected Expr::Define"),
	}
}

#[test]
fn test_lower_pgsql_alter_table() {
	let stmt = surrealdb_pgsql::parse_statement("ALTER TABLE users ADD COLUMN phone VARCHAR(20);")
		.expect("parse");
	let exprs = lower_pgsql_ddl(&stmt).expect("lower");
	assert_eq!(exprs.len(), 1);
	assert!(matches!(&exprs[0], Expr::Define(d) if matches!(&**d, DefineStatement::Field(_))));

	let drop_col =
		surrealdb_pgsql::parse_statement("ALTER TABLE users DROP COLUMN IF EXISTS phone;")
			.expect("parse");
	let exprs_drop = lower_pgsql_ddl(&drop_col).expect("lower");
	assert_eq!(exprs_drop.len(), 1);
	assert!(matches!(&exprs_drop[0], Expr::Remove(r) if matches!(&**r, RemoveStatement::Field(_))));

	let alter_type =
		surrealdb_pgsql::parse_statement("ALTER TABLE users ALTER COLUMN age TYPE BIGINT;")
			.expect("parse");
	let exprs_type = lower_pgsql_ddl(&alter_type).expect("lower");
	assert_eq!(exprs_type.len(), 1);

	let set_def =
		surrealdb_pgsql::parse_statement("ALTER TABLE users ALTER COLUMN age SET DEFAULT 21;")
			.expect("parse");
	let exprs_def = lower_pgsql_ddl(&set_def).expect("lower");
	assert_eq!(exprs_def.len(), 1);

	let drop_def =
		surrealdb_pgsql::parse_statement("ALTER TABLE users ALTER COLUMN age DROP DEFAULT;")
			.expect("parse");
	let exprs_drop_def = lower_pgsql_ddl(&drop_def).expect("lower");
	assert_eq!(exprs_drop_def.len(), 1);

	let set_nn =
		surrealdb_pgsql::parse_statement("ALTER TABLE users ALTER COLUMN age SET NOT NULL;")
			.expect("parse");
	let exprs_nn = lower_pgsql_ddl(&set_nn).expect("lower");
	assert_eq!(exprs_nn.len(), 1);

	let drop_nn =
		surrealdb_pgsql::parse_statement("ALTER TABLE users ALTER COLUMN age DROP NOT NULL;")
			.expect("parse");
	let exprs_drop_nn = lower_pgsql_ddl(&drop_nn).expect("lower");
	assert_eq!(exprs_drop_nn.len(), 1);
}

#[test]
fn test_lower_pgsql_drop() {
	let stmt =
		surrealdb_pgsql::parse_statement("DROP TABLE IF EXISTS users, profiles;").expect("parse");
	let exprs = lower_pgsql_ddl(&stmt).expect("lower");
	assert_eq!(exprs.len(), 2);
	for e in &exprs {
		assert!(
			matches!(e, Expr::Remove(r) if matches!(&**r, RemoveStatement::Table(t) if t.if_exists))
		);
	}

	let drop_idx =
		surrealdb_pgsql::parse_statement("DROP INDEX IF EXISTS idx_users_email;").expect("parse");
	let exprs_idx = lower_pgsql_ddl(&drop_idx).expect("lower");
	assert_eq!(exprs_idx.len(), 1);
	assert!(
		matches!(&exprs_idx[0], Expr::Remove(r) if matches!(&**r, RemoveStatement::Index(i) if i.if_exists))
	);

	let drop_view = surrealdb_pgsql::parse_statement("DROP VIEW active_users;").expect("parse");
	let exprs_view = lower_pgsql_ddl(&drop_view).expect("lower");
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

	let exprs = lower_mysql_ddl(&stmt).expect("lower");
	// 1 DefineTable + 7 DefineField + 2 DefineIndex (id PK + sku UNIQUE) = 10
	assert_eq!(exprs.len(), 10);

	match &exprs[0] {
		Expr::Define(d) => match &**d {
			DefineStatement::Table(t) => {
				assert_eq!(t.full, true);
				assert_eq!(t.kind, DefineKind::IfNotExists);
			}
			_ => panic!("Expected DefineTable"),
		},
		_ => panic!("Expected Expr::Define"),
	}
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

	let exprs = lower_mysql_ddl(&stmt).expect("lower");
	// 1 DefineTable + 3 DefineField + 2 TableConstraint indexes = 6
	assert_eq!(exprs.len(), 6);
}

#[test]
fn test_lower_mysql_drop_table() {
	let stmt =
		surrealdb_mysql::parse_statement("DROP TABLE IF EXISTS orders, items;").expect("parse");

	let exprs = lower_mysql_ddl(&stmt).expect("lower");
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

	let exprs = lower_pgsql_ddl(&stmt).expect("lower");
	// 1 DefineTable + 3 DefineField + 1 PK Index = 5
	assert_eq!(exprs.len(), 5);

	// Find the author_id field
	let author_field = exprs
		.iter()
		.find(|e| match e {
			Expr::Define(d) => match &**d {
				DefineStatement::Field(f) => f.name.to_sql() == "author_id",
				_ => false,
			},
			_ => false,
		})
		.expect("found author_id field");

	if let Expr::Define(d) = author_field
		&& let DefineStatement::Field(f) = &**d
	{
		assert!(f.reference.is_some(), "author_id should have a reference constraint");
	}
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

	let exprs = lower_pgsql_ddl(&stmt).expect("lower");
	// 1 DefineTable + 4 DefineField + 1 PK Index + 2 FK field updates = 8
	assert_eq!(exprs.len(), 8);
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

	let exprs = lower_pgsql_ddl(&stmt).expect("lower");
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
		let exprs = lower_pgsql_ddl(&stmt).expect("lower");
		match &exprs[0] {
			Expr::Define(d) => match &**d {
				DefineStatement::Field(f) => f.field_kind.clone(),
				_ => panic!("Expected DefineField"),
			},
			_ => panic!("Expected Expr::Define"),
		}
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

	let exprs = lower_pgsql_ddl(&stmt).expect("lower");
	assert_eq!(exprs.len(), 5);

	// Action 1: Add column status (DefineField)
	assert!(matches!(&exprs[0], Expr::Define(d) if matches!(&**d, DefineStatement::Field(_))));
	// Action 2: Drop column old_status (RemoveField)
	assert!(matches!(&exprs[1], Expr::Remove(r) if matches!(&**r, RemoveStatement::Field(_))));
	// Action 3: Alter balance type (DefineField)
	assert!(matches!(&exprs[2], Expr::Define(d) if matches!(&**d, DefineStatement::Field(_))));
	// Action 4: Set balance default (DefineField)
	assert!(matches!(&exprs[3], Expr::Define(d) if matches!(&**d, DefineStatement::Field(_))));
	// Action 5: Set user_id NOT NULL (DefineField)
	assert!(matches!(&exprs[4], Expr::Define(d) if matches!(&**d, DefineStatement::Field(_))));
}

#[test]
fn test_lower_pgsql_alter_table_constraints() {
	let add_pk = surrealdb_pgsql::parse_statement(
		"ALTER TABLE memberships ADD PRIMARY KEY (user_id, team_id);",
	)
	.expect("parse");
	let exprs_pk = lower_pgsql_ddl(&add_pk).expect("lower");
	assert_eq!(exprs_pk.len(), 1);
	match &exprs_pk[0] {
		Expr::Define(d) => match &**d {
			DefineStatement::Index(idx) => {
				assert_eq!(idx.index, surrealdb_expr::expr::index_kind::Index::Uniq);
				assert_eq!(idx.cols.len(), 2);
			}
			_ => panic!("Expected DefineIndex"),
		},
		_ => panic!("Expected Expr::Define"),
	}

	let add_uq =
		surrealdb_pgsql::parse_statement("ALTER TABLE memberships ADD UNIQUE (user_id, role);")
			.expect("parse");
	let exprs_uq = lower_pgsql_ddl(&add_uq).expect("lower");
	assert_eq!(exprs_uq.len(), 1);
	match &exprs_uq[0] {
		Expr::Define(d) => match &**d {
			DefineStatement::Index(idx) => {
				assert_eq!(idx.index, surrealdb_expr::expr::index_kind::Index::Uniq);
				assert_eq!(idx.cols.len(), 2);
			}
			_ => panic!("Expected DefineIndex"),
		},
		_ => panic!("Expected Expr::Define"),
	}

	let drop_con = surrealdb_pgsql::parse_statement(
		"ALTER TABLE memberships DROP CONSTRAINT IF EXISTS uq_user_role;",
	)
	.expect("parse");
	let exprs_drop_con = lower_pgsql_ddl(&drop_con).expect("lower");
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

	let exprs = lower_pgsql_ddl(&stmt).expect("lower");
	assert_eq!(exprs.len(), 1);
	match &exprs[0] {
		Expr::Define(d) => match &**d {
			DefineStatement::Index(idx) => {
				assert!(idx.concurrently);
				assert_eq!(idx.index, surrealdb_expr::expr::index_kind::Index::Idx);
			}
			_ => panic!("Expected DefineIndex"),
		},
		_ => panic!("Expected Expr::Define"),
	}
}

#[test]
fn test_lower_pgsql_create_view_wildcard() {
	let stmt = surrealdb_pgsql::parse_statement("CREATE VIEW v_all_users AS SELECT * FROM users;")
		.expect("parse");

	let exprs = lower_pgsql_ddl(&stmt).expect("lower");
	assert_eq!(exprs.len(), 1);
	match &exprs[0] {
		Expr::Define(d) => match &**d {
			DefineStatement::Table(t) => {
				assert!(t.view.is_some());
				let v = t.view.as_ref().unwrap();
				assert_eq!(v.expr, surrealdb_expr::expr::Fields::all());
			}
			_ => panic!("Expected DefineTable"),
		},
		_ => panic!("Expected Expr::Define"),
	}
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

	let exprs = lower_mysql_ddl(&stmt).expect("lower");
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

	let exprs = lower_mysql_ddl(&stmt).expect("lower");
	// 1 DefineTable + 3 DefineField + 1 PK Index + 2 FK fields = 7
	assert_eq!(exprs.len(), 7);
}

#[test]
fn test_lower_mysql_drop_multiple_tables() {
	let stmt =
		surrealdb_mysql::parse_statement("DROP TABLE IF EXISTS t1, t2, t3, t4;").expect("parse");

	let exprs = lower_mysql_ddl(&stmt).expect("lower");
	assert_eq!(exprs.len(), 4);
	for (i, expr) in exprs.iter().enumerate() {
		match expr {
			Expr::Remove(r) => match &**r {
				RemoveStatement::Table(t) => {
					assert!(t.if_exists);
					assert_eq!(t.name.to_sql(), format!("t{}", i + 1));
				}
				_ => panic!("Expected RemoveTable"),
			},
			_ => panic!("Expected Expr::Remove"),
		}
	}
}
