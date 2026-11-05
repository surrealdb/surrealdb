//! The lowering of PostgreSQL user-defined types (ADR 0014 D9) onto `DEFINE TYPE`: `CREATE DOMAIN`,
//! `ALTER DOMAIN`, `CREATE TYPE ... AS (...)`, `CREATE TYPE ... AS ENUM (...)`, `ALTER TYPE` of an
//! enum, and the `DROP` of any of them.
//!
//! A column declared with one of these types is a field whose kind names the type and whose
//! `SQL TYPE` names it too; the type itself carries what a value of it must satisfy, so a change
//! to the type reaches every column of it without restating a field.
//!
//! - A domain is a type whose body is its base kind, admitting `NONE` and `NULL` unless the domain
//!   is `NOT NULL`, with one named `ASSERT` per `CHECK` (testing `check != false`, so a check that
//!   yields NULL passes, as in PostgreSQL), its `DEFAULT`, and `SQL TYPE POSTGRESQL` naming the
//!   base type.
//! - A composite type is a type whose body is an object of its attributes, each admitting `NONE`
//!   and `NULL`, with `SQL TYPE POSTGRESQL '(attr type, …)'` keeping the attributes' declared types
//!   and their order.
//! - An enum is a type whose body is a union of its labels.

use surrealdb_expr::expr::statements::alter::{
	AlterStatement, AlterTypeAction, AlterTypeStatement, EnumPosition,
};
use surrealdb_expr::expr::statements::define::{
	DefineKind, DefineStatement, DefineTypeStatement, TypeAssert, union_kind,
};
use surrealdb_expr::expr::statements::remove::{
	RemoveStatement, RemoveTypeStatement, SqlTypeObject,
};
use surrealdb_expr::expr::{BinaryOperator, Expr, Kind, KindLiteral, Literal};
use surrealdb_pgsql::ast::stmt::{
	AlterDomainAction, AlterDomainStmt, AlterTypeAction as PgAlterTypeAction,
	AlterTypeStmt as PgAlterTypeStmt, CreateCompositeTypeStmt, CreateDomainStmt,
	CreateTypeStmt as PgCreateTypeStmt, DomainConstraintKind, EnumValuePlacement,
};
use surrealdb_strand::Strand;

use super::super::write::lower_pgsql_expr;
use super::{domain_of, lower_pgsql_domain_check, map_pgsql_type_in, pgsql_declared_type};
use crate::SqlType;
use crate::catalog::{BinderCatalog, DomainCheck, DomainType};
use crate::error::PlanError;

/// The name a statement gives a type, as the catalog keys it.
fn type_name(parts: &[String]) -> Result<String, PlanError> {
	let (_, name) = crate::namespace::resolve_pgsql_table_name(parts, None)?;
	Ok(name.as_str().to_owned())
}

/// The domain `name` of `catalog`, looked up as written and as PostgreSQL folds it.
fn find_domain(catalog: &dyn BinderCatalog, name: &str) -> Option<DomainType> {
	catalog.domain_type(name).or_else(|| catalog.domain_type(&name.to_lowercase()))
}

/// Whether a value of `kind` may be NULL.
fn admits_null(kind: &Kind) -> bool {
	if let Kind::Either(kinds) = kind {
		return kinds.iter().any(admits_null);
	}
	matches!(kind, Kind::Any | Kind::Null)
}

/// `kind`, admitting `NONE` and `NULL`.
fn nullable(kind: Kind) -> Kind {
	Kind::either(vec![Kind::None, Kind::Null, kind])
}

/// The `DEFINE TYPE` of `statement`.
fn define(statement: DefineTypeStatement) -> Expr {
	Expr::Define(Box::new(DefineStatement::Type(statement)))
}

/// The `DEFINE TYPE` of the domain `domain`.
pub(crate) fn define_domain(domain: &DomainType, kind: DefineKind) -> Expr {
	define(DefineTypeStatement {
		kind,
		name: Strand::new(domain.name.as_str()),
		body: if domain.not_null {
			domain.base.clone()
		} else {
			nullable(domain.base.clone())
		},
		// A `NOT NULL` domain over a base that admits NULL (`json`) cannot leave NULL out of its
		// body, so it states the constraint as an assertion of its own.
		asserts: (domain.not_null && admits_null(&domain.base))
			.then(|| TypeAssert {
				name: Some(format!("{}_not_null", domain.name)),
				check: super::not_null_assertion(),
			})
			.into_iter()
			.chain(domain.checks.iter().map(|check| TypeAssert {
				name: check.name.clone(),
				check: Expr::Binary {
					left: Box::new(check.check.clone()),
					op: BinaryOperator::NotEqual,
					right: Box::new(Expr::Literal(Literal::Bool(false))),
				},
			}))
			.collect(),
		// PostgreSQL gives a column of a domain with no default NULL when a write omits it.
		default: Some(domain.default.clone().unwrap_or(Expr::Literal(Literal::Null))),
		sql_type: Some(domain.base_type.clone()),
		..Default::default()
	})
}

/// Lower `CREATE DOMAIN name AS type [DEFAULT ...] [NOT NULL | NULL] [CHECK (...)] ...`. The base
/// may be any type a column may have but a domain or a composite type, whose own constraints a
/// domain over it could not carry.
pub(crate) fn lower_create_domain(
	create: &CreateDomainStmt,
	catalog: &dyn BinderCatalog,
) -> Result<Vec<Expr>, PlanError> {
	let name = type_name(&create.name.parts)?;
	super::super::vector_ddl::check_column(&create.data_type)?;
	if let Some(base) = create.data_type.name.parts.last() {
		let base = base.trim_end_matches("[]");
		if find_domain(catalog, base).is_some()
			|| catalog.composite_type(base).is_some()
			|| catalog.composite_type(&base.to_lowercase()).is_some()
		{
			return Err(PlanError::unsupported(format!(
				"a domain over the user-defined type \"{base}\""
			)));
		}
	}
	let mut domain = DomainType {
		name: name.clone(),
		id: 0,
		base: map_pgsql_type_in(&create.data_type, catalog),
		base_type: pgsql_declared_type(&create.data_type)?.text,
		not_null: false,
		default: None,
		checks: Vec::new(),
	};
	for constraint in &create.constraints {
		match &constraint.kind {
			DomainConstraintKind::NotNull => domain.not_null = true,
			DomainConstraintKind::Null => domain.not_null = false,
			DomainConstraintKind::Default(expr) => {
				domain.default = Some(lowered_default(expr, &domain, catalog)?);
			}
			DomainConstraintKind::Check(expr) => {
				let check_name = match &constraint.name {
					Some(given) => {
						if domain.checks.iter().any(|c| c.name.as_deref() == Some(given.as_str())) {
							return Err(PlanError::DuplicateDomainConstraint {
								constraint: given.clone(),
								domain: name,
							});
						}
						given.clone()
					}
					None => generated_check_name(&name, &domain.checks),
				};
				domain.checks.push(DomainCheck {
					name: Some(check_name),
					check: lower_pgsql_domain_check(expr)?,
				});
			}
		}
	}
	Ok(vec![define_domain(&domain, DefineKind::Default)])
}

/// The name PostgreSQL gives a check of the domain `domain` declared with none: `<domain>_check`,
/// numbered from 1 when a check already has that name.
fn generated_check_name(domain: &str, checks: &[DomainCheck]) -> String {
	let taken = |candidate: &str| checks.iter().any(|c| c.name.as_deref() == Some(candidate));
	let base = format!("{domain}_check");
	if !taken(&base) {
		return base;
	}
	(1..).map(|n| format!("{base}{n}")).find(|candidate| !taken(candidate)).unwrap_or(base)
}

/// Lower `ALTER DOMAIN`: the domain's new definition, which every column of the domain enforces
/// from then on. `VALIDATE CONSTRAINT` changes nothing, as every check of a domain holds for each
/// value written.
///
/// Rows written before the change are not checked against it.
pub(crate) fn lower_alter_domain(
	alter: &AlterDomainStmt,
	catalog: &dyn BinderCatalog,
) -> Result<Vec<Expr>, PlanError> {
	let name = type_name(&alter.name.parts)?;
	let current =
		find_domain(catalog, &name).ok_or_else(|| PlanError::UndefinedType(name.clone()))?;
	let mut updated = current.clone();
	let missing_constraint = |constraint: &str| PlanError::UndefinedDomainConstraint {
		constraint: constraint.to_owned(),
		domain: current.name.clone(),
	};
	match &alter.action {
		AlterDomainAction::SetDefault(expr) => {
			updated.default = Some(lowered_default(expr, &updated, catalog)?);
		}
		AlterDomainAction::DropDefault => updated.default = None,
		AlterDomainAction::SetNotNull => updated.not_null = true,
		AlterDomainAction::DropNotNull => updated.not_null = false,
		AlterDomainAction::AddConstraint {
			name: given,
			check,
		} => {
			let check_name = match given {
				Some(given) => {
					if updated.checks.iter().any(|c| c.name.as_deref() == Some(given.as_str())) {
						return Err(PlanError::DuplicateDomainConstraint {
							constraint: given.clone(),
							domain: current.name.clone(),
						});
					}
					given.clone()
				}
				None => generated_check_name(&current.name, &updated.checks),
			};
			updated.checks.push(DomainCheck {
				name: Some(check_name),
				check: lower_pgsql_domain_check(check)?,
			});
		}
		AlterDomainAction::DropConstraint {
			name: dropped,
			if_exists,
		} => {
			let before = updated.checks.len();
			updated.checks.retain(|c| c.name.as_deref() != Some(dropped.as_str()));
			if updated.checks.len() == before {
				if *if_exists {
					return Ok(Vec::new());
				}
				return Err(missing_constraint(dropped));
			}
		}
		AlterDomainAction::ValidateConstraint {
			name: validated,
		} => {
			if updated.checks.iter().any(|c| c.name.as_deref() == Some(validated.as_str())) {
				return Ok(Vec::new());
			}
			return Err(missing_constraint(validated));
		}
	}
	Ok(vec![define_domain(&updated, DefineKind::Overwrite)])
}

/// A domain's `DEFAULT`, lowered. A constant must be a value of the domain's base type, as
/// PostgreSQL requires when the domain is defined rather than when the default is first taken: a
/// string constant must read as the base type, and a boolean or numeric constant, signed or not,
/// must be one the base type is assigned from (a boolean to a boolean or text type, a number to a
/// numeric or text type). A base type that names another user type is not judged here.
///
/// A constant of a text, boolean, integer, floating-point or numeric base type is stored as the
/// value the assignment converts it to, so a row that takes the default holds a value of the base
/// type: `1` is `'1'` to `text`, `true` is `'true'`, `2.5` is `3` to `integer` (a numeric rounds
/// half away from zero), and a numeric's precision and scale round it and bound it. A constant
/// beyond the range of the base type is refused, with PostgreSQL's `22003` message.
fn lowered_default(
	default: &surrealdb_pgsql::ast::expr::Expr,
	domain: &DomainType,
	catalog: &dyn BinderCatalog,
) -> Result<Expr, PlanError> {
	use crate::BoundLiteral;
	let Some(constant) = default_constant(default) else {
		if let Some(found) = expression_type_refused(default, domain, catalog) {
			return Err(PlanError::DatatypeMismatch(format!(
				"column \"{}\" is of type {} but default expression is of type {found}",
				domain.name, domain.base_type
			)));
		}
		return lower_pgsql_expr(default);
	};
	let found = match &constant {
		BoundLiteral::String(_) => {
			crate::semantics::eval_cast(
				&constant,
				&domain.base_type,
				&crate::DialectPolicy::postgres(),
			)?;
			None
		}
		BoundLiteral::Boolean(_) => {
			(!matches!(domain.base, Kind::Bool | Kind::String | Kind::Named(_)))
				.then_some("boolean")
		}
		BoundLiteral::Integer(_) | BoundLiteral::Unsigned(_) => {
			(!admits_number(&domain.base)).then_some("integer")
		}
		BoundLiteral::Float(_) | BoundLiteral::Numeric(_) => {
			(!admits_number(&domain.base)).then_some("numeric")
		}
		BoundLiteral::Null => None,
	};
	if let Some(found) = found {
		return Err(PlanError::DatatypeMismatch(format!(
			"column \"{}\" is of type {} but default expression is of type {found}",
			domain.name, domain.base_type
		)));
	}
	match assigned_constant(&constant, domain)? {
		Some(converted) => Ok(Expr::Literal(converted)),
		None => lower_pgsql_expr(default),
	}
}

/// The type of a domain's non-constant `DEFAULT`, when it is known and no assignment converts it
/// to the domain's base type: a number, a boolean, a date or timestamp and an interval each assign
/// only to their own kind of type. `None` when either type is outside those, or the expression's
/// type is not known before it runs.
fn expression_type_refused(
	default: &surrealdb_pgsql::ast::expr::Expr,
	domain: &DomainType,
	catalog: &dyn BinderCatalog,
) -> Option<String> {
	#[derive(PartialEq)]
	enum Family {
		Number,
		Boolean,
		Moment,
		Span,
	}
	let base = if matches!(domain.base, Kind::Bool) {
		Family::Boolean
	} else if matches!(domain.base, Kind::Datetime | Kind::Date) {
		Family::Moment
	} else if matches!(domain.base, Kind::Duration) {
		Family::Span
	} else if matches!(domain.base, Kind::String | Kind::Named(_)) || !admits_number(&domain.base) {
		return None;
	} else {
		Family::Number
	};
	let bound = crate::binder::pgsql::bind_expr(
		default,
		&crate::QueryScope::new(),
		catalog,
		&crate::DialectPolicy::postgres(),
	)
	.ok()?;
	let found = crate::ir::BoundSelect::default().infer_expr_type(&bound, catalog);
	let family = if matches!(
		found,
		SqlType::Int2
			| SqlType::Int4
			| SqlType::Int8
			| SqlType::UInt8
			| SqlType::Float4
			| SqlType::Float8
			| SqlType::Numeric
	) {
		Family::Number
	} else if matches!(found, SqlType::Bool) {
		Family::Boolean
	} else if matches!(found, SqlType::Date | SqlType::Timestamp | SqlType::TimestampTz) {
		Family::Moment
	} else if matches!(found, SqlType::Interval) {
		Family::Span
	} else {
		return None;
	};
	(family != base).then(|| found.name()).flatten()
}

/// The constant a domain's `DEFAULT` is, when it is one: a string, boolean or numeric literal, or
/// a numeric literal with a sign. `None` for `NULL`, a bit string, and any other expression.
fn default_constant(default: &surrealdb_pgsql::ast::expr::Expr) -> Option<crate::BoundLiteral> {
	use surrealdb_pgsql::ast::expr::Expr as PgExpr;
	use surrealdb_pgsql::ast::literal::Literal as PgLiteral;
	use surrealdb_pgsql::ast::op::UnaryOperator;

	use crate::BoundLiteral;
	if let PgExpr::Literal {
		lit,
		..
	} = default
	{
		return match lit {
			PgLiteral::Integer(i) => Some(BoundLiteral::Integer(*i)),
			PgLiteral::Float(f) => Some(BoundLiteral::Float(*f)),
			PgLiteral::Numeric(text) => Some(BoundLiteral::Numeric(text.clone())),
			PgLiteral::String(text) => Some(BoundLiteral::String(text.clone())),
			PgLiteral::Boolean(b) => Some(BoundLiteral::Boolean(*b)),
			PgLiteral::Null
			| PgLiteral::BitString {
				..
			} => None,
		};
	}
	let PgExpr::UnaryOp {
		op,
		expr,
		..
	} = default
	else {
		return None;
	};
	let negate = match op {
		UnaryOperator::Minus => true,
		UnaryOperator::Plus => false,
		UnaryOperator::Not | UnaryOperator::BitNot | UnaryOperator::Custom(_) => return None,
	};
	match default_constant(expr)? {
		BoundLiteral::Integer(i) if negate => Some(match i.checked_neg() {
			Some(negated) => BoundLiteral::Integer(negated),
			None => BoundLiteral::Numeric(i.unsigned_abs().to_string()),
		}),
		BoundLiteral::Float(f) if negate => Some(BoundLiteral::Float(-f)),
		BoundLiteral::Numeric(text) if negate => {
			Some(BoundLiteral::Numeric(match text.strip_prefix('-') {
				Some(positive) => positive.to_owned(),
				None => format!("-{text}"),
			}))
		}
		number @ (BoundLiteral::Integer(_) | BoundLiteral::Float(_) | BoundLiteral::Numeric(_)) => {
			Some(number)
		}
		BoundLiteral::Unsigned(_)
		| BoundLiteral::String(_)
		| BoundLiteral::Boolean(_)
		| BoundLiteral::Null => None,
	}
}

/// The literal a domain's constant `DEFAULT` is assigned as, for a domain over a text, boolean,
/// integer, floating-point or numeric type; `None` for any other base type, whose default is
/// lowered as it is written. A constant beyond the range of the base type is refused.
fn assigned_constant(
	constant: &crate::BoundLiteral,
	domain: &DomainType,
) -> Result<Option<Literal>, PlanError> {
	use crate::BoundLiteral;
	use crate::semantics::eval_cast;
	let policy = crate::DialectPolicy::postgres();
	let base = &domain.base;
	if matches!(base, Kind::String | Kind::Bool) {
		let target = if matches!(base, Kind::String) {
			"text"
		} else {
			"boolean"
		};
		return Ok(match eval_cast(constant, target, &policy)? {
			BoundLiteral::String(text) => Some(Literal::String(Strand::from(text.as_str()))),
			BoundLiteral::Boolean(b) => Some(Literal::Bool(b)),
			BoundLiteral::Integer(_)
			| BoundLiteral::Unsigned(_)
			| BoundLiteral::Float(_)
			| BoundLiteral::Numeric(_)
			| BoundLiteral::Null => None,
		});
	}
	if matches!(base, Kind::I16 | Kind::I32 | Kind::Int) {
		return assigned_integer(constant, base).map(|n| n.map(Literal::Integer));
	}
	if matches!(base, Kind::F32 | Kind::Float) {
		return assigned_float(constant, base).map(|f| f.map(Literal::Float));
	}
	if matches!(base, Kind::Decimal | Kind::BigDecimal) {
		return assigned_numeric(constant, domain);
	}
	Ok(None)
}

/// `constant` assigned to the integer kind `kind` (`smallint`, `integer` or `bigint`): a numeric
/// rounds half away from zero, and a value beyond the type's range is refused.
fn assigned_integer(constant: &crate::BoundLiteral, kind: &Kind) -> Result<Option<i64>, PlanError> {
	use crate::BoundLiteral;
	let (type_name, range) = if matches!(kind, Kind::I16) {
		("smallint", i64::from(i16::MIN)..=i64::from(i16::MAX))
	} else if matches!(kind, Kind::I32) {
		("integer", i64::from(i32::MIN)..=i64::from(i32::MAX))
	} else {
		("bigint", i64::MIN..=i64::MAX)
	};
	// A text constant is read by the type's input, which names the text it refuses.
	let out_of_range = || {
		PlanError::ValueOutOfRange(match constant {
			BoundLiteral::String(text) => {
				format!("value \"{text}\" is out of range for type {type_name}")
			}
			BoundLiteral::Integer(_)
			| BoundLiteral::Unsigned(_)
			| BoundLiteral::Float(_)
			| BoundLiteral::Numeric(_)
			| BoundLiteral::Boolean(_)
			| BoundLiteral::Null => format!("{type_name} out of range"),
		})
	};
	match crate::semantics::eval_cast(constant, "bigint", &crate::DialectPolicy::postgres()) {
		Ok(BoundLiteral::Integer(n)) if range.contains(&n) => Ok(Some(n)),
		Ok(BoundLiteral::Integer(_)) | Err(PlanError::NumericValueOutOfRange) => {
			Err(out_of_range())
		}
		Ok(
			BoundLiteral::Unsigned(_)
			| BoundLiteral::Float(_)
			| BoundLiteral::Numeric(_)
			| BoundLiteral::String(_)
			| BoundLiteral::Boolean(_)
			| BoundLiteral::Null,
		) => Ok(None),
		Err(err) => Err(err),
	}
}

/// `constant` assigned to the floating-point kind `kind` (`real` or `double precision`): a value
/// beyond the type's range overflows, and a nonzero one it would hold as zero underflows.
fn assigned_float(constant: &crate::BoundLiteral, kind: &Kind) -> Result<Option<f64>, PlanError> {
	use crate::BoundLiteral;
	let target = if matches!(kind, Kind::F32) {
		"float4"
	} else {
		"float8"
	};
	match crate::semantics::eval_cast(constant, target, &crate::DialectPolicy::postgres()) {
		Ok(BoundLiteral::Float(f)) => Ok(Some(f)),
		Ok(
			BoundLiteral::Integer(_)
			| BoundLiteral::Unsigned(_)
			| BoundLiteral::Numeric(_)
			| BoundLiteral::String(_)
			| BoundLiteral::Boolean(_)
			| BoundLiteral::Null,
		) => Ok(None),
		Err(PlanError::NumericValueOutOfRange) => {
			let underflow = match constant {
				BoundLiteral::Numeric(text) => {
					text.parse::<f64>().is_ok_and(|value| value.abs() < 1.0)
				}
				BoundLiteral::Integer(_)
				| BoundLiteral::Unsigned(_)
				| BoundLiteral::Float(_)
				| BoundLiteral::String(_)
				| BoundLiteral::Boolean(_)
				| BoundLiteral::Null => false,
			};
			Err(PlanError::ValueOutOfRange(
				if underflow {
					"value out of range: underflow"
				} else {
					"value out of range: overflow"
				}
				.to_owned(),
			))
		}
		Err(err) => Err(err),
	}
}

/// `constant` assigned to the domain's `numeric` base type, rounded to the declared scale and
/// refused when its integer digits exceed the declared precision less the scale.
fn assigned_numeric(
	constant: &crate::BoundLiteral,
	domain: &DomainType,
) -> Result<Option<Literal>, PlanError> {
	use surrealdb_expr::val::{Number, Value};

	use crate::BoundLiteral;
	use crate::lower::expr::NumericValue;
	let text = match constant {
		BoundLiteral::Integer(i) => i.to_string(),
		BoundLiteral::Unsigned(u) => u.to_string(),
		BoundLiteral::Float(f) => f.to_string(),
		BoundLiteral::Numeric(text) => text.clone(),
		BoundLiteral::String(text) => text.trim().to_owned(),
		BoundLiteral::Boolean(_) | BoundLiteral::Null => return Ok(None),
	};
	let number = match crate::lower::expr::numeric_value(&text)? {
		NumericValue::Decimal(decimal) => Number::Decimal(decimal),
		NumericValue::BigDecimal(big) => Number::BigDecimal(Box::new(big)),
	};
	let mut value = Value::Number(number);
	if let Ok(declared) = crate::declared_type::DeclaredSqlType::parse(
		surrealdb_expr::expr::statements::define::SqlTypeDialect::PostgreSql,
		&domain.base_type,
	) {
		value =
			crate::sql_types::validate(&declared, value, &domain.name).map_err(|violation| {
				match violation.code {
					"22003" => PlanError::ValueOutOfRange(violation.message),
					"22P02" => PlanError::InvalidTextRepresentation(violation.message),
					_ => PlanError::DataException(violation.message),
				}
			})?;
	}
	Ok(if let Value::Number(Number::Decimal(decimal)) = value {
		Some(Literal::Decimal(decimal))
	} else if let Value::Number(Number::BigDecimal(big)) = value {
		Some(Literal::BigDecimal(big))
	} else {
		None
	})
}

/// Whether a column of kind `kind` is assigned from a numeric constant: a numeric or text kind, or
/// one that names a user type, which is not judged here.
fn admits_number(kind: &Kind) -> bool {
	matches!(
		kind,
		Kind::Int
			| Kind::I8
			| Kind::I16
			| Kind::I32
			| Kind::U8
			| Kind::U16
			| Kind::U32
			| Kind::U64
			| Kind::Float
			| Kind::F16
			| Kind::F32
			| Kind::Decimal
			| Kind::BigDecimal
			| Kind::Number
			| Kind::String
			| Kind::Named(_)
	)
}

/// Lower `CREATE TYPE name AS (attribute type, ...)`. Each attribute admits NULL, as every
/// attribute of a composite type does. An attribute of a domain is refused: the domain's checks
/// would apply to it, which the kind of the composite column cannot state.
pub(crate) fn lower_create_composite(
	create: &CreateCompositeTypeStmt,
	catalog: &dyn BinderCatalog,
) -> Result<Vec<Expr>, PlanError> {
	let name = type_name(&create.name.parts)?;
	let mut fields = std::collections::BTreeMap::new();
	let mut declared: Vec<String> = Vec::with_capacity(create.attributes.len());
	for attribute in &create.attributes {
		if fields.contains_key(&Strand::new(attribute.name.as_str())) {
			return Err(PlanError::DuplicateColumn(attribute.name.clone()));
		}
		super::super::vector_ddl::check_column(&attribute.data_type)?;
		if let Some(domain) = domain_of(&attribute.data_type, catalog)? {
			return Err(PlanError::unsupported(format!(
				"the attribute \"{}\" of the domain \"{}\" in a composite type",
				attribute.name, domain.name
			)));
		}
		let kind = map_pgsql_type_in(&attribute.data_type, catalog);
		let kind = if matches!(kind, Kind::Any) {
			kind
		} else {
			nullable(kind)
		};
		fields.insert(Strand::new(attribute.name.as_str()), kind);
		declared.push(format!(
			"{} {}",
			attribute.name,
			pgsql_declared_type(&attribute.data_type)?.text
		));
	}
	Ok(vec![define(DefineTypeStatement {
		name: Strand::new(name),
		body: Kind::Literal(KindLiteral::Object(fields)),
		sql_type: Some(format!("({})", declared.join(", "))),
		..Default::default()
	})])
}

/// Lower `CREATE TYPE name AS ENUM (...)` to a type whose body is the union of the labels.
/// A label listed more than once is refused, as PostgreSQL refuses it.
pub fn lower_create_enum(create: &PgCreateTypeStmt) -> Result<Vec<Expr>, PlanError> {
	let name = type_name(&create.name.parts)?;
	if create.labels.is_empty() {
		return Err(PlanError::unsupported(format!("the enum type \"{name}\" with no labels")));
	}
	for (index, label) in create.labels.iter().enumerate() {
		if create.labels.iter().take(index).any(|earlier| earlier == label) {
			return Err(PlanError::DuplicateEnumLabel(label.clone()));
		}
	}
	Ok(vec![define(DefineTypeStatement {
		name: Strand::new(name),
		body: union_kind(create.labels.iter().map(String::as_str)),
		..Default::default()
	})])
}

/// Lower `ALTER TYPE name ADD VALUE ...` and `ALTER TYPE name RENAME VALUE ...`.
pub fn lower_alter_enum(alter: &PgAlterTypeStmt) -> Result<Vec<Expr>, PlanError> {
	let name = type_name(&alter.name.parts)?;
	let action = match &alter.action {
		PgAlterTypeAction::AddValue {
			if_not_exists,
			value,
			placement,
		} => AlterTypeAction::AddValue {
			value: value.clone(),
			position: placement.as_ref().map(|placement| match placement {
				EnumValuePlacement::Before(anchor) => EnumPosition::Before(anchor.clone()),
				EnumValuePlacement::After(anchor) => EnumPosition::After(anchor.clone()),
			}),
			if_not_exists: *if_not_exists,
		},
		PgAlterTypeAction::RenameValue {
			from,
			to,
		} => AlterTypeAction::RenameValue {
			from: from.clone(),
			to: to.clone(),
		},
	};
	Ok(vec![Expr::Alter(Box::new(AlterStatement::Type(AlterTypeStatement {
		name: Strand::new(name),
		if_exists: false,
		actions: vec![action],
	})))])
}

/// The removal of the type `name`, for `DROP TYPE` (`object` is [`SqlTypeObject::Type`]) and
/// `DROP DOMAIN` ([`SqlTypeObject::Domain`]). The removal is refused when it runs if the type is
/// not that kind of object, since the type may be created earlier in the same request. `CASCADE`,
/// which would also drop the columns of the type, is refused.
pub(crate) fn drop_type(
	name: &str,
	object: SqlTypeObject,
	if_exists: bool,
	cascade: bool,
) -> Result<Expr, PlanError> {
	if cascade {
		let what = match object {
			SqlTypeObject::Type => "DROP TYPE",
			SqlTypeObject::Domain => "DROP DOMAIN",
		};
		return Err(PlanError::unsupported(format!("{what} ... CASCADE")));
	}
	Ok(Expr::Remove(Box::new(RemoveStatement::Type(RemoveTypeStatement {
		name: Strand::new(name),
		if_exists,
		object: Some(object),
	}))))
}
