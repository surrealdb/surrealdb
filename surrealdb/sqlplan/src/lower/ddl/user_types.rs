//! The lowering of PostgreSQL domains and composite types (ADR 0014 D9): `CREATE DOMAIN`,
//! `ALTER DOMAIN`, `CREATE TYPE ... AS (...)` and the `DROP` of either.
//!
//! A type is a catalog entity of the database (`DEFINE` of a user type), and a column declared
//! with it is an ordinary field: a domain column has the domain's base kind, its `NOT NULL` and
//! its checks joined into the field's `ASSERT` with the column's own `NOT NULL`, and its `DEFAULT`,
//! or NULL, when the column names none; a composite column is an object of the attributes' kinds.
//! `ALTER DOMAIN` therefore restates the field of every column of the domain alongside the new
//! definition.

use surrealdb_expr::expr::Expr;
use surrealdb_expr::expr::statements::alter::{AlterDefault, AlterKind};
use surrealdb_expr::expr::statements::define::{
	CompositeBody, CompositeField, DefineKind, DefineStatement, DefineUserTypeStatement,
	DomainBody, DomainCheck as BodyCheck, UserTypeBody, UserTypeKind,
};
use surrealdb_expr::expr::statements::remove::{RemoveStatement, RemoveUserTypeStatement};
use surrealdb_pgsql::ast::stmt::{
	AlterDomainAction, AlterDomainStmt, CreateCompositeTypeStmt, CreateDomainStmt,
	DomainConstraintKind,
};
use surrealdb_strand::Strand;

use super::super::foreign_key::{alter as alter_field, alter_column};
use super::super::write::lower_pgsql_expr;
use super::{
	column_kind, domain_assertion, domain_of, inherited_default, lower_pgsql_domain_check,
	map_pgsql_type_in, pgsql_declared_type,
};
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

/// The `DEFINE` of the user type `name` with `body`.
fn define(name: &str, body: UserTypeBody, kind: DefineKind) -> Expr {
	Expr::Define(Box::new(DefineStatement::UserType(DefineUserTypeStatement {
		kind,
		name: Strand::new(name),
		body,
	})))
}

/// The body of the domain `domain` as the catalog describes it.
fn domain_body(domain: &DomainType) -> DomainBody {
	DomainBody {
		base: domain.base.clone(),
		base_type: domain.base_type.clone(),
		not_null: domain.not_null,
		default: domain.default.clone(),
		checks: domain
			.checks
			.iter()
			.map(|check| BodyCheck {
				name: check.name.clone(),
				check: check.check.clone(),
			})
			.collect(),
	}
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
	let mut domain = DomainBody {
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
			DomainConstraintKind::Default(expr) => domain.default = Some(lower_pgsql_expr(expr)?),
			DomainConstraintKind::Check(expr) => {
				let check_name = match &constraint.name {
					Some(given) => given.clone(),
					None => generated_check_name(&name, &domain.checks),
				};
				domain.checks.push(BodyCheck {
					name: Some(check_name),
					check: lower_pgsql_domain_check(expr)?,
				});
			}
		}
	}
	Ok(vec![define(&name, UserTypeBody::Domain(domain), DefineKind::Default)])
}

/// The name PostgreSQL gives a check of the domain `domain` declared with none: `<domain>_check`,
/// numbered from 1 when a check already has that name.
fn generated_check_name(domain: &str, checks: &[BodyCheck]) -> String {
	let taken = |candidate: &str| checks.iter().any(|c| c.name.as_deref() == Some(candidate));
	let base = format!("{domain}_check");
	if !taken(&base) {
		return base;
	}
	(1..).map(|n| format!("{base}{n}")).find(|candidate| !taken(candidate)).unwrap_or(base)
}

/// Lower `ALTER DOMAIN`: the domain's new definition, and the field of each column of the domain
/// restated as the change requires, so that the columns enforce the domain as it now stands.
/// `VALIDATE CONSTRAINT` changes nothing, as every check of a domain holds for each value written.
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
		AlterDomainAction::SetDefault(expr) => updated.default = Some(lower_pgsql_expr(expr)?),
		AlterDomainAction::DropDefault => updated.default = None,
		AlterDomainAction::SetNotNull => updated.not_null = true,
		AlterDomainAction::DropNotNull => updated.not_null = false,
		AlterDomainAction::AddConstraint {
			name: given,
			check,
		} => {
			let body = domain_body(&updated);
			let check_name = match given {
				Some(given) => {
					if body.checks.iter().any(|c| c.name.as_deref() == Some(given.as_str())) {
						return Err(PlanError::DuplicateDomainConstraint {
							constraint: given.clone(),
							domain: current.name.clone(),
						});
					}
					given.clone()
				}
				None => generated_check_name(&current.name, &body.checks),
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
	let mut exprs = vec![define(
		&current.name,
		UserTypeBody::Domain(domain_body(&updated)),
		DefineKind::Overwrite,
	)];
	let not_null_changed = updated.not_null != current.not_null;
	let checks_changed = updated.checks != current.checks;
	let default_changed = updated.default != current.default;
	for column in catalog.domain_columns(&current.name) {
		let mut field = alter_column(&column.table, &column.column);
		let mut changed = false;
		if checks_changed || not_null_changed {
			field.assert = match domain_assertion(column.not_null, &updated) {
				Some(assertion) => AlterKind::Set(assertion),
				None => AlterKind::Drop,
			};
			changed = true;
		}
		// A column whose default is inherited from the domain follows it; one with a default of
		// its own keeps it, even one equal to the domain's.
		if default_changed && column.default.as_ref() == Some(&inherited_default(&current)) {
			field.default = AlterDefault::Set(inherited_default(&updated));
			changed = true;
		}
		if changed {
			exprs.push(alter_field(field));
		}
	}
	Ok(exprs)
}

/// Lower `CREATE TYPE name AS (attribute type, ...)`. Each attribute admits NULL, as every
/// attribute of a composite type does. An attribute of a domain is refused: the domain's checks
/// would apply to it, which the kind of the composite column cannot state.
pub(crate) fn lower_create_composite(
	create: &CreateCompositeTypeStmt,
	catalog: &dyn BinderCatalog,
) -> Result<Vec<Expr>, PlanError> {
	let name = type_name(&create.name.parts)?;
	let mut fields: Vec<CompositeField> = Vec::with_capacity(create.attributes.len());
	for attribute in &create.attributes {
		if fields.iter().any(|field| field.name == attribute.name) {
			return Err(PlanError::DuplicateColumn(attribute.name.clone()));
		}
		super::super::vector_ddl::check_column(&attribute.data_type)?;
		if let Some(domain) = domain_of(&attribute.data_type, catalog)? {
			return Err(PlanError::unsupported(format!(
				"the attribute \"{}\" of the domain \"{}\" in a composite type",
				attribute.name, domain.name
			)));
		}
		fields.push(CompositeField {
			name: attribute.name.clone(),
			kind: column_kind(&attribute.data_type, false, catalog),
			sql_type: pgsql_declared_type(&attribute.data_type)?.text,
		});
	}
	Ok(vec![define(
		&name,
		UserTypeBody::Composite(CompositeBody {
			fields,
		}),
		DefineKind::Default,
	)])
}

/// The removal of the user type `name` of `type_kind`. `CASCADE`, which would also drop the
/// columns of the type, is refused.
pub(crate) fn drop_user_type(
	name: &str,
	type_kind: UserTypeKind,
	if_exists: bool,
	cascade: bool,
) -> Result<Expr, PlanError> {
	if cascade {
		return Err(PlanError::unsupported(match type_kind {
			UserTypeKind::Domain => "DROP DOMAIN ... CASCADE",
			UserTypeKind::Composite => "DROP TYPE ... CASCADE of a composite type",
		}));
	}
	Ok(Expr::Remove(Box::new(RemoveStatement::UserType(RemoveUserTypeStatement {
		name: Strand::new(name),
		type_kind,
		if_exists,
	}))))
}
