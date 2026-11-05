//! The order of `inet` and `cidr` values, as PostgreSQL sorts them: by address family, then by
//! the address bits both prefixes cover, then by prefix length, then by the whole address.
//!
//! A network value is stored as its canonical text, which compares by spelling. A comparison, a
//! `BETWEEN` and an `ORDER BY` over a network operand compare `sql::network_key(value)` on both
//! sides instead, a key whose order is PostgreSQL's. The operand opposite a network is keyed by
//! the same function, so a text literal compares as the network it reads as, and text that does
//! not read as a network is refused as PostgreSQL refuses it.

use surrealdb_expr::expr::Function;
use surrealdb_expr::expr::logical::{LogicalCall, LogicalExpr, LogicalLiteral};
use surrealdb_expr::expr::statements::define::SqlTypeDialect;
use surrealdb_expr::expr::statements::define::sql_types::text_shaped::network;
use surrealdb_strand::Strand;

use crate::SqlType;
use crate::error::PlanError;
use crate::ir::{BoundExpr, BoundLiteral};

/// The network type of `expr`: a column declared `inet` or `cidr`, by its type, by its `SQL
/// TYPE`, or by the base type of the domain its `SQL TYPE` names, a cast to one, or a `COALESCE`,
/// `NULLIF`, `GREATEST`, `LEAST` or `CASE` one of whose results is, of their [common network
/// type](common_network_type). `None` for any other expression.
fn network_type(expr: &BoundExpr) -> Option<SqlType> {
	let ty = if let BoundExpr::Column(col) = expr {
		match col.declared_sql.as_deref() {
			// A declared type that is not a built-in one names a domain, whose column the
			// catalog types by the domain's base type.
			Some(declared)
				if declared.dialect() == SqlTypeDialect::PostgreSql
					&& declared.array_dimensions().is_empty()
					&& SqlType::from_name(declared.base()) != SqlType::Unknown =>
			{
				SqlType::from_name(declared.base())
			}
			Some(_) | None => col.declared_type,
		}
	} else if let BoundExpr::Cast {
		target_type,
		..
	} = expr
	{
		SqlType::from_name(target_type)
	} else if let BoundExpr::FunctionCall {
		name,
		args,
		..
	} = expr
		&& matches!(name.as_str(), "coalesce" | "nullif" | "greatest" | "least")
	{
		return common_network_type(args.iter());
	} else if let BoundExpr::Case {
		branches,
		else_expr,
		..
	} = expr
	{
		return common_network_type(
			branches.iter().map(|(_, result)| result).chain(else_expr.as_deref()),
		);
	} else {
		return None;
	};
	matches!(ty, SqlType::Inet | SqlType::Cidr).then_some(ty)
}

/// The type PostgreSQL resolves a set of results to when they are networks: `inet` when any is an
/// `inet`, as a `cidr` converts to `inet` implicitly, and `cidr` when every one is a `cidr`.
/// A string literal, `NULL` or parameter takes the network type. `None` when no result is a
/// network, or when one is of a type of its own that is not a network, which PostgreSQL resolves
/// to no common type.
fn common_network_type<'e>(results: impl Iterator<Item = &'e BoundExpr>) -> Option<SqlType> {
	let mut common = None;
	for result in results {
		if crate::lower::expr::is_untyped(result) {
			continue;
		}
		let ty = network_type(result)?;
		common = Some(match common {
			Some(SqlType::Inet) => SqlType::Inet,
			Some(_) | None => ty,
		});
	}
	common
}

/// The expression among `exprs` whose network type is their [common network
/// type](common_network_type): an `inet` one when any is, otherwise the first network. `None`
/// when none is a network.
pub(crate) fn common_network<'e>(
	exprs: impl Iterator<Item = &'e BoundExpr> + Clone,
) -> Option<&'e BoundExpr> {
	let mut networks = exprs.filter(|expr| network_type(expr).is_some());
	networks
		.clone()
		.find(|expr| network_type(expr) == Some(SqlType::Inet))
		.or_else(|| networks.next())
}

/// The name of the network type of `expr`, `inet` or `cidr`, as [`network_type`] reads it.
pub(crate) fn network_type_name(expr: &BoundExpr) -> Option<&'static str> {
	network_type(expr).map(|ty| {
		if ty == SqlType::Cidr {
			"cidr"
		} else {
			"inet"
		}
	})
}

/// Whether `expr` is a network value, as [`network_type`] reads it.
pub(crate) fn is_network(expr: &BoundExpr) -> bool {
	network_type(expr).is_some()
}

/// `sql::network_key(value)`: the key `value` sorts by as a network.
pub(crate) fn key(value: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal("sql::network_key".to_owned()),
		args: vec![value],
	}))
}

/// `sql::network_text(value, 'inet' | 'cidr')` of a parameter `operand` compared with `network`,
/// so the value bound to it is read as the network it spells, as a literal is by
/// [`canonical_literal`]. `None` when `network` is not a network value or `operand` is not a
/// parameter.
pub(crate) fn canonical_param(
	network: &BoundExpr,
	operand: &BoundExpr,
	lowered: &LogicalExpr,
) -> Option<LogicalExpr> {
	let ty = network_type(network)?;
	if !matches!(operand, BoundExpr::Param { .. }) {
		return None;
	}
	let ty = if ty == SqlType::Cidr {
		"cidr"
	} else {
		"inet"
	};
	Some(LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal("sql::network_text".to_owned()),
		args: vec![lowered.clone(), LogicalExpr::Literal(LogicalLiteral::String(Strand::from(ty)))],
	})))
}

/// The canonical text of the network a string literal `operand` reads as, where `network` is a
/// network value: an `inet` keeps its host bits and drops a full-length prefix, and a `cidr`
/// always writes its prefix. `None` when `network` is not a network value or `operand` is not a
/// string literal. Text that does not read as a network, or a `cidr` with a host bit set to the
/// right of its prefix, is refused, as PostgreSQL refuses the literal as input to the type.
pub(crate) fn canonical_literal(
	network: &BoundExpr,
	operand: &BoundExpr,
) -> Option<Result<LogicalExpr, PlanError>> {
	let ty = network_type(network)?;
	let BoundExpr::Literal(BoundLiteral::String(text)) = operand else {
		return None;
	};
	let (name, cidr) = if ty == SqlType::Cidr {
		("cidr", true)
	} else {
		("inet", false)
	};
	let parsed = if cidr {
		network::parse_cidr(text)
	} else {
		network::parse(text)
	};
	Some(match parsed {
		Ok(parsed) => {
			let canonical = if cidr {
				parsed.cidr_text()
			} else {
				parsed.inet_text()
			};
			Ok(LogicalExpr::Literal(LogicalLiteral::String(Strand::from(canonical.as_str()))))
		}
		Err(_) => Err(PlanError::InvalidTextRepresentation(format!("{name}: \"{text}\""))),
	})
}
