//! Builds a [`FieldPath`] from an idiom, rejecting the shapes the streaming
//! operators cannot express (conditions, method calls).

use surrealdb_types::ToSql;

use crate::err::Error;
use crate::exec::Error as ExecError;
use crate::expr::part::Part;
use crate::expr::{Expr, Idiom, Literal};
use crate::val::field_path::{FieldPath, FieldPathPart};

/// Builds a [`FieldPath`] from an idiom, rejecting shapes the streaming
/// operators cannot express.
pub(crate) fn field_path_from_idiom(idiom: &Idiom) -> Result<FieldPath, Error> {
	let mut parts = Vec::with_capacity(idiom.len());
	for part in idiom.iter() {
		match part {
			Part::Field(name) => parts.push(FieldPathPart::Field(name.as_str().to_owned())),
			Part::First => parts.push(FieldPathPart::First),
			Part::Last => parts.push(FieldPathPart::Last),
			Part::Value(Expr::Literal(Literal::Integer(i))) if *i >= 0 => {
				parts.push(FieldPathPart::Index(*i as usize))
			}
			Part::Lookup(lookup) => {
				// Graph traversal key like "->table" - convert to string representation
				parts.push(FieldPathPart::Lookup(lookup.to_sql()))
			}
			// Skip parts that don't affect output path structure
			Part::Destructure(_) | Part::Start(_) => {}
			_ => {
				return Err(ExecError::Query {
					message: format!(
						"FieldPath cannot contain complex parts like where clauses or method calls. \
			 Only simple field access (a.b.c), literal indices ([0], [$]), and graph traversals are supported. \
			 Got: {:?}",
						idiom
					),
				}
				.into());
			}
		}
	}
	Ok(FieldPath(parts))
}
