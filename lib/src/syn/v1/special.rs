use super::ParseError;
use crate::sql::{Field, Fields, Groups, Idiom, Orders, Splits, Value};
use nom::Err;
use nom::Err::Failure;

/// Check to see whether the expression is in the SELECT clause
fn contains_idiom(fields: &Fields, idiom: &Idiom) -> bool {
	fields.iter().any(|field| {
		match field {
			// There is a SELECT * expression, so presume everything is ok
			Field::All => true,
			// Check each field
			Field::Single {
				expr,
				alias,
			} => {
				if let Some(i) = alias {
					// This field is aliased, so check the alias name
					i.as_ref() == idiom.as_ref()
				} else {
					// This field is not aliased, so check the field value
					match expr {
						// Use raw idiom (TODO: should this use `simplify`?)
						Value::Idiom(i) => i.as_ref() == idiom.as_ref(),
						// Check the expression
						v => v.to_idiom().as_ref() == idiom.as_ref(),
					}
				}
			}
		}
	})
}

pub fn check_split_on_fields<'a>(
	i: &'a str,
	fields: &Fields,
	splits: &Option<Splits>,
) -> Result<(), Err<ParseError<&'a str>>> {
	// Check to see if a SPLIT ON clause has been defined
	if let Some(splits) = splits {
		// Loop over each of the expressions in the SPLIT ON clause
		for split in splits.iter() {
			if !contains_idiom(fields, &split.0) {
				// If the expression isn't specified in the SELECT clause, then error
				return Err(Failure(ParseError::Split(i, split.to_string())));
			}
		}
	}
	// This query is ok to run
	Ok(())
}

pub fn check_order_by_fields<'a>(
	i: &'a str,
	fields: &Fields,
	orders: &Option<Orders>,
) -> Result<(), Err<ParseError<&'a str>>> {
	// Check to see if a ORDER BY clause has been defined
	if let Some(orders) = orders {
		// Loop over each of the expressions in the ORDER BY clause
		for order in orders.iter() {
			if order.random {
				// don't check for a field if the order is random.
				continue;
			}
			if !contains_idiom(fields, order) {
				// If the expression isn't specified in the SELECT clause, then error
				return Err(Failure(ParseError::Order(i, order.to_string())));
			}
		}
	}
	// This query is ok to run
	Ok(())
}

pub fn check_group_by_fields<'a>(
	i: &'a str,
	fields: &Fields,
	groups: &Option<Groups>,
) -> Result<(), Err<ParseError<&'a str>>> {
	// Check to see if a GROUP BY clause has been defined
	if let Some(groups) = groups {
		// Loop over each of the expressions in the GROUP BY clause
		for group in groups.iter() {
			if !contains_idiom(fields, &group.0) {
				// If the expression isn't specified in the SELECT clause, then error
				return Err(Failure(ParseError::Group(i, group.to_string())));
			}
		}
		// Check if this is a GROUP ALL clause or a GROUP BY clause
		if !groups.is_empty() {
			// Loop over each of the expressions in the SELECT clause
			'outer: for field in fields.iter() {
				// Loop over each of the expressions in the GROUP BY clause
				for group in groups.iter() {
					// Check to see whether the expression is in the GROUP BY clause or is an aggregate
					if let Field::Single {
						expr,
						alias,
					} = field
					{
						if alias.as_ref().map(|i| i.as_ref() == group.as_ref()).unwrap_or(false) {
							// This field is aliased, and the alias name matched
							continue 'outer;
						} else {
							match expr {
								// If the expression in the SELECT clause is a field, check to see if it exists in the GROUP BY
								Value::Idiom(i) if i == &group.0 => continue 'outer,
								// If the expression in the SELECT clause is a function, check to see if it is an aggregate function
								Value::Function(f) if f.is_aggregate() => continue 'outer,
								// Otherwise check if the expression itself exists in the GROUP BY clause
								v if v.to_idiom() == group.0 => continue 'outer,
								// Check if this is a static value which can be used in the GROUP BY clause
								v if v.is_static() => continue 'outer,
								// If not, then this query should fail
								_ => (),
							}
						}
					}
				}
				// If the expression isn't an aggregate function and isn't specified in the GROUP BY clause, then error
				return Err(Failure(ParseError::Field(i, field.to_string())));
			}
		}
	}
	// This query is ok to run
	Ok(())
}
