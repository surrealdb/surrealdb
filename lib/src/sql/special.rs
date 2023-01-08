use crate::sql::error::Error;
use crate::sql::field::{Field, Fields};
use crate::sql::group::Groups;
use crate::sql::order::Orders;
use crate::sql::split::Splits;
use crate::sql::value::Value;
use nom::Err;
use nom::Err::Failure;

pub fn check_split_on_fields<'a>(
	i: &'a str,
	fields: &Fields,
	splits: &Option<Splits>,
) -> Result<(), Err<Error<&'a str>>> {
	// Check to see if a ORDER BY clause has been defined
	if let Some(splits) = splits {
		// Loop over each of the expressions in the SPLIT ON clause
		'outer: for split in splits.iter() {
			// Loop over each of the expressions in the SELECT clause
			for field in fields.iter() {
				// Check to see whether the expression is in the SELECT clause
				match field {
					// There is a SELECT * expression, so presume everything is ok
					Field::All => break 'outer,
					// This field is aliased, so check the alias name
					Field::Alias(_, i) if i.as_ref() == split.as_ref() => continue 'outer,
					// This field is not aliased, so check the field value
					Field::Alone(v) => {
						match v {
							// If the expression in the SELECT clause is a field, check if it exists in the SPLIT ON clause
							Value::Idiom(i) if i.as_ref() == split.as_ref() => continue 'outer,
							// Otherwise check if the expression itself exists in the SPLIT ON clause
							v if v.to_idiom().as_ref() == split.as_ref() => continue 'outer,
							// If not, then this query should fail
							_ => (),
						}
					}
					// If not, then this query should fail
					_ => (),
				}
			}
			// If the expression isn't specified in the SELECT clause, then error
			return Err(Failure(Error::Split(i, split.to_string())));
		}
	}
	// This query is ok to run
	Ok(())
}

pub fn check_order_by_fields<'a>(
	i: &'a str,
	fields: &Fields,
	orders: &Option<Orders>,
) -> Result<(), Err<Error<&'a str>>> {
	// Check to see if a ORDER BY clause has been defined
	if let Some(orders) = orders {
		// Loop over each of the expressions in the ORDER BY clause
		'outer: for order in orders.iter() {
			// Loop over each of the expressions in the SELECT clause
			for field in fields.iter() {
				// Check to see whether the expression is in the SELECT clause
				match field {
					// There is a SELECT * expression, so presume everything is ok
					Field::All => break 'outer,
					// This field is aliased, so check the alias name
					Field::Alias(_, i) if i.as_ref() == order.as_ref() => continue 'outer,
					// This field is not aliased, so check the field value
					Field::Alone(v) => {
						match v {
							// If the expression in the SELECT clause is a field, check if it exists in the ORDER BY clause
							Value::Idiom(i) if i.as_ref() == order.as_ref() => continue 'outer,
							// Otherwise check if the expression itself exists in the ORDER BY clause
							v if v.to_idiom().as_ref() == order.as_ref() => continue 'outer,
							// If not, then this query should fail
							_ => (),
						}
					}
					// If not, then this query should fail
					_ => (),
				}
			}
			// If the expression isn't specified in the SELECT clause, then error
			return Err(Failure(Error::Order(i, order.to_string())));
		}
	}
	// This query is ok to run
	Ok(())
}

pub fn check_group_by_fields<'a>(
	i: &'a str,
	fields: &Fields,
	groups: &Option<Groups>,
) -> Result<(), Err<Error<&'a str>>> {
	// Check to see if a GROUP BY clause has been defined
	if let Some(groups) = groups {
		// Loop over each of the expressions in the GROUP BY clause
		'outer: for group in groups.iter() {
			// Loop over each of the expressions in the SELECT clause
			for field in fields.iter() {
				// Check to see whether the expression is in the SELECT clause
				match field {
					// This field is aliased, so check the alias name
					Field::Alias(_, i) if i.as_ref() == group.as_ref() => continue 'outer,
					// This field is not aliased, so check the field value
					Field::Alone(v) => {
						match v {
							// If the expression in the SELECT clause is a field, check if it exists in the GROUP BY clause
							Value::Idiom(i) if i.as_ref() == group.as_ref() => continue 'outer,
							// Otherwise check if the expression itself exists in the GROUP BY clause
							v if v.to_idiom().as_ref() == group.as_ref() => continue 'outer,
							// If not, then this query should fail
							_ => (),
						}
					}
					// If not, then this query should fail
					_ => (),
				}
			}
			// If the expression isn't specified in the SELECT clause, then error
			return Err(Failure(Error::Group(i, group.to_string())));
		}
		// Check if this is a GROUP ALL clause or a GROUP BY clause
		if groups.len() > 0 {
			// Loop over each of the expressions in the SELECT clause
			'outer: for field in fields.iter() {
				// Loop over each of the expressions in the GROUP BY clause
				for group in groups.iter() {
					// Check to see whether the expression is in the SELECT clause
					match field {
						// This field is aliased, so check the alias name
						Field::Alias(_, i) if i.as_ref() == group.as_ref() => continue 'outer,
						// Otherwise, check the type of the field value
						Field::Alias(v, _) | Field::Alone(v) => match v {
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
						},
						_ => (),
					}
				}
				// If the expression isn't an aggregate function and isn't specified in the GROUP BY clause, then error
				return Err(Failure(Error::Field(i, field.to_string())));
			}
		}
	}
	// This query is ok to run
	Ok(())
}
