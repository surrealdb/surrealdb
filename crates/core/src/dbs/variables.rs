/*
pub fn sql_variables_to_expr_variables(
	variables: &BTreeMap<String, crate::val::Value>,
) -> BTreeMap<String, crate::expr::Value> {
	let mut expr_variables = BTreeMap::new();
	for (key, val) in variables {
		expr_variables.insert(key.clone(), val.clone().into());
	}
	expr_variables
}

pub fn expr_variables_to_sql_variables(
	variables: &BTreeMap<String, crate::expr::Value>,
) -> BTreeMap<String, crate::sql::SqlValue> {
	let mut sql_variables = BTreeMap::new();
	for (key, val) in variables {
		sql_variables.insert(key.clone(), val.clone().into());
	}
	sql_variables
}
*/
