/// Map a PostgreSQL function name to its SurrealDB equivalent.
pub fn map_function_name(pg_name: &str) -> &str {
	match pg_name {
		// Aggregates
		"count" => "count",
		"sum" => "math::sum",
		"avg" => "math::mean",
		"min" => "math::min",
		"max" => "math::max",
		// String functions
		"length" | "char_length" | "character_length" => "string::len",
		"lower" => "string::lowercase",
		"upper" => "string::uppercase",
		"trim" => "string::trim",
		"concat" => "string::concat",
		"replace" => "string::replace",
		"substring" | "substr" => "string::slice",
		"reverse" => "string::reverse",
		"starts_with" => "string::starts_with",
		"split_part" => "string::split",
		// Math functions
		"abs" => "math::abs",
		"ceil" | "ceiling" => "math::ceil",
		"floor" => "math::floor",
		"round" => "math::round",
		"sqrt" => "math::sqrt",
		"power" | "pow" => "math::pow",
		"log" => "math::log",
		// Type functions
		"to_char" | "cast" => "type::string",
		// Date/time functions
		"now" => "time::now",
		// Array functions
		"array_length" => "array::len",
		"unnest" => "array::flatten",
		// JSON functions
		"json_build_object" => "object::from_entries",
		"jsonb_build_object" => "object::from_entries",
		"json_agg" | "jsonb_agg" => "array::group",
		// Misc
		"coalesce" => "type::coalesce",
		"random" => "rand",
		"gen_random_uuid" => "rand::uuid",
		// Pass through as-is for anything unmapped
		other => other,
	}
}
