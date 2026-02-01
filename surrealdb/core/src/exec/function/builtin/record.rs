//! Record functions

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

define_pure_function!(RecordId, "record::id", (record: Any) -> Any, crate::fnc::record::id);
define_pure_function!(RecordTb, "record::tb", (record: Any) -> String, crate::fnc::record::tb);
define_pure_function!(RecordTable, "record::table", (record: Any) -> String, crate::fnc::record::tb);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, RecordId, RecordTb, RecordTable);
}
