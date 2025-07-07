mod de;
mod ser;

pub(crate) const VALUE_TOKEN: &str = "$surrealdb::private::sql::Value";
pub(crate) const REGEX_TOKEN: &str = "$surrealdb::private::sql::Regex";
pub(crate) const RANGE_TOKEN: &str = "$surrealdb::private::sql::Range";
pub(crate) const STRAND_TOKEN: &str = "$surrealdb::private::sql::Strand";
pub(crate) const THING_TOKEN: &str = "$surrealdb::private::sql::Thing";
pub(crate) const TABLE_TOKEN: &str = "$surrealdb::private::sql::Table";
pub(crate) const ARRAY_TOKEN: &str = "$surrealdb::private::sql::Array";
pub(crate) const OBJECT_TOKEN: &str = "$surrealdb::private::sql::Object";
pub(crate) const UUID_TOKEN: &str = "$surrealdb::private::sql::Uuid";
pub(crate) const DATETIME_TOKEN: &str = "$surrealdb::private::sql::Datetime";
pub(crate) const DURATION_TOKEN: &str = "$surrealdb::private::sql::Duration";
pub(crate) const CLOSURE_TOKEN: &str = "$surrealdb::private::sql::Closure";
pub(crate) const FILE_TOKEN: &str = "$surrealdb::private::sql::File";
pub(crate) const GEOMETRY_TOKEN: &str = "$surrealdb::private::sql::Geometry";
pub(crate) const NUMBER_TOKEN: &str = "$surrealdb::private::sql::Number";

pub use de::from_value;
pub use ser::to_value;
