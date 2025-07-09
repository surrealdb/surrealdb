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

impl Serialize for Regex {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_newtype_struct(REGEX_TOKEN, self.0.as_str())
	}
}

impl<'de> Deserialize<'de> for Regex {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct RegexNewtypeVisitor;

		impl<'de> Visitor<'de> for RegexNewtypeVisitor {
			type Value = Regex;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("a regex newtype")
			}

			fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
			where
				D: Deserializer<'de>,
			{
				struct RegexVisitor;

				impl Visitor<'_> for RegexVisitor {
					type Value = Regex;

					fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
						formatter.write_str("a regex str")
					}

					fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
					where
						E: de::Error,
					{
						Regex::from_str(value).map_err(|_| de::Error::custom("invalid regex"))
					}
				}

				deserializer.deserialize_str(RegexVisitor)
			}
		}

		deserializer.deserialize_newtype_struct(REGEX_TOKEN, RegexNewtypeVisitor)
	}
}
