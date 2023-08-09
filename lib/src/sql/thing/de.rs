use std::fmt;

use crate::sql::Id;

use super::Thing;

use serde::de::{self, MapAccess, Visitor};
use serde::Deserialize;

struct ThingVisitor;

impl<'de> Visitor<'de> for ThingVisitor {
	type Value = Thing;

	fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
		write!(formatter, "either a string following the format \"table:id\", an object of structure {{ tb: \"table\", id: \"id\" }}, or a tuple of type (String, Id)")
	}

	fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
	where
		A: de::SeqAccess<'de>,
	{
		let tb =
			seq.next_element::<String>()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
		let id = seq.next_element::<Id>()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;

		Ok(Thing {
			tb,
			id,
		})
	}

	fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
	where
		V: MapAccess<'de>,
	{
		let mut tb = None;
		let mut id = None;

		while let Some(k) = map.next_key::<&str>()? {
			match k {
				"tb" => {
					if tb.is_some() {
						return Err(de::Error::duplicate_field("tb"));
					}

					tb = Some(map.next_value::<String>()?);
				}

				"id" => {
					if id.is_some() {
						return Err(de::Error::duplicate_field("id"));
					}

					id = Some(map.next_value::<Id>()?);
				}

				_ => {}
			}
		}

		let tb: String = tb.ok_or_else(|| de::Error::missing_field("tb"))?;
		let id: Id = id.ok_or_else(|| de::Error::missing_field("id"))?;

		Ok(Thing {
			tb,
			id,
		})
	}

	fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
	where
		E: de::Error,
	{
		// "table:id"
		// requires a `:` to split at
		if !v.contains(':') {
			return Err(de::Error::custom("str type does not contain character ':'"));
		}

		// safety: we have already check for the existence of the ':'
		//         as such this cannot fail
		let (tb, id) = v.split_once(':').unwrap();

		Ok(Thing {
			tb: tb.to_owned(),
			id: Id::from(id),
		})
	}
}

impl<'de> Deserialize<'de> for Thing {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		// this can come in many types
		//
		// as such this can be considered necessary for better interop
		//
		// it does however, require the use of a structured / descriptive
		// serialization method
		deserializer.deserialize_any(ThingVisitor)
	}
}
