use bitflags::bitflags;
use derive::Store;
use revision::{revisioned, Revisioned};
use serde::{Deserialize, Serialize};
use std::fmt;

// TODO : use a CustomBits type? https://github.com/bitflags/bitflags/blob/main/examples/custom_bits_type.rs

bitflags! {
	#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Store, Hash)]
	#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
    pub struct LiveFilters: u8 {
        const Create = 0b001;
        const Update = 0b010;
        const Delete = 0b100;
    }
}

impl Serialize for LiveFilters {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		serializer.serialize_u8(self.bits())
	}
}

impl<'de> Deserialize<'de> for LiveFilters {
    fn deserialize<D>(deserializer: D) -> Result<LiveFilters, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
		let bits = u8::deserialize(deserializer)?;
        LiveFilters::from_bits(bits).ok_or(serde::de::Error::custom("Invalid bits"))
    }
}

impl Revisioned for LiveFilters {
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> std::result::Result<(), revision::Error> {
		self.bits().serialize_revisioned(writer)
	}

	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(
		reader: &mut R,
	) -> std::result::Result<Self, revision::Error> {
		let bits = u8::deserialize_revisioned(reader)?;
		LiveFilters::from_bits(bits).ok_or(revision::Error::InvalidBoolValue(bits))
	}

	fn revision() -> u16 {
		1
	}
}

impl fmt::Display for LiveFilters {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "<")?;
		
		let mut first = true;
		if self.contains(LiveFilters::Create) {
			write!(f, "CREATE")?;
			first = false;
		}
		if self.contains(LiveFilters::Update) {
			if !first {
				write!(f, " | ")?;
			}
			write!(f, "UPDATE")?;
			first = false;
		}
		if self.contains(LiveFilters::Delete) {
			if !first {
				write!(f, " | ")?;
			}
			write!(f, "DELETE")?;
		}

		write!(f, ">")?;
		Ok(())
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Live {
	// TODO: optimisation this should probably be a &str
	/// The namespace in which this LIVE query exists
	pub ns: String,
	// TODO: optimisation this should probably be a &str
	/// The database in which this LIVE query exists
	pub db: String,
	// TODO: optimisation this should probably be a &str
	/// The table in which this LIVE query exists
	pub tb: String,
	/// The filters applied to this LIVE query (Create, Update, Delete) 
	pub filters: Option<LiveFilters>,
}
