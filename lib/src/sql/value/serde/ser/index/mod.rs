use crate::err::Error;
use crate::sql::index::{Index, SearchParams};
use crate::sql::scoring::Scoring;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Index;
	type Error = Error;

	type SerializeSeq = Impossible<Index, Error>;
	type SerializeTuple = Impossible<Index, Error>;
	type SerializeTupleStruct = Impossible<Index, Error>;
	type SerializeTupleVariant = Impossible<Index, Error>;
	type SerializeMap = Impossible<Index, Error>;
	type SerializeStruct = Impossible<Index, Error>;
	type SerializeStructVariant = SerializeIndex;

	const EXPECTED: &'static str = "an enum `Index`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"Idx" => Ok(Index::Idx),
			"Uniq" => Ok(Index::Uniq),
			variant => Err(Error::custom(format!("unexpected unit variant `{name}::{variant}`"))),
		}
	}

	#[inline]
	fn serialize_struct_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStructVariant, Self::Error> {
		match (name, variant) {
			("Index", "Search") => Ok(SerializeIndex::Search(Default::default())),
			_ => Err(Error::custom(format!("unexpected `{name}::{variant}`"))),
		}
	}
}

pub(super) enum SerializeIndex {
	Search(SerializeSearch),
}

impl serde::ser::SerializeStructVariant for SerializeIndex {
	type Ok = Index;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match self {
			Self::Search(search) => search.serialize_field(key, value),
		}
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match self {
			Self::Search(search) => search.end(),
		}
	}
}

#[derive(Default)]
pub(super) struct SerializeSearch {
	az: Ident,
	hl: bool,
	sc: Option<Scoring>,
	doc_ids_order: u32,
	doc_lengths_order: u32,
	postings_order: u32,
	terms_order: u32,
}

impl serde::ser::SerializeStructVariant for SerializeSearch {
	type Ok = Index;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"az" => {
				self.az = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"hl" => {
				self.hl = value.serialize(ser::primitive::bool::Serializer.wrap())?;
			}
			"sc" => {
				self.sc = Some(value.serialize(ser::scoring::Serializer.wrap())?);
			}
			"doc_ids_order" => {
				self.doc_ids_order = value.serialize(ser::primitive::u32::Serializer.wrap())?;
			}
			"doc_lengths_order" => {
				self.doc_lengths_order = value.serialize(ser::primitive::u32::Serializer.wrap())?;
			}
			"postings_order" => {
				self.postings_order = value.serialize(ser::primitive::u32::Serializer.wrap())?;
			}
			"terms_order" => {
				self.terms_order = value.serialize(ser::primitive::u32::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `Index::Search {{ {key} }}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match self.sc {
			Some(sc) => Ok(Index::Search(SearchParams {
				az: self.az,
				hl: self.hl,
				sc,
				doc_ids_order: self.doc_ids_order,
				doc_lengths_order: self.doc_lengths_order,
				postings_order: self.postings_order,
				terms_order: self.terms_order,
			})),
			_ => Err(Error::custom("`Index::Search` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn idx() {
		let idx = Index::Idx;
		let serialized = idx.serialize(Serializer.wrap()).unwrap();
		assert_eq!(idx, serialized);
	}

	#[test]
	fn uniq() {
		let idx = Index::Uniq;
		let serialized = idx.serialize(Serializer.wrap()).unwrap();
		assert_eq!(idx, serialized);
	}

	#[test]
	fn search() {
		let idx = Index::Search(SearchParams {
			az: Default::default(),
			hl: Default::default(),
			sc: Scoring::Bm {
				k1: Default::default(),
				b: Default::default(),
			},
			doc_ids_order: Default::default(),
			doc_lengths_order: Default::default(),
			postings_order: Default::default(),
			terms_order: Default::default(),
		});
		let serialized = idx.serialize(Serializer.wrap()).unwrap();
		assert_eq!(idx, serialized);
	}
}
