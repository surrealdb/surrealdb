use crate::err::Error;
use crate::sql::index::SearchParams;
use crate::sql::value::serde::ser;
use crate::sql::{Ident, Scoring};
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = SearchParams;
	type Error = Error;

	type SerializeSeq = Impossible<SearchParams, Error>;
	type SerializeTuple = Impossible<SearchParams, Error>;
	type SerializeTupleStruct = Impossible<SearchParams, Error>;
	type SerializeTupleVariant = Impossible<SearchParams, Error>;
	type SerializeMap = Impossible<SearchParams, Error>;
	type SerializeStruct = SerializeSearch;
	type SerializeStructVariant = Impossible<SearchParams, Error>;

	const EXPECTED: &'static str = "a struct `SearchParams`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeSearch::default())
	}

	#[inline]
	fn serialize_newtype_struct<T>(
		self,
		_name: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(self.wrap())
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
	doc_ids_cache: u32,
	doc_lengths_cache: u32,
	postings_cache: u32,
	terms_cache: u32,
}

impl serde::ser::SerializeStruct for SerializeSearch {
	type Ok = SearchParams;
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
			"doc_ids_cache" => {
				self.doc_ids_cache = value.serialize(ser::primitive::u32::Serializer.wrap())?;
			}
			"doc_lengths_cache" => {
				self.doc_lengths_cache = value.serialize(ser::primitive::u32::Serializer.wrap())?;
			}
			"postings_cache" => {
				self.postings_cache = value.serialize(ser::primitive::u32::Serializer.wrap())?;
			}
			"terms_cache" => {
				self.terms_cache = value.serialize(ser::primitive::u32::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `SearchParams {{ {key} }}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match self.sc {
			Some(sc) => Ok(SearchParams {
				az: self.az,
				hl: self.hl,
				sc,
				doc_ids_order: self.doc_ids_order,
				doc_lengths_order: self.doc_lengths_order,
				postings_order: self.postings_order,
				terms_order: self.terms_order,
				doc_ids_cache: self.doc_ids_cache,
				doc_lengths_cache: self.doc_lengths_cache,
				postings_cache: self.postings_cache,
				terms_cache: self.terms_cache,
			}),
			_ => Err(Error::custom("`SearchParams` missing required field(s)")),
		}
	}
}

#[test]
fn search_params() {
	let params = SearchParams {
		az: Default::default(),
		hl: false,
		sc: Scoring::Vs,
		doc_ids_order: 1,
		doc_lengths_order: 2,
		postings_order: 3,
		terms_order: 4,
		doc_ids_cache: 5,
		doc_lengths_cache: 6,
		postings_cache: 7,
		terms_cache: 8,
	};
	let serialized = params.serialize(Serializer.wrap()).unwrap();
	assert_eq!(params, serialized);
}
