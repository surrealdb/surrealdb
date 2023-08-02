use crate::idx::ft::analyzer::Analyzers;
use crate::sql::comment::{mightbespace, shouldbespace};
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use crate::sql::scoring::{scoring, Scoring};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::u16 as uint16;
use nom::character::complete::u32 as uint32;
use nom::combinator::{map, opt};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Index {
	/// (Basic) non unique
	Idx,
	/// Unique index
	Uniq,
	/// Index with Full-Text search capabilities
	Search(SearchParams),
	/// M-Tree index for distance based metrics
	MTree(MTreeParams),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct SearchParams {
	pub az: Ident,
	pub hl: bool,
	pub sc: Scoring,
	pub doc_ids_order: u32,
	pub doc_lengths_order: u32,
	pub postings_order: u32,
	pub terms_order: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct MTreeParams {
	pub dimension: u16,
	pub vector_type: VectorType,
	pub capacity: u16,
	pub doc_ids_order: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum VectorType {
	I64,
	F64,
	U32,
	I32,
	F32,
	U16,
	I16,
}

impl Display for VectorType {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			VectorType::I64 => f.write_str("I64"),
			VectorType::F64 => f.write_str("F64"),
			VectorType::U32 => f.write_str("U32"),
			VectorType::I32 => f.write_str("I32"),
			VectorType::F32 => f.write_str("F32"),
			VectorType::U16 => f.write_str("U16"),
			VectorType::I16 => f.write_str("I16"),
		}
	}
}

impl Default for Index {
	fn default() -> Self {
		Self::Idx
	}
}

impl Display for Index {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Idx => Ok(()),
			Self::Uniq => f.write_str("UNIQUE"),
			Self::Search(p) => {
				write!(
					f,
					"SEARCH ANALYZER {} {} DOC_IDS_ORDER {} DOC_LENGTHS_ORDER {} POSTINGS_ORDER {} TERMS_ORDER {}",
					p.az,
					p.sc,
					p.doc_ids_order,
					p.doc_lengths_order,
					p.postings_order,
					p.terms_order
				)?;
				if p.hl {
					f.write_str(" HIGHLIGHTS")?
				}
				Ok(())
			}
			Self::MTree(p) => {
				write!(
					f,
					"MTREE DIMENSION {} TYPE {} CAPACITY {} DOC_IDS_ORDER {}",
					p.dimension, p.vector_type, p.capacity, p.doc_ids_order
				)
			}
		}
	}
}

pub fn index(i: &str) -> IResult<&str, Index> {
	alt((unique, search, mtree, non_unique))(i)
}

pub fn non_unique(i: &str) -> IResult<&str, Index> {
	let (i, _) = tag("")(i)?;
	Ok((i, Index::Idx))
}

pub fn unique(i: &str) -> IResult<&str, Index> {
	let (i, _) = tag_no_case("UNIQUE")(i)?;
	Ok((i, Index::Uniq))
}

pub fn analyzer(i: &str) -> IResult<&str, Ident> {
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag_no_case("ANALYZER")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, analyzer) = ident(i)?;
	Ok((i, analyzer))
}

fn order<'a>(label: &'static str, i: &'a str) -> IResult<&'a str, u32> {
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag_no_case(label)(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, order) = uint32(i)?;
	Ok((i, order))
}

pub fn doc_ids_order(i: &str) -> IResult<&str, u32> {
	order("DOC_IDS_ORDER", i)
}

pub fn doc_lengths_order(i: &str) -> IResult<&str, u32> {
	order("DOC_LENGTHS_ORDER", i)
}

pub fn postings_order(i: &str) -> IResult<&str, u32> {
	order("POSTINGS_ORDER", i)
}

pub fn terms_order(i: &str) -> IResult<&str, u32> {
	order("TERMS_ORDER", i)
}

pub fn highlights(i: &str) -> IResult<&str, bool> {
	let (i, _) = mightbespace(i)?;
	alt((map(tag("HIGHLIGHTS"), |_| true), map(tag(""), |_| false)))(i)
}

pub fn search(i: &str) -> IResult<&str, Index> {
	let (i, _) = tag_no_case("SEARCH")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, az) = opt(analyzer)(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, sc) = scoring(i)?;
	let (i, o1) = opt(doc_ids_order)(i)?;
	let (i, o2) = opt(doc_lengths_order)(i)?;
	let (i, o3) = opt(postings_order)(i)?;
	let (i, o4) = opt(terms_order)(i)?;
	let (i, hl) = highlights(i)?;
	Ok((
		i,
		Index::Search(SearchParams {
			az: az.unwrap_or_else(|| Ident::from(Analyzers::LIKE)),
			sc,
			hl,
			doc_ids_order: o1.unwrap_or(100),
			doc_lengths_order: o2.unwrap_or(100),
			postings_order: o3.unwrap_or(100),
			terms_order: o4.unwrap_or(100),
		}),
	))
}

pub fn vector_type(i: &str) -> IResult<&str, VectorType> {
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag_no_case("TYPE")(i)?;
	alt((
		map(tag_no_case("I64"), |_| VectorType::I64),
		map(tag_no_case("F64"), |_| VectorType::F64),
		map(tag_no_case("U32"), |_| VectorType::U32),
		map(tag_no_case("I32"), |_| VectorType::I32),
		map(tag_no_case("F32"), |_| VectorType::F32),
		map(tag_no_case("U16"), |_| VectorType::U16),
		map(tag_no_case("I16"), |_| VectorType::I16),
	))(i)
}

pub fn dimension(i: &str) -> IResult<&str, u16> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("DIMENSION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, dim) = uint16(i)?;
	Ok((i, dim))
}

pub fn capacity(i: &str) -> IResult<&str, u16> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("CAPACITY")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, capacity) = uint16(i)?;
	Ok((i, capacity))
}

pub fn mtree(i: &str) -> IResult<&str, Index> {
	let (i, _) = tag_no_case("MTREE")(i)?;
	let (i, dimension) = dimension(i)?;
	let (i, vector_type) = opt(vector_type)(i)?;
	let (i, capacity) = opt(capacity)(i)?;
	let (i, doc_ids_order) = opt(doc_ids_order)(i)?;
	Ok((
		i,
		Index::MTree(MTreeParams {
			dimension,
			vector_type: vector_type.unwrap_or(VectorType::F64),
			capacity: capacity.unwrap_or(40),
			doc_ids_order: doc_ids_order.unwrap_or(100),
		}),
	))
}
