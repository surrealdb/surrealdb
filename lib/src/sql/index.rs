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
	Search {
		az: Ident,
		hl: bool,
		sc: Scoring,
		order: u32,
	},
	BallTree {
		dimension: u16,
		vector_type: VectorType,
		bucket_size: u16,
		doc_ids_order: u32,
	},
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
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Idx => Ok(()),
			Self::Uniq => f.write_str("UNIQUE"),
			Self::Search {
				az,
				hl,
				sc,
				order,
			} => {
				write!(f, "SEARCH ANALYZER {} {} ORDER {}", az, sc, order)?;
				if *hl {
					f.write_str(" HIGHLIGHTS")?
				}
				Ok(())
			}
			Self::BallTree {
				dimension,
				vector_type,
				bucket_size,
				doc_ids_order,
			} => {
				write!(
					f,
					"BALLTREE DIMENSION {} TYPE {} BUCKET_SIZE {} DOCIDS_ORDER {}",
					dimension, vector_type, bucket_size, doc_ids_order
				)
			}
		}
	}
}

pub fn index(i: &str) -> IResult<&str, Index> {
	alt((unique, search, ball_tree, non_unique))(i)
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

pub fn order(i: &str) -> IResult<&str, u32> {
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag_no_case("ORDER")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, order) = uint32(i)?;
	Ok((i, order))
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
	let (i, o) = opt(order)(i)?;
	let (i, hl) = highlights(i)?;
	Ok((
		i,
		Index::Search {
			az: az.unwrap_or_else(|| Ident::from(Analyzers::LIKE)),
			sc,
			hl,
			order: o.unwrap_or(100),
		},
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

pub fn bucket_size(i: &str) -> IResult<&str, u16> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("BUCKET_SIZE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, bucket_size) = uint16(i)?;
	Ok((i, bucket_size))
}

pub fn doc_ids_order(i: &str) -> IResult<&str, u32> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("DOCIDS_ORDER")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, order) = uint32(i)?;
	Ok((i, order))
}

pub fn ball_tree(i: &str) -> IResult<&str, Index> {
	let (i, _) = tag_no_case("BALLTREE")(i)?;
	let (i, dimension) = dimension(i)?;
	let (i, vector_type) = opt(vector_type)(i)?;
	let (i, bucket_size) = opt(bucket_size)(i)?;
	let (i, doc_ids_order) = opt(doc_ids_order)(i)?;
	Ok((
		i,
		Index::BallTree {
			dimension,
			vector_type: vector_type.unwrap_or(VectorType::F64),
			bucket_size: bucket_size.unwrap_or(40),
			doc_ids_order: doc_ids_order.unwrap_or(100),
		},
	))
}
