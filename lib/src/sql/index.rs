use crate::idx::ft::analyzer::Analyzers;
use crate::sql::comment::{mightbespace, shouldbespace};
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use crate::sql::scoring::{scoring, Scoring};
use crate::sql::Number;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::u16 as uint16;
use nom::character::complete::u32 as uint32;
use nom::combinator::{cut, map, opt};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
pub enum Index {
	/// (Basic) non unique
	#[default]
	Idx,
	/// Unique index
	Uniq,
	/// Index with Full-Text search capabilities
	Search(SearchParams),
	/// M-Tree index for distance based metrics
	MTree(MTreeParams),
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
pub struct SearchParams {
	pub az: Ident,
	pub hl: bool,
	pub sc: Scoring,
	pub doc_ids_order: u32,
	pub doc_lengths_order: u32,
	pub postings_order: u32,
	pub terms_order: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
pub struct MTreeParams {
	pub dimension: u16,
	pub distance: Distance,
	pub capacity: u16,
	pub doc_ids_order: u32,
}

#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
pub enum Distance {
	#[default]
	Euclidean,
	Manhattan,
	Cosine,
	Hamming,
	Mahalanobis,
	Minkowski(Number),
}

impl Display for Distance {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			Self::Euclidean => f.write_str("EUCLIDEAN"),
			Self::Manhattan => f.write_str("MANHATTAN"),
			Self::Cosine => f.write_str("COSINE"),
			Self::Hamming => f.write_str("HAMMING"),
			Self::Mahalanobis => f.write_str("MAHALANOBIS"),
			Self::Minkowski(order) => write!(f, "MINKOWSKI {}", order),
		}
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
					"MTREE DIMENSION {} DIST {} CAPACITY {} DOC_IDS_ORDER {}",
					p.dimension, p.distance, p.capacity, p.doc_ids_order
				)
			}
		}
	}
}

pub fn index(i: &str) -> IResult<&str, Index> {
	alt((unique, search, mtree))(i)
}

pub fn unique(i: &str) -> IResult<&str, Index> {
	let (i, _) = tag_no_case("UNIQUE")(i)?;
	Ok((i, Index::Uniq))
}

pub fn analyzer(i: &str) -> IResult<&str, Ident> {
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag_no_case("ANALYZER")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, analyzer) = cut(ident)(i)?;
	Ok((i, analyzer))
}

fn order<'a>(label: &'static str, i: &'a str) -> IResult<&'a str, u32> {
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag_no_case(label)(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, order) = cut(uint32)(i)?;
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
	map(opt(tag("HIGHLIGHTS")), |x| x.is_some())(i)
}

pub fn search(i: &str) -> IResult<&str, Index> {
	let (i, _) = tag_no_case("SEARCH")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
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
	})(i)
}

pub fn distance(i: &str) -> IResult<&str, Distance> {
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag_no_case("DIST")(i)?;
	let (i, _) = shouldbespace(i)?;
	alt((
		map(tag_no_case("EUCLIDEAN"), |_| Distance::Euclidean),
		map(tag_no_case("MANHATTAN"), |_| Distance::Manhattan),
		map(tag_no_case("COSINE"), |_| Distance::Manhattan),
		map(tag_no_case("HAMMING"), |_| Distance::Manhattan),
		map(tag_no_case("MAHALANOBIS"), |_| Distance::Manhattan),
		minkowski,
	))(i)
}

pub fn minkowski(i: &str) -> IResult<&str, Distance> {
	let (i, _) = tag_no_case("MINKOWSKI")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, order) = uint32(i)?;
	Ok((i, Distance::Minkowski(order.into())))
}

pub fn dimension(i: &str) -> IResult<&str, u16> {
	let (i, _) = mightbespace(i)?;
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
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, dimension) = dimension(i)?;
		let (i, distance) = opt(distance)(i)?;
		let (i, capacity) = opt(capacity)(i)?;
		let (i, doc_ids_order) = opt(doc_ids_order)(i)?;
		Ok((
			i,
			Index::MTree(MTreeParams {
				dimension,
				distance: distance.unwrap_or(Distance::Euclidean),
				capacity: capacity.unwrap_or(40),
				doc_ids_order: doc_ids_order.unwrap_or(100),
			}),
		))
	})(i)
}
