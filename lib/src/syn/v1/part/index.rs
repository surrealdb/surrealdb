use super::super::{
	comment::{mightbespace, shouldbespace},
	literal::{ident, scoring},
	IResult,
};
use crate::sql::{
	index::{Distance, MTreeParams, SearchParams, VectorType},
	Ident, Index,
};
use nom::{
	branch::alt,
	bytes::complete::{tag, tag_no_case},
	character::complete::{u16, u32},
	combinator::{cut, map, opt},
};

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
	let (i, order) = cut(u32)(i)?;
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
				az: az.unwrap_or_else(|| Ident::from("like")),
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
	let (i, order) = u32(i)?;
	Ok((i, Distance::Minkowski(order.into())))
}

pub fn vector_type(i: &str) -> IResult<&str, VectorType> {
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag_no_case("TYPE")(i)?;
	let (i, _) = shouldbespace(i)?;
	alt((
		map(tag_no_case("F64"), |_| VectorType::F64),
		map(tag_no_case("F32"), |_| VectorType::F32),
		map(tag_no_case("I64"), |_| VectorType::I64),
		map(tag_no_case("I32"), |_| VectorType::I32),
		map(tag_no_case("I16"), |_| VectorType::I16),
		map(tag_no_case("I8"), |_| VectorType::I8),
	))(i)
}

pub fn dimension(i: &str) -> IResult<&str, u16> {
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag_no_case("DIMENSION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, dim) = u16(i)?;
	Ok((i, dim))
}

pub fn capacity(i: &str) -> IResult<&str, u16> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("CAPACITY")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, capacity) = u16(i)?;
	Ok((i, capacity))
}

pub fn mtree(i: &str) -> IResult<&str, Index> {
	let (i, _) = tag_no_case("MTREE")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, dimension) = dimension(i)?;
		let (i, distance) = opt(distance)(i)?;
		let (i, vector_type) = opt(vector_type)(i)?;
		let (i, capacity) = opt(capacity)(i)?;
		let (i, doc_ids_order) = opt(doc_ids_order)(i)?;
		Ok((
			i,
			Index::MTree(MTreeParams {
				dimension,
				distance: distance.unwrap_or(Distance::Euclidean),
				vector_type: vector_type.unwrap_or(VectorType::F64),
				capacity: capacity.unwrap_or(40),
				doc_ids_order: doc_ids_order.unwrap_or(100),
			}),
		))
	})(i)
}
