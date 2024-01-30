use super::super::{
	comment::{mightbespace, shouldbespace},
	literal::{ident, number, scoring},
	IResult,
};
use crate::sql::{
	index::{Distance, MTreeParams, SearchParams, VectorType},
	Ident, Index,
};

use crate::sql::index::HnswParams;
use nom::{
	branch::alt,
	bytes::complete::{tag, tag_no_case},
	character::complete::{u16, u32},
	combinator::{cut, map, opt},
};

pub fn index(i: &str) -> IResult<&str, Index> {
	alt((unique, search, mtree, hnsw))(i)
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

pub fn doc_ids_cache(i: &str) -> IResult<&str, u32> {
	order("DOC_IDS_CACHE", i)
}

pub fn doc_lengths_order(i: &str) -> IResult<&str, u32> {
	order("DOC_LENGTHS_ORDER", i)
}

pub fn doc_lengths_cache(i: &str) -> IResult<&str, u32> {
	order("DOC_LENGTHS_CACHE", i)
}

pub fn postings_order(i: &str) -> IResult<&str, u32> {
	order("POSTINGS_ORDER", i)
}

pub fn postings_cache(i: &str) -> IResult<&str, u32> {
	order("POSTINGS_CACHE", i)
}

pub fn terms_order(i: &str) -> IResult<&str, u32> {
	order("TERMS_ORDER", i)
}

pub fn terms_cache(i: &str) -> IResult<&str, u32> {
	order("TERMS_CACHE", i)
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
		let (i, c1) = opt(doc_ids_cache)(i)?;
		let (i, c2) = opt(doc_lengths_cache)(i)?;
		let (i, c3) = opt(postings_cache)(i)?;
		let (i, c4) = opt(terms_cache)(i)?;
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
				doc_ids_cache: c1.unwrap_or(100),
				doc_lengths_cache: c2.unwrap_or(100),
				postings_cache: c3.unwrap_or(100),
				terms_cache: c4.unwrap_or(100),
			}),
		))
	})(i)
}

pub fn mtree_distance(i: &str) -> IResult<&str, Distance> {
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag_no_case("DIST")(i)?;
	let (i, _) = shouldbespace(i)?;
	alt((
		map(tag_no_case("EUCLIDEAN"), |_| Distance::Euclidean),
		map(tag_no_case("MANHATTAN"), |_| Distance::Manhattan),
		minkowski,
	))(i)
}

pub fn hnsw_distance(i: &str) -> IResult<&str, Distance> {
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag_no_case("DIST")(i)?;
	let (i, _) = shouldbespace(i)?;
	alt((
		map(tag_no_case("COSINE"), |_| Distance::Cosine),
		map(tag_no_case("EUCLIDEAN"), |_| Distance::Euclidean),
		map(tag_no_case("MANHATTAN"), |_| Distance::Manhattan),
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

pub fn mtree_cache(i: &str) -> IResult<&str, u32> {
	order("MTREE_CACHE", i)
}

pub fn mtree(i: &str) -> IResult<&str, Index> {
	let (i, _) = tag_no_case("MTREE")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, dimension) = dimension(i)?;
		let (i, distance) = opt(mtree_distance)(i)?;
		let (i, vector_type) = opt(vector_type)(i)?;
		let (i, capacity) = opt(capacity)(i)?;
		let (i, doc_ids_order) = opt(doc_ids_order)(i)?;
		let (i, doc_ids_cache) = opt(doc_ids_cache)(i)?;
		let (i, mtree_cache) = opt(mtree_cache)(i)?;
		Ok((
			i,
			Index::MTree(MTreeParams {
				dimension,
				distance: distance.unwrap_or(Distance::Euclidean),
				vector_type: vector_type.unwrap_or(VectorType::F64),
				capacity: capacity.unwrap_or(40),
				doc_ids_order: doc_ids_order.unwrap_or(100),
				doc_ids_cache: doc_ids_cache.unwrap_or(100),
				mtree_cache: mtree_cache.unwrap_or(100),
			}),
		))
	})(i)
}

pub fn hnsw(i: &str) -> IResult<&str, Index> {
	let (i, _) = tag_no_case("HNSW")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, dimension) = dimension(i)?;
		let (i, distance) = opt(hnsw_distance)(i)?;
		let (i, vector_type) = opt(vector_type)(i)?;
		let (i, ef_construction) = opt(ef_construction)(i)?;
		let (i, m) = opt(m)(i)?;
		let (i, m0) = opt(m0)(i)?;
		let (i, ml) = opt(ml)(i)?;
		let ef_construction = ef_construction.unwrap_or(150);
		let m = m.unwrap_or(12);
		let m0 = m0.unwrap_or(m * 2);
		let ml = ml.unwrap_or(1.0 / (m as f64).ln()).into();
		Ok((
			i,
			Index::Hnsw(HnswParams {
				dimension,
				distance: distance.unwrap_or(Distance::Euclidean),
				vector_type: vector_type.unwrap_or(VectorType::F64),
				m,
				m0,
				ef_construction,
				ml,
			}),
		))
	})(i)
}

pub fn m(i: &str) -> IResult<&str, u16> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("M")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, m) = u16(i)?;
	Ok((i, m))
}

pub fn m0(i: &str) -> IResult<&str, u16> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("M0")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, m0) = u16(i)?;
	Ok((i, m0))
}

pub fn ml(i: &str) -> IResult<&str, f64> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ML")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, ml) = number(i)?;
	Ok((i, ml.to_float()))
}

pub fn ef_construction(i: &str) -> IResult<&str, u16> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("EFC")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, efc) = u16(i)?;
	Ok((i, efc))
}
