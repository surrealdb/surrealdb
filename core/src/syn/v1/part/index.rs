use super::super::{
	comment::shouldbespace,
	literal::{ident, number, scoring},
	operator::{distance, minkowski},
	IResult,
};
use crate::sql::{
	index::{Distance, Distance1, HnswParams, MTreeParams, SearchParams, VectorType},
	number::Number,
	Ident, Index, Scoring,
};

use nom::{
	branch::alt,
	bytes::complete::tag_no_case,
	character::complete::{u16, u32, u8},
	combinator::{cut, map},
	multi::many0,
};

pub fn index(i: &str) -> IResult<&str, Index> {
	alt((unique, search, mtree, hnsw))(i)
}

fn unique(i: &str) -> IResult<&str, Index> {
	let (i, _) = tag_no_case("UNIQUE")(i)?;
	Ok((i, Index::Uniq))
}

enum SearchOption {
	Analyzer(Ident),
	Scoring(Scoring),
	DocIdsOrder(u32),
	DocLengthOrder(u32),
	PostingsOrder(u32),
	TermsOrder(u32),
	DocIdsCache(u32),
	DocLengthCache(u32),
	PostingsCache(u32),
	TermsCache(u32),
	Highlights,
}

fn param_u32<'a>(label: &'static str, i: &'a str) -> IResult<&'a str, u32> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case(label)(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(u32)(i)?;
	Ok((i, v))
}

fn param_u16<'a>(label: &'static str, i: &'a str) -> IResult<&'a str, u16> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case(label)(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(u16)(i)?;
	Ok((i, v))
}

fn param_u8<'a>(label: &'static str, i: &'a str) -> IResult<&'a str, u8> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case(label)(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(u8)(i)?;
	Ok((i, v))
}

fn search_analyzer(i: &str) -> IResult<&str, SearchOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ANALYZER")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, analyzer) = cut(ident)(i)?;
	Ok((i, SearchOption::Analyzer(analyzer)))
}

fn search_scoring(i: &str) -> IResult<&str, SearchOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, sc) = scoring(i)?;
	Ok((i, SearchOption::Scoring(sc)))
}

fn search_doc_ids_order(i: &str) -> IResult<&str, SearchOption> {
	let (i, v) = param_u32("DOC_IDS_ORDER", i)?;
	Ok((i, SearchOption::DocIdsOrder(v)))
}

fn search_doc_ids_cache(i: &str) -> IResult<&str, SearchOption> {
	let (i, v) = param_u32("DOC_IDS_CACHE", i)?;
	Ok((i, SearchOption::DocIdsCache(v)))
}

fn search_doc_lengths_order(i: &str) -> IResult<&str, SearchOption> {
	let (i, v) = param_u32("DOC_LENGTHS_ORDER", i)?;
	Ok((i, SearchOption::DocLengthOrder(v)))
}

fn search_doc_lengths_cache(i: &str) -> IResult<&str, SearchOption> {
	let (i, v) = param_u32("DOC_LENGTHS_CACHE", i)?;
	Ok((i, SearchOption::DocLengthCache(v)))
}

fn search_postings_order(i: &str) -> IResult<&str, SearchOption> {
	let (i, v) = param_u32("POSTINGS_ORDER", i)?;
	Ok((i, SearchOption::PostingsOrder(v)))
}

fn search_postings_cache(i: &str) -> IResult<&str, SearchOption> {
	let (i, v) = param_u32("POSTINGS_CACHE", i)?;
	Ok((i, SearchOption::PostingsCache(v)))
}

fn search_terms_order(i: &str) -> IResult<&str, SearchOption> {
	let (i, v) = param_u32("TERMS_ORDER", i)?;
	Ok((i, SearchOption::TermsOrder(v)))
}

fn search_terms_cache(i: &str) -> IResult<&str, SearchOption> {
	let (i, v) = param_u32("TERMS_CACHE", i)?;
	Ok((i, SearchOption::TermsCache(v)))
}

fn search_highlights(i: &str) -> IResult<&str, SearchOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("HIGHLIGHTS")(i)?;
	Ok((i, SearchOption::Highlights))
}

fn search_option(i: &str) -> IResult<&str, SearchOption> {
	alt((
		search_analyzer,
		search_scoring,
		search_highlights,
		search_doc_ids_order,
		search_doc_ids_cache,
		search_doc_lengths_order,
		search_doc_lengths_cache,
		search_postings_order,
		search_postings_cache,
		search_terms_order,
		search_terms_cache,
	))(i)
}
fn search(i: &str) -> IResult<&str, Index> {
	let (i, _) = tag_no_case("SEARCH")(i)?;
	cut(|i| {
		let mut p = SearchParams {
			az: Ident::from("like"),
			sc: Scoring::bm25(),
			hl: false,
			doc_ids_order: 100,
			doc_lengths_order: 100,
			postings_order: 100,
			terms_order: 100,
			doc_ids_cache: 100,
			doc_lengths_cache: 100,
			postings_cache: 100,
			terms_cache: 100,
		};

		let (i, opts) = many0(search_option)(i)?;

		for opt in opts {
			match opt {
				SearchOption::Analyzer(v) => p.az = v,
				SearchOption::Scoring(sc) => p.sc = sc,
				SearchOption::DocIdsOrder(v) => p.doc_ids_order = v,
				SearchOption::DocLengthOrder(v) => p.doc_lengths_order = v,
				SearchOption::PostingsOrder(v) => p.postings_order = v,
				SearchOption::TermsOrder(v) => p.terms_order = v,
				SearchOption::DocIdsCache(v) => p.doc_ids_cache = v,
				SearchOption::DocLengthCache(v) => p.doc_lengths_cache = v,
				SearchOption::PostingsCache(v) => p.postings_cache = v,
				SearchOption::TermsCache(v) => p.terms_cache = v,
				SearchOption::Highlights => p.hl = true,
			}
		}

		Ok((i, Index::Search(p)))
	})(i)
}

enum MtreeOption {
	Distance(Distance),
	VectorType(VectorType),
	Capacity(u16),
	DocIdsOrder(u32),
	DosIdsCache(u32),
	MTreeCache(u32),
}

fn mtree_distance(i: &str) -> IResult<&str, MtreeOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("DIST")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, dist) = alt((
		map(tag_no_case("EUCLIDEAN"), |_| Distance::Euclidean),
		map(tag_no_case("COSINE"), |_| Distance::Cosine),
		map(tag_no_case("MANHATTAN"), |_| Distance::Manhattan),
		minkowski,
	))(i)?;
	Ok((i, MtreeOption::Distance(dist)))
}

fn vector_type(i: &str) -> IResult<&str, VectorType> {
	let (i, _) = shouldbespace(i)?;
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

fn dimension(i: &str) -> IResult<&str, u16> {
	let (i, _) = tag_no_case("DIMENSION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, dim) = u16(i)?;
	Ok((i, dim))
}

fn mtree_vector_type(i: &str) -> IResult<&str, MtreeOption> {
	let (i, v) = vector_type(i)?;
	Ok((i, MtreeOption::VectorType(v)))
}

fn mtree_capacity(i: &str) -> IResult<&str, MtreeOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("CAPACITY")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, capacity) = u16(i)?;
	Ok((i, MtreeOption::Capacity(capacity)))
}

fn mtree_doc_ids_order(i: &str) -> IResult<&str, MtreeOption> {
	let (i, v) = param_u32("DOC_IDS_ORDER", i)?;
	Ok((i, MtreeOption::DocIdsOrder(v)))
}

fn mtree_doc_ids_cache(i: &str) -> IResult<&str, MtreeOption> {
	let (i, v) = param_u32("DOC_IDS_CACHE", i)?;
	Ok((i, MtreeOption::DosIdsCache(v)))
}

fn mtree_cache(i: &str) -> IResult<&str, MtreeOption> {
	let (i, v) = param_u32("MTREE_CACHE", i)?;
	Ok((i, MtreeOption::MTreeCache(v)))
}

fn mtree_option(i: &str) -> IResult<&str, MtreeOption> {
	alt((
		mtree_distance,
		mtree_vector_type,
		mtree_capacity,
		mtree_doc_ids_order,
		mtree_doc_ids_cache,
		mtree_cache,
	))(i)
}

fn mtree(i: &str) -> IResult<&str, Index> {
	let (i, _) = tag_no_case("MTREE")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, dimension) = dimension(i)?;
		let (i, opts) = many0(mtree_option)(i)?;

		let mut p = MTreeParams {
			dimension,
			_distance: Distance1::Euclidean, // TODO remove once 1.0 && 1.1 are EOL
			distance: Distance::Euclidean,
			vector_type: VectorType::F64,
			capacity: 40,
			doc_ids_order: 100,
			doc_ids_cache: 100,
			mtree_cache: 100,
		};

		for opt in opts {
			match opt {
				MtreeOption::Distance(v) => p.distance = v,
				MtreeOption::VectorType(v) => p.vector_type = v,
				MtreeOption::Capacity(v) => p.capacity = v,
				MtreeOption::DocIdsOrder(v) => p.doc_ids_order = v,
				MtreeOption::DosIdsCache(v) => p.doc_ids_cache = v,
				MtreeOption::MTreeCache(v) => p.mtree_cache = v,
			}
		}

		Ok((i, Index::MTree(p)))
	})(i)
}

enum HnswOption {
	Distance(Distance),
	VectorType(VectorType),
	EfConstruction(u16),
	M(u8),
	M0(u8),
	Ml(Number),
	ExtendCandidates,
	KeepPrunedConnections,
}

fn hnsw_distance(i: &str) -> IResult<&str, HnswOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("DIST")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, dist) = distance(i)?;
	Ok((i, HnswOption::Distance(dist)))
}

fn hnsw_vector_type(i: &str) -> IResult<&str, HnswOption> {
	let (i, v) = vector_type(i)?;
	Ok((i, HnswOption::VectorType(v)))
}
fn hnsw_m(i: &str) -> IResult<&str, HnswOption> {
	let (i, v) = param_u8("M", i)?;
	Ok((i, HnswOption::M(v)))
}

fn hnsw_m0(i: &str) -> IResult<&str, HnswOption> {
	let (i, v) = param_u8("M0", i)?;
	Ok((i, HnswOption::M0(v)))
}

fn hnsw_ml(i: &str) -> IResult<&str, HnswOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ML")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = number(i)?;
	Ok((i, HnswOption::Ml(v)))
}

fn hnsw_ef_construction(i: &str) -> IResult<&str, HnswOption> {
	let (i, v) = param_u16("EFC", i)?;
	Ok((i, HnswOption::EfConstruction(v)))
}

fn hnsw_extend_candidates(i: &str) -> IResult<&str, HnswOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("EXTEND_CANDIDATES")(i)?;
	Ok((i, HnswOption::ExtendCandidates))
}

fn hnsw_keep_pruned_connections(i: &str) -> IResult<&str, HnswOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("KEEP_PRUNED_CONNECTIONS")(i)?;
	Ok((i, HnswOption::KeepPrunedConnections))
}

fn hnsw_option(i: &str) -> IResult<&str, HnswOption> {
	alt((
		hnsw_distance,
		hnsw_vector_type,
		hnsw_m,
		hnsw_m0,
		hnsw_ml,
		hnsw_ef_construction,
		hnsw_extend_candidates,
		hnsw_keep_pruned_connections,
	))(i)
}

fn hnsw(i: &str) -> IResult<&str, Index> {
	let (i, _) = tag_no_case("HNSW")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, dimension) = dimension(i)?;
		let (i, opts) = many0(hnsw_option)(i)?;

		let mut distance = Distance::Euclidean;
		let mut vector_type = VectorType::F64;
		let mut m = None;
		let mut m0 = None;
		let mut ml = None;
		let mut ef_construction = 150;
		let mut extend_candidates = false;
		let mut keep_pruned_connections = false;

		for opt in opts {
			match opt {
				HnswOption::Distance(v) => distance = v,
				HnswOption::VectorType(v) => vector_type = v,
				HnswOption::M(v) => m = Some(v),
				HnswOption::M0(v) => m0 = Some(v),
				HnswOption::Ml(v) => ml = Some(v),
				HnswOption::EfConstruction(v) => ef_construction = v,
				HnswOption::ExtendCandidates => extend_candidates = true,
				HnswOption::KeepPrunedConnections => keep_pruned_connections = true,
			}
		}

		let m = m.unwrap_or(12);
		let m0 = m0.unwrap_or(m * 2);
		let ml = ml.unwrap_or((1.0 / (m as f64).ln()).into());

		Ok((
			i,
			Index::Hnsw(HnswParams {
				dimension,
				distance,
				vector_type,
				m,
				m0,
				ef_construction,
				extend_candidates,
				keep_pruned_connections,
				ml,
			}),
		))
	})(i)
}
