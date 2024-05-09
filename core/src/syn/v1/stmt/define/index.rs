use super::super::super::{
	comment::shouldbespace,
	ending,
	error::expect_tag_no_case,
	idiom::{self},
	literal::{ident, strand},
	part::index,
	IResult,
};
use crate::{
	sql::{statements::DefineIndexStatement, Idioms, Index, Strand},
	syn::v1::ParseError,
};
use nom::{
	branch::alt,
	bytes::complete::tag_no_case,
	combinator::{cut, opt},
	multi::many0,
	sequence::tuple,
	Err,
};

pub fn index(i: &str) -> IResult<&str, DefineIndexStatement> {
	let (i, _) = tag_no_case("INDEX")(i)?;
	let (i, if_not_exists) = opt(tuple((
		shouldbespace,
		tag_no_case("IF"),
		shouldbespace,
		tag_no_case("NOT"),
		shouldbespace,
		cut(tag_no_case("EXISTS")),
	)))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, (name, what, opts)) = cut(|i| {
		let (i, name) = ident(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, _) = expect_tag_no_case("ON")(i)?;
		let (i, _) = opt(tuple((shouldbespace, tag_no_case("TABLE"))))(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, what) = ident(i)?;
		let (i, opts) = many0(index_opts)(i)?;
		let (i, _) = ending::query(i)?;
		Ok((i, (name, what, opts)))
	})(i)?;
	// Create the base statement
	let mut res = DefineIndexStatement {
		name,
		what,
		if_not_exists: if_not_exists.is_some(),
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineIndexOption::Index(v) => {
				res.index = v;
			}
			DefineIndexOption::Columns(v) => {
				res.cols = v;
			}
			DefineIndexOption::Comment(v) => {
				res.comment = Some(v);
			}
		}
	}
	// Check necessary options
	if res.cols.is_empty() {
		return Err(Err::Failure(ParseError::ExplainedExpected {
			tried: i,
			expected: "a COLUMNS or FIELDS clause",
			explained: "An index requires a COLUMNS or FIELDS clause to be defined.",
		}));
	}
	// Return the statement
	Ok((i, res))
}

enum DefineIndexOption {
	Index(Index),
	Columns(Idioms),
	Comment(Strand),
}

fn index_opts(i: &str) -> IResult<&str, DefineIndexOption> {
	alt((index_kind, index_columns, index_comment))(i)
}

fn index_kind(i: &str) -> IResult<&str, DefineIndexOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, v) = index::index(i)?;
	Ok((i, DefineIndexOption::Index(v)))
}

fn index_columns(i: &str) -> IResult<&str, DefineIndexOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = alt((tag_no_case("COLUMNS"), tag_no_case("FIELDS")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = idiom::locals(i)?;
	Ok((i, DefineIndexOption::Columns(v)))
}

fn index_comment(i: &str) -> IResult<&str, DefineIndexOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = strand(i)?;
	Ok((i, DefineIndexOption::Comment(v)))
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::index::{
		Distance, Distance1, HnswParams, MTreeParams, SearchParams, VectorType,
	};
	use crate::sql::Part;
	use crate::sql::Scoring;
	use crate::sql::{Ident, Index};
	use crate::sql::{Idiom, Number};

	#[test]
	fn check_create_non_unique_index() {
		let sql = "INDEX my_index ON TABLE my_table COLUMNS my_col";
		let (_, idx) = index(sql).unwrap();
		assert_eq!(
			idx,
			DefineIndexStatement {
				name: Ident("my_index".to_string()),
				what: Ident("my_table".to_string()),
				cols: Idioms(vec![Idiom(vec![Part::Field(Ident("my_col".to_string()))])]),
				index: Index::Idx,
				comment: None,
				if_not_exists: false,
			}
		);
		assert_eq!(idx.to_string(), "DEFINE INDEX my_index ON my_table FIELDS my_col");
	}

	#[test]
	fn check_create_unique_index() {
		let sql = "INDEX my_index ON TABLE my_table COLUMNS my_col UNIQUE";
		let (_, idx) = index(sql).unwrap();
		assert_eq!(
			idx,
			DefineIndexStatement {
				name: Ident("my_index".to_string()),
				what: Ident("my_table".to_string()),
				cols: Idioms(vec![Idiom(vec![Part::Field(Ident("my_col".to_string()))])]),
				index: Index::Uniq,
				comment: None,
				if_not_exists: false,
			}
		);
		assert_eq!(idx.to_string(), "DEFINE INDEX my_index ON my_table FIELDS my_col UNIQUE");
	}

	#[test]
	fn check_create_search_index_with_highlights() {
		let sql = "INDEX my_index ON TABLE my_table COLUMNS my_col SEARCH ANALYZER my_analyzer BM25(1.2,0.75) \
		DOC_IDS_ORDER 1100 DOC_LENGTHS_ORDER 1200 POSTINGS_ORDER 1300 TERMS_ORDER 1400 \
		DOC_IDS_CACHE 100 DOC_LENGTHS_CACHE 200 POSTINGS_CACHE 300 TERMS_CACHE 400 HIGHLIGHTS";
		let (_, idx) = index(sql).unwrap();
		assert_eq!(
			idx,
			DefineIndexStatement {
				name: Ident("my_index".to_string()),
				what: Ident("my_table".to_string()),
				cols: Idioms(vec![Idiom(vec![Part::Field(Ident("my_col".to_string()))])]),
				index: Index::Search(SearchParams {
					az: Ident("my_analyzer".to_string()),
					hl: true,
					sc: Scoring::Bm {
						k1: 1.2,
						b: 0.75,
					},
					doc_ids_order: 1100,
					doc_lengths_order: 1200,
					postings_order: 1300,
					terms_order: 1400,
					doc_ids_cache: 100,
					doc_lengths_cache: 200,
					postings_cache: 300,
					terms_cache: 400,
				}),
				comment: None,
				if_not_exists: false,
			}
		);
		assert_eq!(idx.to_string(), "DEFINE INDEX my_index ON my_table FIELDS my_col SEARCH ANALYZER my_analyzer BM25(1.2,0.75) \
		DOC_IDS_ORDER 1100 DOC_LENGTHS_ORDER 1200 POSTINGS_ORDER 1300 TERMS_ORDER 1400 \
		DOC_IDS_CACHE 100 DOC_LENGTHS_CACHE 200 POSTINGS_CACHE 300 TERMS_CACHE 400 HIGHLIGHTS");
	}

	#[test]
	fn check_create_search_index() {
		let sql = "INDEX my_index ON TABLE my_table COLUMNS my_col SEARCH ANALYZER my_analyzer VS";
		let (_, idx) = index(sql).unwrap();
		assert_eq!(
			idx,
			DefineIndexStatement {
				name: Ident("my_index".to_string()),
				what: Ident("my_table".to_string()),
				cols: Idioms(vec![Idiom(vec![Part::Field(Ident("my_col".to_string()))])]),
				index: Index::Search(SearchParams {
					az: Ident("my_analyzer".to_string()),
					hl: false,
					sc: Scoring::Vs,
					doc_ids_order: 100,
					doc_lengths_order: 100,
					postings_order: 100,
					terms_order: 100,
					doc_ids_cache: 100,
					doc_lengths_cache: 100,
					postings_cache: 100,
					terms_cache: 100,
				}),
				comment: None,
				if_not_exists: false,
			}
		);
		assert_eq!(
			idx.to_string(),
			"DEFINE INDEX my_index ON my_table FIELDS my_col SEARCH ANALYZER my_analyzer VS DOC_IDS_ORDER 100 DOC_LENGTHS_ORDER 100 POSTINGS_ORDER 100 TERMS_ORDER 100 DOC_IDS_CACHE 100 DOC_LENGTHS_CACHE 100 POSTINGS_CACHE 100 TERMS_CACHE 100"
		);
	}

	#[test]
	fn check_create_mtree_index() {
		let sql = "INDEX my_index ON TABLE my_table COLUMNS my_col MTREE DIMENSION 4";
		let (_, idx) = index(sql).unwrap();
		assert_eq!(
			idx,
			DefineIndexStatement {
				name: Ident("my_index".to_string()),
				what: Ident("my_table".to_string()),
				cols: Idioms(vec![Idiom(vec![Part::Field(Ident("my_col".to_string()))])]),
				index: Index::MTree(MTreeParams {
					dimension: 4,
					vector_type: VectorType::F64,
					_distance: Distance1::Euclidean,
					distance: Distance::Euclidean,
					capacity: 40,
					doc_ids_order: 100,
					doc_ids_cache: 100,
					mtree_cache: 100,
				}),
				comment: None,
				if_not_exists: false,
			}
		);
		assert_eq!(
			idx.to_string(),
			"DEFINE INDEX my_index ON my_table FIELDS my_col MTREE DIMENSION 4 DIST EUCLIDEAN TYPE F64 CAPACITY 40 DOC_IDS_ORDER 100 DOC_IDS_CACHE 100 MTREE_CACHE 100"
		);
	}

	#[test]
	fn check_create_hnsw_index() {
		let sql =
			"INDEX my_index ON my_table FIELDS my_col HNSW DIMENSION 4 EXTEND_CANDIDATES EFC 500 KEEP_PRUNED_CONNECTIONS DIST MANHATTAN TYPE F32 M 16 M0 24";
		let (_, idx) = index(sql).unwrap();
		assert_eq!(
			idx,
			DefineIndexStatement {
				name: Ident("my_index".to_string()),
				what: Ident("my_table".to_string()),
				cols: Idioms(vec![Idiom(vec![Part::Field(Ident("my_col".to_string()))])]),
				index: Index::Hnsw(HnswParams {
					dimension: 4,
					distance: Distance::Manhattan,
					vector_type: VectorType::F32,
					m: 16,
					m0: 24,
					ml: Number::Float(0.36067376022224085),
					ef_construction: 500,
					extend_candidates: true,
					keep_pruned_connections: true
				}),
				comment: None,
				if_not_exists: false,
			}
		);
		assert_eq!(
			idx.to_string(),
			"DEFINE INDEX my_index ON my_table FIELDS my_col HNSW DIMENSION 4 DIST MANHATTAN TYPE F32 EFC 500 M 16 M0 24 ML 0.36067376022224085f EXTEND_CANDIDATES KEEP_PRUNED_CONNECTIONS"
		);
	}

	#[test]
	fn define_index_without_columns_clause() {
		let sql = "INDEX test ON test";
		let res = index(sql);

		assert!(res.is_err())
	}
}
