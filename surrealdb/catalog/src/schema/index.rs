use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};

use anyhow::{Context as _, Result};
use revision::{
	DeserializeRevisioned, Revisioned, SerializeRevisioned, SkipRevisioned, revisioned,
};
use storekey::{BorrowDecode, Encode};
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::catalog::{FromStored, IdiomText, SurqlText};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Cond, Idiom};
use crate::sql;
use crate::sql::statements::define::DefineKind;
use crate::val::{Array, Number, TableName, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, BorrowDecode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[repr(transparent)]
pub struct IndexId(pub u32);

impl_kv_value_revisioned!(IndexId);

impl Revisioned for IndexId {
	fn revision() -> u16 {
		1
	}
}

impl SerializeRevisioned for IndexId {
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		SerializeRevisioned::serialize_revisioned(&self.0, writer)
	}
}

impl DeserializeRevisioned for IndexId {
	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		DeserializeRevisioned::deserialize_revisioned(reader).map(IndexId)
	}
}

impl SkipRevisioned for IndexId {
	#[inline]
	fn skip_revisioned<R: std::io::Read>(reader: &mut R) -> Result<(), revision::Error> {
		<u32 as SkipRevisioned>::skip_revisioned(reader)
	}
}

impl revision::WalkRevisioned for IndexId {
	type Walker<'r, R: revision::BorrowedReader + 'r> = revision::LeafWalker<'r, IndexId, R>;

	#[inline]
	fn walk_revisioned<'r, R: revision::BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, revision::Error> {
		Ok(revision::LeafWalker::new(reader))
	}
}

impl From<u32> for IndexId {
	fn from(value: u32) -> Self {
		IndexId(value)
	}
}

/// Current on-disk format version stamped on newly defined or rebuilt indexes.
/// Bump when any index kind's persisted layout changes. Each kind's *required*
/// version (see [`StoredIndexDefinition::required_format_version`]) stays at the
/// version that introduced its current mandatory layout, so bumping this
/// constant does not invalidate existing indexes of other kinds.
pub const INDEX_FORMAT_VERSION: u16 = 2;

/// Format version at which full-text, HNSW and DiskAnn indexes migrated onto
/// the shared table-level doc-ID space. These kinds cannot be read below this
/// version and require `REBUILD INDEX` after an upgrade.
pub const DOC_IDS_FORMAT_VERSION: u16 = 1;

/// Format version at which b-tree (`Idx`/`Uniq`) index entry values carry the
/// record's table-level doc-ID appended to the record ID (8 bytes big-endian).
/// Older b-tree indexes remain readable (their entries simply lack doc-IDs);
/// this version only gates eligibility for doc-ID-based plans such as roaring
/// bitmap candidate fusion.
pub const BTREE_ENTRY_DOC_IDS_FORMAT_VERSION: u16 = 2;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StoredIndexDefinition {
	pub index_id: IndexId,
	pub name: Strand,
	pub table_name: Strand,
	/// Raw idiom-path text for each indexed column, in `Idiom::to_raw_string()`
	/// form (e.g. `"address.city"`). Compiled on demand by consumers that
	/// need the structured `Idiom` (index maintenance, planner analysis).
	pub cols: Vec<IdiomText>,
	pub index: Index,
	pub comment: Option<String>,
	/// Whether this index has been marked for removal via `REMOVE INDEX`.
	/// Indexes marked for removal are excluded from query planning and document
	/// indexing, and any in-progress index builds are cancelled.
	pub prepare_remove: bool,
	/// On-disk format version of this index's persisted data. Definitions written
	/// by a binary predating the table-level doc-ID space decode this as `0`; the
	/// doc-ID-backed kinds require [`INDEX_FORMAT_VERSION`] and are rejected with
	/// [`Error::IndexRebuildRequired`](crate::catalog::Error::IndexRebuildRequired)
	/// until rebuilt.
	#[revision(start = 2)]
	pub format_version: u16,
}

impl_kv_value_revisioned!(StoredIndexDefinition);

impl IndexDefinition {
	/// Lowers `index` to its sql-side form. The count index's guard condition
	/// comes from the compiled `count_cond`; every other kind is plain data
	/// and lowers via [`Index::to_sql_definition`].
	fn index_to_sql_definition(&self) -> sql::index::Index {
		match &self.index {
			Index::Count(_) => sql::index::Index::Count(self.count_cond.clone().map(Into::into)),
			other => other.to_sql_definition(),
		}
	}

	fn to_sql_definition(&self) -> sql::DefineIndexStatement {
		sql::DefineIndexStatement {
			kind: DefineKind::Default,
			name: sql::Expr::Idiom(sql::Idiom::field(self.name.clone())),
			what: sql::Expr::Table(self.table_name.clone().into()),
			cols: self.cols.iter().map(|i| sql::Expr::Idiom(i.clone().into())).collect(),
			index: self.index_to_sql_definition(),
			comment: self
				.comment
				.clone()
				.map(|x| sql::Expr::Literal(sql::Literal::String(x.into())))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
			concurrently: false,
		}
	}
}

impl InfoStructure for IndexDefinition {
	fn structure(self) -> Value {
		let index = Value::from(self.index_to_sql_definition().to_sql());
		Value::from(map! {
			"name" => self.name.into(),
			"table" => Value::String(self.table_name.into()),
			// `structure`, not `to_raw_string`: the latter escapes through
			// `EscapeKwFreeIdent`, which leaves a reserved word bare, so a column
			// named `value` would render here as `value` while the same
			// definition's `index` key and plain `INFO FOR TABLE` both render it
			// as `` `value` ``. The two INFO forms must agree.
			"cols" => Value::Array(Array(self.cols.iter().map(|i| i.clone().structure()).collect())),
			"index" => index,
			"comment", if let Some(v) = self.comment => v.into(),
			"prepare_remove", if self.prepare_remove => self.prepare_remove.into()
		})
	}
}

impl ToSql for IndexDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}

/// Canonical SurrealQL text of a Count index's optional guard condition
/// (the bare expression following WHERE, no prefix) -- the stored-text
/// counterpart to expr::Cond, kept as a revisioned single-field wrapper
/// because the wire format it replaced (`Cond(Expr)`, a derived rev-1
/// wrapper) carries a revision header that a bare `SurqlText` does not.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CondText(pub SurqlText<Cond>);

impl CondText {
	/// Compiles the stored condition text into a `sql::Cond` AST node, for
	/// handing to `sql::index::Index::Count` at the INFO/export rendering
	/// boundary. The text was already parsed successfully once, when the
	/// `DEFINE INDEX` statement that produced it was executed, so a failure
	/// here is a stored-catalog invariant violation, not bad user input.
	/// This path is only reachable from cold, informational rendering;
	/// query execution reads the stored text directly and never goes
	/// through this conversion.
	fn to_ast(&self) -> Option<sql::Cond> {
		match self.0.compile() {
			Ok(cond) => Some(cond.into()),
			Err(err) => {
				debug_assert!(false, "stored count-index condition failed to parse: {:?}", self.0);
				// Warn, not silence: the rendered `DEFINE INDEX` loses its
				// `WHERE`, so an export and re-import would rebuild the index
				// unfiltered and it would return different counts.
				// `IndexDefinition::from_stored` propagates for the same bytes,
				// so the two paths disagree about whether the definition is
				// readable at all — which is worth seeing in a log.
				warn!(
					target: "surrealdb::core::catalog",
					error = %err,
					"Stored count-index condition no longer parses; it is omitted from the \
					 rendered definition, so an export of this index would be unfiltered"
				);
				None
			}
		}
	}
}

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum Index {
	/// (Basic) non unique
	#[default]
	Idx,
	/// Unique index
	Uniq,
	/// HNSW index for distance-based metrics
	Hnsw(HnswParams),
	/// Index with Full-Text search capabilities
	FullText(FullTextParams),
	/// Count index
	Count(Option<CondText>),
	/// DiskANN index for distance-based metrics
	#[revision(start = 2)]
	DiskAnn(DiskAnnParams),
}

impl Index {
	pub fn to_sql_definition(&self) -> sql::index::Index {
		match self {
			Self::Idx => sql::index::Index::Idx,
			Self::Uniq => sql::index::Index::Uniq,
			Self::Hnsw(params) => sql::index::Index::Hnsw(params.clone().into()),
			Self::DiskAnn(params) => sql::index::Index::DiskAnn(params.clone().into()),
			Self::FullText(params) => sql::index::Index::FullText(params.clone().into()),
			Self::Count(cond) => sql::index::Index::Count(cond.as_ref().and_then(CondText::to_ast)),
		}
	}

	/// Returns true if this index type can be used for ORDER BY optimization.
	/// Only indexes storing values in lexicographic order (Idx, Uniq) support ordered iteration.
	pub fn supports_order(&self) -> bool {
		matches!(self, Self::Idx | Self::Uniq)
	}
}

impl InfoStructure for Index {
	fn structure(self) -> Value {
		self.to_sql().into()
	}
}

impl ToSql for Index {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}

impl From<sql::index::Index> for Index {
	fn from(v: sql::index::Index) -> Self {
		match v {
			sql::index::Index::Idx => Self::Idx,
			sql::index::Index::Uniq => Self::Uniq,
			sql::index::Index::Hnsw(p) => Self::Hnsw(p.into()),
			sql::index::Index::DiskAnn(p) => Self::DiskAnn(p.into()),
			sql::index::Index::FullText(p) => Self::FullText(p.into()),
			sql::index::Index::Count(c) => Self::Count(c.map(|cond| {
				let cond: Cond = cond.into();
				CondText(SurqlText::new(&cond))
			})),
		}
	}
}

impl From<Index> for sql::index::Index {
	fn from(v: Index) -> Self {
		match v {
			Index::Idx => Self::Idx,
			Index::Uniq => Self::Uniq,
			Index::Hnsw(p) => Self::Hnsw(p.into()),
			Index::DiskAnn(p) => Self::DiskAnn(p.into()),
			Index::FullText(p) => Self::FullText(p.into()),
			Index::Count(c) => Self::Count(c.as_ref().and_then(CondText::to_ast)),
		}
	}
}

/// Full-Text search parameters.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FullTextParams {
	/// The analyzer to use.
	pub analyzer: Strand,
	/// Whether to highlight the search results.
	pub highlight: bool,
	/// The scoring to use.
	pub scoring: Scoring,
}

impl From<sql::index::FullTextParams> for FullTextParams {
	fn from(v: sql::index::FullTextParams) -> Self {
		FullTextParams {
			analyzer: v.az.clone(),
			highlight: v.hl,
			scoring: v.sc.into(),
		}
	}
}

impl From<FullTextParams> for sql::index::FullTextParams {
	fn from(v: FullTextParams) -> Self {
		Self {
			az: v.analyzer.clone(),
			hl: v.highlight,
			sc: v.scoring.into(),
		}
	}
}

/// Scoring for Full-Text search.
#[revisioned(revision = 1)]
#[derive(Clone, Debug)]
pub enum Scoring {
	/// BestMatching25 scoring.
	///
	/// <https://en.wikipedia.org/wiki/Okapi_BM25>
	Bm {
		/// The k~1~ parameter.
		k1: f32,
		/// The b parameter.
		b: f32,
	},
	/// VectorSearch scoring.
	Vs,
}

impl Eq for Scoring {}

impl PartialEq for Scoring {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(
				Scoring::Bm {
					k1,
					b,
				},
				Scoring::Bm {
					k1: other_k1,
					b: other_b,
				},
			) => k1.to_bits() == other_k1.to_bits() && b.to_bits() == other_b.to_bits(),
			(Scoring::Vs, Scoring::Vs) => true,
			_ => false,
		}
	}
}

impl Hash for Scoring {
	fn hash<H: Hasher>(&self, state: &mut H) {
		match self {
			Scoring::Bm {
				k1,
				b,
			} => {
				k1.to_bits().hash(state);
				b.to_bits().hash(state);
			}
			Scoring::Vs => 0.hash(state),
		}
	}
}

impl Default for Scoring {
	fn default() -> Self {
		Self::Bm {
			k1: 1.2,
			b: 0.75,
		}
	}
}

impl From<sql::scoring::Scoring> for Scoring {
	fn from(v: sql::scoring::Scoring) -> Self {
		match v {
			sql::scoring::Scoring::Bm {
				k1,
				b,
			} => Self::Bm {
				k1,
				b,
			},
			sql::scoring::Scoring::Vs => Self::Vs,
		}
	}
}

impl From<Scoring> for sql::scoring::Scoring {
	fn from(v: Scoring) -> Self {
		match v {
			Scoring::Bm {
				k1,
				b,
			} => sql::scoring::Scoring::Bm {
				k1,
				b,
			},
			Scoring::Vs => sql::scoring::Scoring::Vs,
		}
	}
}

/// Distance metric for calculating distances between vectors.
#[revisioned(revision = 2)]
#[derive(Clone, Default, Debug, Eq, PartialEq, Hash)]
pub enum Distance {
	/// Chebyshev distance.
	///
	/// <https://en.wikipedia.org/wiki/Chebyshev_distance>
	Chebyshev,
	/// Cosine distance.
	///
	/// <https://en.wikipedia.org/wiki/Cosine_similarity>
	Cosine,
	/// Euclidean distance.
	///
	/// <https://en.wikipedia.org/wiki/Euclidean_distance>
	#[default]
	Euclidean,
	/// Hamming distance.
	///
	/// <https://en.wikipedia.org/wiki/Hamming_distance>
	Hamming,
	/// Jaccard distance.
	///
	/// <https://en.wikipedia.org/wiki/Jaccard_index>
	Jaccard,
	/// Manhattan distance.
	///
	/// <https://en.wikipedia.org/wiki/Manhattan_distance>
	Manhattan,
	/// Minkowski distance.
	///
	/// <https://en.wikipedia.org/wiki/Minkowski_distance>
	Minkowski(Number),
	/// Pearson distance.
	///
	/// <https://en.wikipedia.org/wiki/Pearson_correlation_coefficient>
	Pearson,
	/// Cosine distance for already-normalized vectors.
	#[revision(start = 2)]
	CosineNormalized,
	/// Inner product similarity, transformed as a distance score.
	#[revision(start = 2)]
	InnerProduct,
}

impl ToSql for Distance {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Chebyshev => f.push_str("CHEBYSHEV"),
			Self::Cosine => f.push_str("COSINE"),
			Self::CosineNormalized => f.push_str("COSINE_NORMALIZED"),
			Self::Euclidean => f.push_str("EUCLIDEAN"),
			Self::Hamming => f.push_str("HAMMING"),
			Self::InnerProduct => f.push_str("INNER_PRODUCT"),
			Self::Jaccard => f.push_str("JACCARD"),
			Self::Manhattan => f.push_str("MANHATTAN"),
			Self::Minkowski(order) => write_sql!(f, fmt, "MINKOWSKI {}", order),
			Self::Pearson => f.push_str("PEARSON"),
		}
	}
}

impl From<sql::index::Distance> for Distance {
	fn from(v: sql::index::Distance) -> Self {
		match v {
			sql::index::Distance::Chebyshev => Self::Chebyshev,
			sql::index::Distance::Cosine => Self::Cosine,
			sql::index::Distance::CosineNormalized => Self::CosineNormalized,
			sql::index::Distance::Euclidean => Self::Euclidean,
			sql::index::Distance::Hamming => Self::Hamming,
			sql::index::Distance::InnerProduct => Self::InnerProduct,
			sql::index::Distance::Jaccard => Self::Jaccard,
			sql::index::Distance::Manhattan => Self::Manhattan,
			sql::index::Distance::Minkowski(n) => Self::Minkowski(n.into()),
			sql::index::Distance::Pearson => Self::Pearson,
		}
	}
}

impl From<Distance> for sql::index::Distance {
	fn from(v: Distance) -> Self {
		match v {
			Distance::Chebyshev => sql::index::Distance::Chebyshev,
			Distance::Cosine => sql::index::Distance::Cosine,
			Distance::CosineNormalized => sql::index::Distance::CosineNormalized,
			Distance::Euclidean => sql::index::Distance::Euclidean,
			Distance::Hamming => sql::index::Distance::Hamming,
			Distance::InnerProduct => sql::index::Distance::InnerProduct,
			Distance::Jaccard => sql::index::Distance::Jaccard,
			Distance::Manhattan => sql::index::Distance::Manhattan,
			Distance::Minkowski(n) => sql::index::Distance::Minkowski(n.into()),
			Distance::Pearson => sql::index::Distance::Pearson,
		}
	}
}

/// Vector type for storing vectors.
#[revisioned(revision = 2)]
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq, Hash)]
pub enum VectorType {
	/// 64-bit floating point.
	F64,
	/// 32-bit floating point.
	#[default]
	F32,
	/// 64-bit signed integer.
	I64,
	/// 32-bit signed integer.
	I32,
	/// 16-bit signed integer.
	I16,
	/// 16-bit floating point.
	#[revision(start = 2)]
	F16,
	/// 8-bit signed integer.
	#[revision(start = 2)]
	I8,
	/// 8-bit unsigned integer.
	#[revision(start = 2)]
	U8,
}

impl Display for VectorType {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			Self::F64 => f.write_str("F64"),
			Self::F16 => f.write_str("F16"),
			Self::F32 => f.write_str("F32"),
			Self::I64 => f.write_str("I64"),
			Self::I32 => f.write_str("I32"),
			Self::I16 => f.write_str("I16"),
			Self::I8 => f.write_str("I8"),
			Self::U8 => f.write_str("U8"),
		}
	}
}

impl From<sql::index::VectorType> for VectorType {
	fn from(v: sql::index::VectorType) -> Self {
		match v {
			sql::index::VectorType::F64 => Self::F64,
			sql::index::VectorType::F16 => Self::F16,
			sql::index::VectorType::F32 => Self::F32,
			sql::index::VectorType::I64 => Self::I64,
			sql::index::VectorType::I32 => Self::I32,
			sql::index::VectorType::I16 => Self::I16,
			sql::index::VectorType::I8 => Self::I8,
			sql::index::VectorType::U8 => Self::U8,
		}
	}
}

impl From<VectorType> for sql::index::VectorType {
	fn from(v: VectorType) -> Self {
		match v {
			VectorType::F64 => Self::F64,
			VectorType::F16 => Self::F16,
			VectorType::F32 => Self::F32,
			VectorType::I64 => Self::I64,
			VectorType::I32 => Self::I32,
			VectorType::I16 => Self::I16,
			VectorType::I8 => Self::I8,
			VectorType::U8 => Self::U8,
		}
	}
}

/// HNSW index parameters.
#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct HnswParams {
	/// The dimension of the index.
	pub dimension: u16,
	/// The distance metric to use.
	pub distance: Distance,
	/// The vector type to use.
	pub vector_type: VectorType,
	/// The m parameter.
	pub m: u8,
	/// The m0 parameter.
	pub m0: u8,
	/// The ml parameter.
	pub ml: Number,
	/// The ef_construction parameter.
	pub ef_construction: u16,
	/// Whether to extend candidates.
	pub extend_candidates: bool,
	/// Whether to keep pruned connections.
	pub keep_pruned_connections: bool,
	/// Whether to use vector hash for vector retrieval.
	#[revision(start = 2)]
	pub use_hashed_vector: bool,
}

impl From<sql::index::HnswParams> for HnswParams {
	fn from(v: sql::index::HnswParams) -> Self {
		HnswParams {
			dimension: v.dimension,
			distance: v.distance.into(),
			vector_type: v.vector_type.into(),
			m: v.m,
			m0: v.m0,
			ef_construction: v.ef_construction,
			ml: v.ml.into(),
			extend_candidates: v.extend_candidates,
			keep_pruned_connections: v.keep_pruned_connections,
			use_hashed_vector: v.use_hashed_vector,
		}
	}
}

impl From<HnswParams> for sql::index::HnswParams {
	fn from(v: HnswParams) -> Self {
		Self {
			dimension: v.dimension,
			distance: v.distance.into(),
			vector_type: v.vector_type.into(),
			m: v.m,
			m0: v.m0,
			ef_construction: v.ef_construction,
			ml: v.ml.into(),
			extend_candidates: v.extend_candidates,
			keep_pruned_connections: v.keep_pruned_connections,
			use_hashed_vector: v.use_hashed_vector,
		}
	}
}

/// DiskANN index parameters.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DiskAnnParams {
	/// The dimension of the index.
	pub dimension: u16,
	/// The distance metric to use.
	pub distance: Distance,
	/// The vector type to use.
	pub vector_type: VectorType,
	/// Target graph degree.
	pub degree: u16,
	/// Construction search list size.
	pub l_build: u16,
	/// DiskANN pruning alpha.
	pub alpha: Number,
	/// Whether to use vector hashes for vector retrieval.
	pub use_hashed_vector: bool,
}

impl From<sql::index::DiskAnnParams> for DiskAnnParams {
	fn from(v: sql::index::DiskAnnParams) -> Self {
		DiskAnnParams {
			dimension: v.dimension,
			distance: v.distance.into(),
			vector_type: v.vector_type.into(),
			degree: v.degree,
			l_build: v.l_build,
			alpha: v.alpha.into(),
			use_hashed_vector: v.use_hashed_vector,
		}
	}
}

impl From<DiskAnnParams> for sql::index::DiskAnnParams {
	fn from(v: DiskAnnParams) -> Self {
		Self {
			dimension: v.dimension,
			distance: v.distance.into(),
			vector_type: v.vector_type.into(),
			degree: v.degree,
			l_build: v.l_build,
			alpha: v.alpha.into(),
			use_hashed_vector: v.use_hashed_vector,
		}
	}
}

/// Runtime form of [`StoredIndexDefinition`].
///
/// `index` stays in its stored form (all index parameters are plain data);
/// the one expression it may carry, the count index's guard condition, is
/// compiled into `count_cond`, which is `None` for every other index kind
/// and for an unguarded count index. Read structure from `index`, the guard
/// from `count_cond`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexDefinition {
	pub index_id: IndexId,
	pub name: Strand,
	pub table_name: TableName,
	pub cols: Vec<Idiom>,
	pub index: Index,
	pub count_cond: Option<Cond>,
	pub prepare_remove: bool,
	pub format_version: u16,
	pub comment: Option<String>,
}

impl IndexDefinition {
	/// See [`StoredIndexDefinition::has_entry_doc_ids`]; same predicate on the
	/// compiled form.
	pub fn has_entry_doc_ids(&self) -> bool {
		matches!(self.index, Index::Idx | Index::Uniq)
			&& self.format_version >= BTREE_ENTRY_DOC_IDS_FORMAT_VERSION
	}

	/// See [`StoredIndexDefinition::uses_doc_ids`]; same predicate on the
	/// compiled form.
	pub fn uses_doc_ids(&self) -> bool {
		matches!(self.index, Index::FullText(_) | Index::Hnsw(_) | Index::DiskAnn(_))
			|| self.has_entry_doc_ids()
	}

	/// See [`StoredIndexDefinition::ensure_current_format`]; same check on the
	/// compiled form.
	pub fn ensure_current_format(&self) -> anyhow::Result<()> {
		let required = match self.index {
			Index::FullText(_) | Index::Hnsw(_) | Index::DiskAnn(_) => DOC_IDS_FORMAT_VERSION,
			Index::Idx | Index::Uniq | Index::Count(_) => 0,
		};
		if self.format_version < required {
			return Err(anyhow::Error::new(crate::catalog::Error::IndexRebuildRequired {
				index: self.name.to_string(),
				table: self.table_name.to_string(),
				expected: required,
				actual: self.format_version,
			}));
		}
		Ok(())
	}
}

impl FromStored for IndexDefinition {
	type Stored = StoredIndexDefinition;

	fn from_stored(stored: &StoredIndexDefinition) -> anyhow::Result<IndexDefinition> {
		fn build(stored: &StoredIndexDefinition) -> anyhow::Result<IndexDefinition> {
			Ok(IndexDefinition {
				index_id: stored.index_id,
				name: stored.name.clone(),
				table_name: TableName::from(stored.table_name.clone()),
				cols: stored.cols.iter().map(|t| t.compile()).collect::<anyhow::Result<_>>()?,
				index: stored.index.clone(),
				count_cond: match &stored.index {
					Index::Count(Some(cond)) => Some(cond.0.compile()?),
					_ => None,
				},
				prepare_remove: stored.prepare_remove,
				format_version: stored.format_version,
				comment: stored.comment.clone(),
			})
		}
		build(stored).with_context(|| {
			format!(
				"the stored definition of index `{}` on table `{}` no longer compiles",
				stored.name, stored.table_name
			)
		})
	}
}

impl IndexDefinition {
	pub fn to_stored(&self) -> StoredIndexDefinition {
		StoredIndexDefinition {
			index_id: self.index_id,
			name: self.name.clone(),
			table_name: self.table_name.clone().into(),
			cols: self.cols.iter().map(IdiomText::new).collect(),
			// The stored-form clone in `index` may hold a stale guard text if
			// the compiled `count_cond` was mutated, so re-render it.
			index: match &self.index {
				Index::Count(_) => {
					Index::Count(self.count_cond.as_ref().map(|c| CondText(SurqlText::new(c))))
				}
				other => other.clone(),
			},
			comment: self.comment.clone(),
			prepare_remove: self.prepare_remove,
			format_version: self.format_version,
		}
	}
}

impl From<crate::expr::index_kind::Index> for Index {
	fn from(v: crate::expr::index_kind::Index) -> Self {
		match v {
			crate::expr::index_kind::Index::Idx => Self::Idx,
			crate::expr::index_kind::Index::Uniq => Self::Uniq,
			crate::expr::index_kind::Index::Hnsw(p) => Self::Hnsw(p.into()),
			crate::expr::index_kind::Index::FullText(p) => Self::FullText(p.into()),
			crate::expr::index_kind::Index::Count(c) => {
				Self::Count(c.map(|cond| CondText(SurqlText::new(&cond))))
			}
			crate::expr::index_kind::Index::DiskAnn(p) => Self::DiskAnn(p.into()),
		}
	}
}

#[cfg(test)]
mod tests {
	use revision::{DeserializeRevisioned, SerializeRevisioned, revisioned};

	use super::*;

	#[revisioned(revision = 1)]
	#[derive(Clone, Debug, Eq, PartialEq, Hash)]
	enum OldIndex {
		Idx,
		Uniq,
		Hnsw(HnswParams),
		FullText(FullTextParams),
		// `CondText` is byte-identical to revision 1's `Cond(Expr)`, for the
		// reason its own documentation gives; only the variant list matters here.
		Count(Option<CondText>),
	}

	#[revisioned(revision = 1)]
	#[derive(Clone, Debug, Eq, PartialEq, Hash)]
	enum OldDistance {
		Chebyshev,
		Cosine,
		Euclidean,
		Hamming,
		Jaccard,
		Manhattan,
		Minkowski(Number),
		Pearson,
	}

	#[revisioned(revision = 1)]
	#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
	enum OldVectorType {
		F64,
		F32,
		I64,
		I32,
		I16,
	}

	fn revision_encode<T: SerializeRevisioned>(value: &T) -> Vec<u8> {
		let mut bytes = Vec::new();
		SerializeRevisioned::serialize_revisioned(value, &mut bytes).unwrap();
		bytes
	}

	fn revision_decode<T: DeserializeRevisioned>(bytes: &[u8]) -> T {
		DeserializeRevisioned::deserialize_revisioned(&mut &*bytes).unwrap()
	}

	#[test]
	fn revision_1_index_variants_keep_their_main_discriminants() {
		let index = revision_decode::<Index>(&revision_encode(&OldIndex::Count(None)));
		assert_eq!(index, Index::Count(None));
	}

	#[test]
	fn revision_1_distance_variants_keep_their_main_discriminants() {
		let distance = revision_decode::<Distance>(&revision_encode(&OldDistance::Pearson));
		assert_eq!(distance, Distance::Pearson);
	}

	#[test]
	fn revision_1_vector_type_variants_keep_their_main_discriminants() {
		let vector_type = revision_decode::<VectorType>(&revision_encode(&OldVectorType::I16));
		assert_eq!(vector_type, VectorType::I16);
	}
}
