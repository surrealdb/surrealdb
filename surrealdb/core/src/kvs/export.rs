use std::fmt;

use anyhow::Result;
use async_channel::Sender;
pub use surrealdb_rpc::export::{Config, ExcludedTables, TableConfig};
use surrealdb_types::ToSql;

use super::Transaction;
use crate::catalog::providers::{
	ApiProvider, AuthorisationProvider, BucketProvider, DatabaseProvider, TableProvider,
	UserProvider,
};
use crate::catalog::{DatabaseId, Error, NamespaceId, Record, TableDefinition};
use crate::expr::access::AccessDuration;
use crate::expr::access_type::{
	AccessType, BearerAccess, BearerAccessSubject, BearerAccessType, JwtAccess, JwtAccessIssue,
	JwtAccessVerify, JwtAccessVerifyJwks, JwtAccessVerifyKey, RecordAccess,
};
use crate::expr::paths::{IN, OUT};
use crate::expr::statements::define::{DefineAccessStatement, DefineKind, DefineUserStatement};
use crate::expr::user::UserDuration;
use crate::expr::{Algorithm, Base, DefineAnalyzerStatement, Expr, Idiom, Literal};
use crate::idx::IndexKeyBase;
use crate::idx::ft::fulltext::mean_tokens_per_document;
use crate::key::schema::{RecordKey, RecordPrefix};
use crate::key::{KVKeyDecode, KVSubspace, KVValue};
use crate::sql::statements::OptionStatement;
use crate::val::TableName;
use crate::{catalog, val};

struct InlineCommentWriter<'a, F>(&'a mut F);
impl<F: fmt::Write> fmt::Write for InlineCommentWriter<'_, F> {
	fn write_str(&mut self, s: &str) -> fmt::Result {
		for c in s.chars() {
			self.write_char(c)?
		}
		Ok(())
	}

	fn write_char(&mut self, c: char) -> fmt::Result {
		match c {
			'\n' => self.0.write_str("\\n"),
			'\r' => self.0.write_str("\\r"),
			// NEL/Next Line
			'\u{0085}' => self.0.write_str("\\u{0085}"),
			// line separator
			'\u{2028}' => self.0.write_str("\\u{2028}"),
			// Paragraph separator
			'\u{2029}' => self.0.write_str("\\u{2029}"),
			_ => self.0.write_char(c),
		}
	}
}

struct InlineCommentDisplay<F>(F);
impl<F: fmt::Display> fmt::Display for InlineCommentDisplay<F> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		fmt::Write::write_fmt(&mut InlineCommentWriter(f), format_args!("{}", self.0))
	}
}

/// Writes the full database contents as binary SQL.
pub(crate) async fn export(
	tx: &Transaction,
	ns: &str,
	db: &str,
	cfg: Config,
	batch_size: u32,
	chn: Sender<Vec<u8>>,
) -> Result<()> {
	let db = tx.get_db_by_name(ns, db, None).await?.ok_or_else(|| {
		anyhow::Error::new(Error::DbNotFound {
			name: db.to_owned(),
		})
	})?;

	// Output USERS, ACCESSES, PARAMS, FUNCTIONS, ANALYZERS
	export_metadata(tx, &cfg, &chn, db.namespace_id, db.database_id).await?;
	// Output TABLES
	export_tables(tx, &cfg, &chn, db.namespace_id, db.database_id, batch_size).await?;
	Ok(())
}

async fn export_metadata(
	tx: &Transaction,
	cfg: &Config,
	chn: &Sender<Vec<u8>>,
	ns: NamespaceId,
	db: DatabaseId,
) -> Result<()> {
	// Output OPTIONS
	export_section("OPTION", [OptionStatement::import()].into_iter(), chn).await?;

	// Output USERS
	if cfg.users {
		let users = tx.all_db_users(ns, db, None).await?;
		export_section(
			"USERS",
			users.iter().map(|x| define_user_statement_from_definition(Base::Db, x)),
			chn,
		)
		.await?;
	}

	// Output ACCESSES
	if cfg.accesses {
		let accesses = tx.all_db_accesses(ns, db, None).await?;
		export_section(
			"ACCESSES",
			accesses.iter().map(|x| define_access_statement_from_definition(Base::Db, x).redact()),
			chn,
		)
		.await?;
	}

	// Output PARAMS
	if cfg.params {
		let params = tx.all_db_params(ns, db, None).await?;
		export_section("PARAMS", params.iter(), chn).await?;
	}

	// Output FUNCTIONS
	if cfg.functions {
		let functions = tx.all_db_functions(ns, db, None).await?;
		export_section("FUNCTIONS", functions.iter(), chn).await?;
	}

	// Output ANALYZERS
	if cfg.analyzers {
		let analyzers = tx.all_db_analyzers(ns, db, None).await?;
		export_section(
			"ANALYZERS",
			analyzers.iter().map(define_analyzer_statement_from_definition),
			chn,
		)
		.await?;
	}

	// Output APIS
	if cfg.apis {
		let apis = tx.all_db_apis(ns, db, None).await?;
		export_section("APIS", apis.iter(), chn).await?;
	}

	// Output BUCKETS
	if cfg.buckets {
		let buckets = tx.all_db_buckets(ns, db, None).await?;
		export_section("BUCKETS", buckets.iter(), chn).await?;
	}

	// Output MODULES
	if cfg.modules {
		let modules = tx.all_db_modules(ns, db, None).await?;
		export_section("MODULES", modules.iter(), chn).await?;
	}

	// Output CONFIGS
	if cfg.configs {
		let configs = tx.all_db_configs(ns, db, None).await?;
		export_section("CONFIGS", configs.iter(), chn).await?;
	}

	// Output SEQUENCES
	if cfg.sequences {
		let sequences = tx.all_db_sequences(ns, db, None).await?;
		export_section("SEQUENCES", sequences.iter(), chn).await?;
	}

	Ok(())
}

async fn export_section<T>(
	title: &str,
	items: impl ExactSizeIterator<Item = T>,
	chn: &Sender<Vec<u8>>,
) -> Result<()>
where
	T: ToSql,
{
	if items.len() == 0 {
		return Ok(());
	}

	chn.send(bytes!("-- ------------------------------")).await?;
	chn.send(bytes!(format!("-- {}", InlineCommentDisplay(title)))).await?;
	chn.send(bytes!("-- ------------------------------")).await?;
	chn.send(bytes!("")).await?;

	for item in items {
		chn.send(bytes!(format!("{};", item.to_sql()))).await?;
	}

	chn.send(bytes!("")).await?;
	Ok(())
}

async fn export_tables(
	tx: &Transaction,
	cfg: &Config,
	chn: &Sender<Vec<u8>>,
	ns: NamespaceId,
	db: DatabaseId,
	batch_size: u32,
) -> Result<()> {
	// Check if tables are included in the export config
	if !cfg.tables.is_any() {
		return Ok(());
	}
	// Fetch all of the tables for this NS / DB
	let tables = tx.all_tb(ns, db, None).await?;
	// Warn if any specified table names don't match existing tables
	if let Some(names) = cfg.tables.names() {
		let existing: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
		for name in names {
			if !existing.contains(&name.as_str()) {
				warn!("Table '{name}' does not exist in the database");
			}
		}
	}
	// Loop over all of the tables in order
	for table in tables.iter() {
		// Check if this table is included in the export config
		if !cfg.tables.includes(table.name.as_str()) {
			continue;
		}
		// Export the table definition structure first
		export_table_structure(tx, ns, db, table, chn).await?;
		// Then export the table data if its desired
		if cfg.records {
			export_table_data(tx, ns, db, table, chn, batch_size).await?;
		}
	}

	Ok(())
}

async fn export_table_structure(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableDefinition,
	chn: &Sender<Vec<u8>>,
) -> Result<()> {
	chn.send(bytes!("-- ------------------------------")).await?;
	chn.send(bytes!(format!("-- TABLE: {}", InlineCommentDisplay(&table.name)))).await?;
	chn.send(bytes!("-- ------------------------------")).await?;
	chn.send(bytes!("")).await?;
	chn.send(bytes!(format!("{};", table.to_sql()))).await?;
	chn.send(bytes!("")).await?;
	let tb_name = table.name.clone();
	// Export all table field definitions with OVERWRITE to ensure
	// idempotent re-import (relation tables auto-generate in/out fields,
	// and array types generate sub-field definitions that would conflict).
	let fields = tx.all_tb_fields(ns, db, &tb_name, None).await?;
	for field in fields.iter() {
		chn.send(bytes!(format!("{};", field.to_sql_overwrite()))).await?;
	}
	chn.send(bytes!("")).await?;
	// Export all table index definitions for this table
	let indexes = tx.all_tb_indexes(ns, db, &tb_name, None).await?;
	for index in indexes.iter() {
		chn.send(bytes!(format!("{};", index.to_sql()))).await?;
	}
	chn.send(bytes!("")).await?;
	// Export all table event definitions for this table
	let events = tx.all_tb_events(ns, db, &tb_name, None).await?;
	for event in events.iter() {
		chn.send(bytes!(format!("{};", event.to_sql()))).await?;
	}
	chn.send(bytes!("")).await?;
	// Everything ok
	Ok(())
}

/// Target number of KV keys the records of one emitted `INSERT` should write on
/// re-import.
///
/// An import executes one statement per transaction, so how many records an
/// `INSERT` line carries decides how large a transaction is on the way back in.
/// What a transaction costs is not its record count but the number of keys those
/// records write, and a table's index set moves that by two orders of magnitude:
/// a bare record writes 2 keys, eight b-tree indexes take it to 12, and a single
/// full-text index to ~170. Sizing by keys keeps a transaction's cost roughly
/// constant across index sets instead of leaving it to vary with the schema.
///
/// The value is chosen so a table whose records write few keys keeps using the
/// whole `export_batch_size` — the cap below — and only a table carrying an
/// index whose per-record key count is large groups its records more tightly.
const INSERT_KEY_BUDGET: usize = 20_000;

/// Keys a record writes for itself, independent of any index.
const KEYS_PER_RECORD: usize = 2;

/// Keys a record writes per index whose per-record key count is fixed and small:
/// plain, unique, and count indexes each write a bounded number of entries.
const KEYS_PER_VALUE_INDEX: usize = 2;

/// Keys a full-text index writes per indexed term: the posting.
///
/// The delta log a term also contributes to is batched per transaction rather
/// than per (term, document), so its cost is bounded by the statement's distinct
/// vocabulary and amortises to well under one key per term as the group grows.
/// Counting it here would shrink the group for keys the group does not write.
const KEYS_PER_FULLTEXT_TERM: usize = 1;

/// Fallback allowance for one index whose per-record key count scales with the
/// indexed content rather than being fixed, used when the index carries no
/// statistic to derive it from — an index with no documents yet, or a vector
/// index, whose per-record cost follows graph connectivity rather than a length
/// the index records.
///
/// Deliberately generous. Overestimating only groups records more tightly, and
/// throughput is flat across a wide range of small groupings, whereas
/// underestimating leaves the oversized transaction in place — so the error that
/// costs nothing is the one to make.
const KEYS_PER_CONTENT_INDEX_FALLBACK: usize = 256;

/// Records sampled from a batch to price a value index's fan-out.
///
/// A table's records are usually alike in shape, so a handful is enough to tell
/// a scalar field from an array one and to size the array within an order of
/// magnitude. The sample is taken per batch, so a table whose shape varies down
/// its key range is re-priced as the export walks it.
const FAN_OUT_SAMPLE: usize = 8;

/// The number of index entries one record produces for an index over `cols`.
///
/// A value index over an array-valued field does not write one entry: the
/// indexer expands a non-flattened array into one entry per element, so a
/// record with a 500-element `tags` array writes 500 entries to a single index.
/// Ignoring that would let exactly the schema this change exists to bound —
/// records that write far more keys than their count suggests — keep producing
/// oversized transactions.
///
/// Takes the largest fan-out across the index's columns rather than their
/// product: the combinator advances one column per step, so the entry count
/// tracks the longest array rather than every combination of them.
fn index_fan_out(data: &val::Value, cols: &[Idiom]) -> usize {
	cols.iter()
		.map(|col| match data.pick(col) {
			val::Value::Array(a) => a.len().max(1),
			_ => 1,
		})
		.max()
		.unwrap_or(1)
}

/// Estimates the keys one record of `table` writes, given its index set and a
/// sample of the records about to be emitted.
///
/// Two index kinds cost more per record than the schema alone reveals, and both
/// are resolved from data rather than guessed:
///
/// - A full-text index writes per distinct term, so a table of 8 KB documents costs an order of
///   magnitude more per record than one of 700 B documents. Derived from the mean document length
///   the index already maintains for BM25 scoring, using the token mean as a stand-in for the
///   distinct-term count — which overestimates, since documents repeat words, in the direction that
///   costs nothing.
/// - A value index over an array field writes one entry per element. Derived from the sampled
///   records, since no statistic records it.
///
/// Falls back to a fixed allowance where neither is available: a full-text index
/// with no documents to average, and the vector indexes, whose per-record cost
/// follows graph connectivity rather than anything visible here.
async fn keys_per_record(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	indexes: &[catalog::IndexDefinition],
	sample: &[val::Value],
) -> Result<usize> {
	let mut keys = KEYS_PER_RECORD;
	for ix in indexes {
		keys += match ix.index {
			catalog::Index::FullText(_) => {
				let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
				match mean_tokens_per_document(tx, &ikb).await? {
					Some(mean) => (mean as usize).saturating_mul(KEYS_PER_FULLTEXT_TERM),
					None => KEYS_PER_CONTENT_INDEX_FALLBACK,
				}
			}
			catalog::Index::Hnsw(_) | catalog::Index::DiskAnn(_) => KEYS_PER_CONTENT_INDEX_FALLBACK,
			catalog::Index::Idx | catalog::Index::Uniq | catalog::Index::Count(_) => {
				let fan_out =
					sample.iter().map(|data| index_fan_out(data, &ix.cols)).max().unwrap_or(1);
				KEYS_PER_VALUE_INDEX.saturating_mul(fan_out)
			}
		};
	}
	Ok(keys)
}

/// Decodes up to `limit` records from the head of a scan batch, for sizing.
///
/// Sizing has to look at record content, and the alternative to decoding a few
/// twice is threading decoded records through the emit path — which would hold a
/// whole batch's `Value`s alive for the sake of a handful used to measure.
fn decode_sample(batch: &[(Vec<u8>, Vec<u8>)], limit: usize) -> Result<Vec<val::Value>> {
	batch
		.iter()
		.take(limit)
		.map(|(k, v)| {
			let k = RecordKey::decode_key(k)?;
			let rid = crate::val::RecordId {
				table: k.tb.into_owned(),
				key: k.id.into_owned(),
			};
			Ok(Record::kv_decode_value(v, rid)?.data)
		})
		.collect()
}

/// Chooses how many records to put in one emitted `INSERT`, from the keys the
/// table's index set makes each record write.
///
/// Never exceeds `batch_size`, so `export_batch_size` remains the bound a
/// deployment can already configure and this only ever groups more tightly than
/// it asks for.
fn records_per_insert(keys_per_record: usize, batch_size: u32) -> usize {
	(INSERT_KEY_BUDGET / keys_per_record.max(1)).clamp(1, batch_size.max(1) as usize)
}

async fn export_table_data(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableDefinition,
	chn: &Sender<Vec<u8>>,
	batch_size: u32,
) -> Result<()> {
	chn.send(bytes!("-- ------------------------------")).await?;
	chn.send(bytes!(format!("-- TABLE DATA: {}", InlineCommentDisplay(&table.name)))).await?;
	chn.send(bytes!("-- ------------------------------")).await?;
	chn.send(bytes!("")).await?;

	let tb_name = table.name.clone();
	// How many records one `INSERT` carries is a separate question from how many
	// keys one scan reads: the scan size trades KV round trips against memory,
	// while the `INSERT` size sets the size of a transaction on re-import. The
	// index set only bears on the second.
	let indexes = tx.all_tb_indexes(ns, db, &tb_name, None).await?;
	// A record's value decodes only with its own key's record id, so the table's
	// records are read as bytes and decoded per key by `export_regular_data`.
	let records = RecordPrefix {
		ns,
		db,
		tb: std::borrow::Cow::Borrowed(&tb_name),
	};
	let mut next = Some(records.range()?);

	while let Some(rng) = next {
		let batch = tx.batch_keys_vals_raw(rng, batch_size, None).await?;
		// What a batch leaves unread is a tail of the same region, so the bound that
		// produced the range is the one that wraps the continuation.
		next = batch.next.map(|rng| records.raw(rng));
		// If there are no values, return early.
		if batch.result.is_empty() {
			break;
		}
		// Price this batch from its own records. Sizing needs to see the data
		// because a value index over an array field writes one entry per
		// element, which the schema does not state.
		let sample = decode_sample(&batch.result, FAN_OUT_SAMPLE)?;
		let per_insert = records_per_insert(
			keys_per_record(tx, ns, db, &tb_name, &indexes, &sample).await?,
			batch_size,
		);
		for group in batch.result.chunks(per_insert) {
			export_regular_data(group, chn).await?;
		}
	}

	chn.send(bytes!("")).await?;
	Ok(())
}

/// Processes a record and categorizes it for SQL export.
///
/// This function processes a record, categorizing it into either normal
/// records or graph edge records, and writes it to the appropriate string
/// buffer for later SQL generation.
///
/// Note: Only the latest version of each record is exported. Historical
/// versions must be exported at the KV level.
///
/// # Arguments
///
/// * `record` - The record to be processed. The `id` field must already be present in `data` (this
///   is the case when the record was produced by [`Record::kv_decode_value_with_id`]).
/// * `records_relate` - A mutable reference to a string buffer for graph edge records.
/// * `records_normal` - A mutable reference to a string buffer for normal records.
fn process_record(record: &Record, records_relate: &mut String, records_normal: &mut String) {
	// Match on the value to determine if it is a graph edge record or a normal record.
	if record.is_edge()
		&& let crate::val::Value::RecordId(_) = record.data.pick(&IN)
		&& let crate::val::Value::RecordId(_) = record.data.pick(&OUT)
	{
		// If the value is a graph edge record (indicated by EDGE, IN, and OUT fields):
		// Write the value to the records_relate string.
		if !records_relate.is_empty() {
			records_relate.push_str(", ");
		}
		records_relate.push_str(&record.data.to_sql());
	} else {
		// If the value is a normal record, write it to the records_normal string.
		if !records_normal.is_empty() {
			records_normal.push_str(", ");
		}
		records_normal.push_str(&record.data.to_sql());
	}
}

/// Exports regular data to the provided channel.
///
/// This function processes a list of regular values, converting them into
/// SQL commands and sending them to the provided channel. It handles both
/// normal records and graph edge records, and ensures that the appropriate
/// SQL commands are generated for each type of record.
///
/// # Arguments
///
/// * `regular_values` - A vector of tuples containing the regular values to be exported. Each tuple
///   consists of a key and a value.
/// * `chn` - A reference to the channel to which the SQL commands will be sent.
///
/// # Returns
///
/// * `Result<()>` - Returns `Ok(())` if the operation is successful, or an `Error` if an error
///   occurs.
async fn export_regular_data(
	regular_values: &[(Vec<u8>, Vec<u8>)],
	chn: &Sender<Vec<u8>>,
) -> Result<()> {
	// Initialize strings to hold normal records and graph edge records.
	// Write directly to strings to avoid unnecessary allocations.
	let mut records_normal = String::new();
	let mut records_relate = String::new();

	// Process each regular value.
	for (k, v) in regular_values {
		let k = RecordKey::decode_key(k)?;
		let rid = crate::val::RecordId {
			table: k.tb.into_owned(),
			key: k.id.into_owned(),
		};
		let v = Record::kv_decode_value(v, rid)?;
		// Process the value and categorize it into records_relate or records_normal.
		process_record(&v, &mut records_relate, &mut records_normal);
	}

	// If there are normal records, generate and send the INSERT SQL command.
	if !records_normal.is_empty() {
		let sql = format!("INSERT [ {} ];", records_normal);
		chn.send(bytes!(sql)).await?;
	}

	// If there are graph edge records, generate and send the INSERT RELATION SQL
	// command.
	if !records_relate.is_empty() {
		let sql = format!("INSERT RELATION [ {} ];", records_relate);
		chn.send(bytes!(sql)).await?;
	}

	Ok(())
}

pub(crate) fn define_access_statement_from_definition(
	base: Base,
	def: &catalog::AccessDefinition,
) -> DefineAccessStatement {
	fn convert_algorithm(access: catalog::Algorithm) -> Algorithm {
		match &access {
			catalog::Algorithm::EdDSA => Algorithm::EdDSA,
			catalog::Algorithm::Es256 => Algorithm::Es256,
			catalog::Algorithm::Es384 => Algorithm::Es384,
			catalog::Algorithm::Es512 => Algorithm::Es512,
			catalog::Algorithm::Hs256 => Algorithm::Hs256,
			catalog::Algorithm::Hs384 => Algorithm::Hs384,
			catalog::Algorithm::Hs512 => Algorithm::Hs512,
			catalog::Algorithm::Ps256 => Algorithm::Ps256,
			catalog::Algorithm::Ps384 => Algorithm::Ps384,
			catalog::Algorithm::Ps512 => Algorithm::Ps512,
			catalog::Algorithm::Rs256 => Algorithm::Rs256,
			catalog::Algorithm::Rs384 => Algorithm::Rs384,
			catalog::Algorithm::Rs512 => Algorithm::Rs512,
		}
	}

	fn convert_jwt_access(access: &catalog::JwtAccess) -> JwtAccess {
		JwtAccess {
			verify: match &access.verify {
				catalog::JwtAccessVerify::Key(k) => JwtAccessVerify::Key(JwtAccessVerifyKey {
					alg: convert_algorithm(k.alg),
					key: Expr::Literal(Literal::String(k.key.as_str().into())),
				}),
				catalog::JwtAccessVerify::Jwks(j) => JwtAccessVerify::Jwks(JwtAccessVerifyJwks {
					url: Expr::Literal(Literal::String(j.url.as_str().into())),
				}),
			},
			issue: access.issue.as_ref().map(|x| JwtAccessIssue {
				alg: convert_algorithm(x.alg),
				key: Expr::Literal(Literal::String(x.key.as_str().into())),
			}),
		}
	}

	fn convert_bearer_access(access: &catalog::BearerAccess) -> BearerAccess {
		BearerAccess {
			kind: match access.kind {
				catalog::BearerAccessType::Bearer => BearerAccessType::Bearer,
				catalog::BearerAccessType::Refresh => BearerAccessType::Refresh,
			},
			subject: match access.subject {
				catalog::BearerAccessSubject::Record => BearerAccessSubject::Record,
				catalog::BearerAccessSubject::User => BearerAccessSubject::User,
			},
			jwt: convert_jwt_access(&access.jwt),
		}
	}

	DefineAccessStatement {
		kind: DefineKind::Default,
		base,
		name: Expr::Idiom(Idiom::field(def.name.clone())),
		duration: AccessDuration {
			grant: def
				.grant_duration
				.map(|v| Expr::Literal(Literal::Duration(val::Duration(v))))
				.unwrap_or(Expr::Literal(Literal::None)),
			token: def
				.token_duration
				.map(|v| Expr::Literal(Literal::Duration(val::Duration(v))))
				.unwrap_or(Expr::Literal(Literal::None)),
			session: def
				.session_duration
				.map(|v| Expr::Literal(Literal::Duration(val::Duration(v))))
				.unwrap_or(Expr::Literal(Literal::None)),
		},
		comment: def
			.comment
			.clone()
			.map(|x| Expr::Literal(Literal::String(x.into())))
			.unwrap_or(Expr::Literal(Literal::None)),
		authenticate: def.authenticate.clone(),
		access_type: match &def.access_type {
			catalog::AccessType::Record(record_access) => {
				AccessType::Record(Box::new(RecordAccess {
					signup: def.signup.clone(),
					signin: def.signin.clone(),
					jwt: convert_jwt_access(&record_access.jwt),
					bearer: record_access.bearer.as_ref().map(convert_bearer_access),
				}))
			}
			catalog::AccessType::Jwt(jwt_access) => AccessType::Jwt(convert_jwt_access(jwt_access)),
			catalog::AccessType::Bearer(bearer_access) => {
				AccessType::Bearer(convert_bearer_access(bearer_access))
			}
		},
	}
}

pub(crate) fn define_analyzer_statement_from_definition(
	def: &catalog::AnalyzerDefinition,
) -> DefineAnalyzerStatement {
	DefineAnalyzerStatement {
		kind: DefineKind::Default,
		name: Expr::Idiom(Idiom::field(def.name.clone())),
		function: def.function.clone(),
		tokenizers: def.tokenizers.clone(),
		filters: def.filters.clone(),
		comment: def
			.comment
			.as_ref()
			.map(|x| Expr::Literal(Literal::String(x.as_str().into())))
			.unwrap_or(Expr::Literal(Literal::None)),
	}
}

pub(crate) fn define_user_statement_from_definition(
	base: Base,
	def: &catalog::UserDefinition,
) -> DefineUserStatement {
	DefineUserStatement {
		kind: DefineKind::Default,
		base,
		name: Expr::Idiom(Idiom::field(def.name.clone())),
		hash: def.hash.clone(),
		code: def.code.clone(),
		roles: def.roles.clone(),
		duration: UserDuration {
			token: def
				.token_duration
				.map(|x| Expr::Literal(Literal::Duration(val::Duration(x))))
				.unwrap_or(Expr::Literal(Literal::None)),
			session: def
				.session_duration
				.map(|x| Expr::Literal(Literal::Duration(val::Duration(x))))
				.unwrap_or(Expr::Literal(Literal::None)),
		},
		comment: def
			.comment
			.as_ref()
			.map(|x| Expr::Idiom(Idiom::field(x.clone())))
			.unwrap_or(Expr::Literal(Literal::None)),
		scram: def.scram.clone(),
	}
}
