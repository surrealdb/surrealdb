//! The kernels a program may call: what a capability manifest for it names.
//!
//! [`kernels`] walks a program and every unit it holds, and names the kernels
//! (`idx::kernels`) each source, instruction and index-function call reaches.

use std::collections::{BTreeSet, HashSet};

use crate::exec::vm::program::{CountSource, CountVia, Instr, Program, Source, WriteInstr};
use crate::exec::vm::write::StorageInstr;
use crate::idx::kernels::Kernel;

/// The kernels `program` may call, with those of the units it holds: what a
/// capability manifest for the program names.
///
/// - a B-tree read (an index scan, a lookup, a B-tree branch of a union or of a bitmap plan) calls
///   the bounds and entry kernels, and a count by B-tree keys the bounds kernel;
/// - a write maintaining a B-tree or `UNIQUE` index calls the entry-key kernel;
/// - a full-text read (a full-text source, a full-text branch of a union or of a bitmap plan, a
///   `MATCHES` judged on a value) calls the term kernels, and a source resolves its hits through
///   the doc-ID kernels;
/// - a bitmap source resolves its doc-IDs through the doc-ID kernels;
/// - an ANN source may rank through the exact tier, calling the query and score kernels and the
///   doc-ID kernels;
/// - a brute-force KNN ranking calls the distance kernel;
/// - `search::score`, `search::highlight` and `search::offsets` call the BM25, highlight and
///   offsets kernels.
#[cfg_attr(not(all(test, feature = "kv-mem")), allow(dead_code))]
pub(super) fn kernels(program: &Program) -> BTreeSet<Kernel> {
	let mut out = BTreeSet::new();
	let mut seen = HashSet::new();
	visit(program, &mut out, &mut seen);
	out
}

const BTREE_READ: [Kernel; 2] = [Kernel::BTreeBounds, Kernel::BTreeDecodeEntry];
const TERMS: [Kernel; 4] = [
	Kernel::FullTextTermReads,
	Kernel::FullTextTermDeltas,
	Kernel::FullTextTermDocs,
	Kernel::FullTextCombine,
];
const DOC_IDS: [Kernel; 2] = [Kernel::DocIdRecordKey, Kernel::DocIdDecodeRecordKey];

#[cfg_attr(not(all(test, feature = "kv-mem")), allow(dead_code))]
fn visit(program: &Program, out: &mut BTreeSet<Kernel>, seen: &mut HashSet<*const Program>) {
	if !seen.insert(program as *const Program) {
		return;
	}
	for pipeline in program.pipelines.iter().chain(&program.host_code) {
		source_kernels(&pipeline.source, out);
		for instr in &pipeline.code {
			match instr {
				Instr::Write(WriteInstr::Storage(StorageInstr::IndexEntries {
					..
				})) => {
					out.insert(Kernel::BTreeEntryKey);
				}
				Instr::Matches {
					..
				} => out.extend(TERMS),
				Instr::KnnRank {
					..
				} => {
					out.insert(Kernel::VectorDistance);
				}
				Instr::CallIndexFn {
					call,
					..
				} => match program.calls[*call as usize].name.as_str() {
					"search::score" => {
						out.insert(Kernel::FullTextBm25);
					}
					"search::highlight" => {
						out.insert(Kernel::FullTextHighlight);
					}
					"search::offsets" => {
						out.insert(Kernel::FullTextOffsets);
					}
					_ => {}
				},
				_ => {}
			}
		}
	}
	for unit in program.units.iter() {
		visit(unit, out, seen);
	}
}

#[cfg_attr(not(all(test, feature = "kv-mem")), allow(dead_code))]
fn source_kernels(source: &Source, out: &mut BTreeSet<Kernel>) {
	use super::plan::BranchRead;
	match source {
		Source::Index {
			..
		}
		| Source::Lookup {
			..
		} => out.extend(BTREE_READ),
		Source::IndexUnion {
			branches,
			..
		} => {
			for branch in branches {
				match branch.read {
					BranchRead::BTree(_) => out.extend(BTREE_READ),
					BranchRead::FullText {
						..
					} => {
						out.extend(TERMS);
						out.extend(DOC_IDS);
					}
				}
			}
		}
		Source::Count {
			count: CountSource::Indexed {
				via,
				..
			},
			..
		} => match via {
			CountVia::BTreeKeys {
				..
			} => {
				out.insert(Kernel::BTreeBounds);
			}
			// A cardinality reads the plan's leaves and resolves no doc-ID.
			CountVia::Bitmap(plan) => bitmap_kernels(plan, out),
			CountVia::Deltas {
				..
			} => {}
		},
		Source::Bitmap {
			root,
			fallback,
			..
		} => {
			bitmap_kernels(root, out);
			out.extend(DOC_IDS);
			if let Some(fallback) = fallback {
				source_kernels(fallback, out);
			}
		}
		Source::FullText {
			..
		} => {
			out.extend(TERMS);
			out.extend(DOC_IDS);
		}
		Source::Knn {
			..
		} => {
			out.insert(Kernel::VectorQuery);
			out.insert(Kernel::VectorScore);
			out.extend(DOC_IDS);
		}
		Source::Union(sources) => sources.iter().for_each(|s| source_kernels(s, out)),
		_ => {}
	}
}

#[cfg_attr(not(all(test, feature = "kv-mem")), allow(dead_code))]
fn bitmap_kernels(plan: &crate::exec::index::access_path::BitmapPlan, out: &mut BTreeSet<Kernel>) {
	use crate::exec::index::access_path::BitmapPlan;
	match plan {
		BitmapPlan::BTree {
			..
		} => out.extend(BTREE_READ),
		BitmapPlan::FullText {
			..
		} => out.extend(TERMS),
		BitmapPlan::Graph {
			..
		} => {}
		BitmapPlan::And(plans) | BitmapPlan::Or(plans) => {
			plans.iter().for_each(|p| bitmap_kernels(p, out))
		}
		BitmapPlan::AndNot {
			base,
			subtract,
		} => {
			bitmap_kernels(base, out);
			bitmap_kernels(subtract, out);
		}
	}
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::TestDb;
	use crate::exec::vm::tests_sources::program_of;

	const SETUP: &str = "DEFINE TABLE t SCHEMALESS;
		DEFINE INDEX t_v ON t FIELDS v;
		DEFINE INDEX t_u ON t FIELDS u UNIQUE;
		CREATE t:1 SET v = 1, u = 1;
		DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
		DEFINE TABLE d SCHEMALESS;
		DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
		CREATE d:1 SET body = 'hello world';
		DEFINE TABLE e SCHEMALESS;
		DEFINE INDEX e_emb ON e FIELDS emb HNSW DIMENSION 2 DIST EUCLIDEAN;
		CREATE e:1 SET emb = [1, 2];
		DEFINE TABLE k SCHEMAFULL;
		DEFINE FIELD n ON k TYPE int;
		DEFINE FIELD m ON k TYPE int;
		DEFINE INDEX k_n ON k FIELDS n;
		DEFINE INDEX k_m ON k FIELDS m;
		CREATE k:1 SET n = 1, m = 1;";

	async fn named(sql: &str, write: bool) -> Vec<&'static str> {
		let db = TestDb::new_with_auth(SETUP).await;
		let tt = if write {
			crate::kvs::TransactionType::Write
		} else {
			crate::kvs::TransactionType::Read
		};
		let ctx = db.exec_ctx_as(&TestDb::owner(), tt).await;
		kernels(&program_of(&ctx, sql).await).into_iter().map(Kernel::name).collect()
	}

	#[tokio::test]
	async fn a_program_names_the_kernels_its_index_reads_and_writes_call() {
		assert_eq!(
			named("SELECT * FROM t WHERE v = 1", false).await,
			["btree.bounds", "btree.decode_entry"]
		);
		assert_eq!(
			named("SELECT * FROM t WHERE u > 0", false).await,
			["btree.bounds", "btree.decode_entry"]
		);
		assert_eq!(named("SELECT * FROM t WITH NOINDEX WHERE v = 1", false).await, [] as [&str; 0]);
		assert_eq!(named("CREATE t:2 SET v = 2, u = 2", true).await, ["btree.entry_key"]);
		// A count answered by the cardinality of a bitmap over two B-tree
		// indexes reads each leaf's entries and resolves no doc-ID.
		assert_eq!(
			named("SELECT count() FROM k WHERE n = 1 AND m = 1 GROUP ALL", false).await,
			["btree.bounds", "btree.decode_entry"]
		);
	}

	#[tokio::test]
	async fn a_program_names_the_kernels_its_full_text_and_vector_reads_call() {
		// In the registry's declaration order, which is the kernels' order.
		let terms = [
			"docids.record_key",
			"docids.decode_record_key",
			"fulltext.term_reads",
			"fulltext.term_deltas",
			"fulltext.term_docs",
			"fulltext.combine",
		];
		assert_eq!(named("SELECT * FROM d WHERE body @@ 'hello'", false).await, terms);
		let scored = named(
			"SELECT search::score(1), search::highlight('<b>', '</b>', 1), search::offsets(1) \
			 FROM d WHERE body @1@ 'hello'",
			false,
		)
		.await;
		for kernel in ["fulltext.bm25", "fulltext.highlight", "fulltext.offsets"] {
			assert!(scored.contains(&kernel), "{kernel} missing from {scored:?}");
		}
		assert_eq!(
			named("SELECT * FROM e WHERE emb <|1, 10|> [1, 2]", false).await,
			["docids.record_key", "docids.decode_record_key", "vector.query", "vector.score"]
		);
		assert_eq!(
			named("SELECT * FROM e WHERE emb <|1, EUCLIDEAN|> [1, 2]", false).await,
			["vector.distance"]
		);
	}

	/// Every kernel the registry names is one some program calls, so the
	/// manifest leaves none out.
	#[tokio::test]
	async fn every_kernel_is_named_by_some_program() {
		let mut named_by_any = BTreeSet::new();
		for (sql, write) in [
			("SELECT * FROM t WHERE v = 1", false),
			("CREATE t:2 SET v = 2, u = 2", true),
			(
				"SELECT search::score(1), search::highlight('<b>', '</b>', 1), \
				 search::offsets(1) FROM d WHERE body @1@ 'hello'",
				false,
			),
			("SELECT * FROM e WHERE emb <|1, 10|> [1, 2]", false),
			("SELECT * FROM e WHERE emb <|1, EUCLIDEAN|> [1, 2]", false),
		] {
			named_by_any.extend(named(sql, write).await);
		}
		let all: BTreeSet<_> = Kernel::ALL.iter().map(|k| k.name()).collect();
		assert_eq!(named_by_any, all);
	}
}
