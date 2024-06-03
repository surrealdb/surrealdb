use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::ft::doclength::{DocLength, DocLengths};
use crate::idx::ft::postings::{Postings, TermFrequency};
use crate::idx::ft::termdocs::TermsDocs;
use crate::idx::ft::Bm25Params;
use crate::kvs::Transaction;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(super) type Score = f32;

pub(crate) struct BM25Scorer {
	postings: Arc<RwLock<Postings>>,
	terms_docs: TermsDocs,
	doc_lengths: Arc<RwLock<DocLengths>>,
	average_doc_length: f32,
	doc_count: f32,
	bm25: Bm25Params,
}

impl BM25Scorer {
	pub(super) fn new(
		postings: Arc<RwLock<Postings>>,
		terms_docs: TermsDocs,
		doc_lengths: Arc<RwLock<DocLengths>>,
		total_docs_length: u128,
		doc_count: u64,
		bm25: Bm25Params,
	) -> Self {
		Self {
			postings,
			terms_docs,
			doc_lengths,
			average_doc_length: (total_docs_length as f32) / (doc_count as f32),
			doc_count: doc_count as f32,
			bm25,
		}
	}

	async fn term_score(
		&self,
		tx: &mut Transaction,
		doc_id: DocId,
		term_doc_count: DocLength,
		term_frequency: TermFrequency,
	) -> Result<Score, Error> {
		let dl = self.doc_lengths.read().await;
		let doc_length = dl.get_doc_length(tx, doc_id).await?.unwrap_or(0);
		drop(dl);
		Ok(self.compute_bm25_score(term_frequency as f32, term_doc_count as f32, doc_length as f32))
	}

	pub(crate) async fn score(
		&self,
		tx: &mut Transaction,
		doc_id: DocId,
	) -> Result<Option<Score>, Error> {
		let mut sc = 0.0;
		let p = self.postings.read().await;
		for (term_id, docs) in self.terms_docs.iter().flatten() {
			if docs.contains(doc_id) {
				let tf = p.get_term_frequency(tx, *term_id, doc_id).await?;
				if let Some(term_freq) = tf {
					sc += self.term_score(tx, doc_id, docs.len(), term_freq).await?;
				}
			}
		}
		drop(p);
		Ok(Some(sc))
	}

	// https://en.wikipedia.org/wiki/Okapi_BM25
	// Including the lower-bounding term frequency normalization (2011 CIKM)
	fn compute_bm25_score(&self, term_freq: f32, term_doc_count: f32, doc_length: f32) -> f32 {
		// (n(qi) + 0.5)
		let denominator = term_doc_count + 0.5;
		// (N - n(qi) + 0.5)
		let numerator = self.doc_count - term_doc_count + 0.5;
		let idf = (numerator / denominator).ln();
		if idf.is_nan() {
			return f32::NAN;
		}
		let tf_prim = 1.0 + term_freq.ln();
		// idf * (k1 + 1)
		let numerator = idf * (self.bm25.k1 + 1.0) * tf_prim;
		// 1 - b + b * (|D| / avgDL)
		let denominator = 1.0 - self.bm25.b + self.bm25.b * (doc_length / self.average_doc_length);
		// numerator / (k1 * denominator + 1)
		numerator / (self.bm25.k1 * denominator + 1.0)
	}
}
