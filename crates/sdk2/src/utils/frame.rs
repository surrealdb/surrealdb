use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::Error;
use futures::Stream;
use pin_project_lite::pin_project;
use surrealdb_types::{
    Duration, QueryChunk, QueryResponseKind, QueryStats, QueryType, SurrealValue, Value,
};

/// A frame representing a single item from a query result stream.
pub enum QueryFrame<T: SurrealValue = Value> {
    /// A value from the query results
    Value {
        /// The query index (0-based) this value belongs to
        query: u64,
        /// The actual value
        value: T,
    },
    /// An error occurred during query execution
    Error {
        /// The query index (0-based) that failed
        query: u64,
        /// Query execution statistics
        stats: QueryStats,
        /// The error that occurred
        error: Error,
    },
    /// Query execution completed
    Done {
        /// The query index (0-based) that completed
        query: u64,
        /// Query execution statistics
        stats: QueryStats,
        /// The type of query (e.g., Live, Other)
        r#type: QueryType,
    },
}

impl<T: SurrealValue> QueryFrame<T> {
    /// Returns the query index this frame belongs to.
    pub fn query(&self) -> u64 {
        match self {
            QueryFrame::Value { query, .. } => *query,
            QueryFrame::Error { query, .. } => *query,
            QueryFrame::Done { query, .. } => *query,
        }
    }

    pub fn is_value(&self) -> bool {
        matches!(self, QueryFrame::Value { .. })
    }

    pub fn is_error(&self) -> bool {
        matches!(self, QueryFrame::Error { .. })
    }

    pub fn is_done(&self) -> bool {
        matches!(self, QueryFrame::Done { .. })
    }

    /// Extracts the value if this is a Value frame.
    pub fn into_value(self) -> Option<T> {
        match self {
            QueryFrame::Value { value, .. } => Some(value),
            _ => None,
        }
    }
}

pin_project! {
    /// A stream of [`QueryFrame`]s from query execution.
    ///
    /// This wrapper provides:
    /// 1. A generic stream of [`QueryFrame`] items with proper batching
    /// 2. Index-based filtering via [`.index()`](Self::index) to get results for a single query
    pub struct QueryStream<S: Stream<Item = QueryChunk>> {
        #[pin]
        inner: S,
        // Buffer for values from batched chunks
        buffer: VecDeque<QueryFrame>,
    }
}

impl<S: Stream<Item = QueryChunk>> QueryStream<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            buffer: VecDeque::new(),
        }
    }

    /// Returns a specialized stream for a specific query index, with type conversion.
    ///
    /// # Type Parameters
    /// - `T`: The target type to convert values to. Defaults to [`Value`].
    ///
    /// # Arguments
    /// - `index`: The zero-based query index to filter for
    ///
    /// # Example
    /// ```ignore
    /// let stream = db.query("SELECT * FROM user; SELECT * FROM post").stream().await?;
    ///
    /// // Get only the user results, typed as User
    /// let users: IndexedQueryStream<_, User> = stream.index::<User>(0);
    /// ```
    pub fn index<T: SurrealValue>(self, index: u64) -> IndexedQueryStream<S, T> {
        IndexedQueryStream {
            inner: self.inner,
            index,
            buffer: VecDeque::new(),
            _marker: PhantomData,
        }
    }
}

impl<S: Stream<Item = QueryChunk>> Stream for QueryStream<S> {
    type Item = QueryFrame;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        // First, drain any buffered frames
        if let Some(frame) = this.buffer.pop_front() {
            return Poll::Ready(Some(frame));
        }

        // Then poll the inner stream
        match this.inner.poll_next(cx) {
            Poll::Ready(Some(chunk)) => {
                chunk_to_frames(chunk, this.buffer);
                // Return the first frame from the buffer
                Poll::Ready(this.buffer.pop_front())
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project! {
    /// A filtered stream for a single query index with type conversion.
    ///
    /// Created via [`QueryStream::index()`].
    pub struct IndexedQueryStream<S: Stream<Item = QueryChunk>, T: SurrealValue = Value> {
        #[pin]
        inner: S,
        index: u64,
        buffer: VecDeque<QueryFrame<T>>,
        _marker: PhantomData<T>,
    }
}

impl<S: Stream<Item = QueryChunk>, T: SurrealValue> Stream for IndexedQueryStream<S, T> {
    type Item = QueryFrame<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // First, drain any buffered frames
        if let Some(frame) = this.buffer.pop_front() {
            return Poll::Ready(Some(frame));
        }

        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(chunk)) => {
                    if chunk.query == *this.index {
                        chunk_to_frames_typed(chunk, this.buffer);
                        if let Some(frame) = this.buffer.pop_front() {
                            return Poll::Ready(Some(frame));
                        }
                    }
                    // Skip chunks for other query indices, continue polling
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn empty_stats() -> QueryStats {
    QueryStats {
        records_received: 0,
        bytes_received: 0,
        records_scanned: 0,
        bytes_scanned: 0,
        duration: Duration::default(),
    }
}

/// Converts a QueryChunk into one or more QueryFrames, pushing them to the buffer.
fn chunk_to_frames(chunk: QueryChunk, buffer: &mut VecDeque<QueryFrame>) {
    chunk_to_frames_typed(chunk, buffer)
}

/// Converts a QueryChunk into typed QueryFrames.
fn chunk_to_frames_typed<T: SurrealValue>(chunk: QueryChunk, buffer: &mut VecDeque<QueryFrame<T>>) {
    let query = chunk.query;

    // Handle error case
    if let Some(error) = chunk.error {
        buffer.push_back(QueryFrame::Error {
            query,
            stats: chunk.stats.unwrap_or_else(empty_stats),
            error: Error::msg(error),
        });
        return;
    }

    // Handle value case - emit a frame for each value in the batch
    if let Some(results) = chunk.result {
        for value in results {
            match T::from_value(value) {
                Ok(typed) => {
                    buffer.push_back(QueryFrame::Value { query, value: typed });
                }
                Err(e) => {
                    buffer.push_back(QueryFrame::Error {
                        query,
                        stats: empty_stats(),
                        error: e,
                    });
                }
            }
        }
    }

    // Emit Done frame for final chunks
    if matches!(chunk.kind, QueryResponseKind::Single | QueryResponseKind::BatchedFinal) {
        buffer.push_back(QueryFrame::Done {
            query,
            stats: chunk.stats.unwrap_or_else(empty_stats),
            r#type: chunk.r#type.unwrap_or(QueryType::Other),
        });
    }
}
