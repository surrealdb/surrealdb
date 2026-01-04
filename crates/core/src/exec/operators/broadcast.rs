use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use tokio::sync::{RwLock, watch};

use crate::err::Error;
use crate::exec::{
	AccessMode, ContextLevel, ExecutionContext, OperatorPlan, ValueBatch, ValueBatchStream,
};

/// A source that executes once, caches incrementally, and allows multiple
/// consumers to read at their own pace—getting cached data immediately
/// or waiting for new data if they've caught up to the producer.
///
/// This is useful for multi-use subquery parameters where the same query
/// result is consumed by multiple downstream operators.
///
/// # Example
/// ```sql
/// LET $a = SELECT * FROM person;
/// SELECT * FROM $a WHERE age > 18;  -- Consumer 1
/// SELECT * FROM $a WHERE age <= 18; -- Consumer 2
/// ```
///
/// Without BroadcastSource, the subquery would execute twice. With BroadcastSource,
/// it executes once and both consumers read from the shared cache.
#[derive(Debug, Clone)]
pub struct BroadcastSource {
	input: Arc<dyn OperatorPlan>,
	state: Arc<BroadcastState>,
}

struct BroadcastState {
	/// Has the producer been started?
	started: AtomicBool,
	/// Incrementally growing cache of batches
	cache: RwLock<Vec<ValueBatch>>,
	/// Is the producer finished?
	complete: AtomicBool,
	/// Did the producer encounter an error?
	error: RwLock<Option<anyhow::Error>>,
	/// Notifies consumers of state changes - increments on each change
	changed: watch::Sender<usize>,
}

impl std::fmt::Debug for BroadcastState {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("BroadcastState")
			.field("started", &self.started.load(Ordering::SeqCst))
			.field("complete", &self.complete.load(Ordering::SeqCst))
			.finish()
	}
}

impl BroadcastSource {
	pub fn new(input: Arc<dyn OperatorPlan>) -> Self {
		let (tx, _rx) = watch::channel(0);
		Self {
			input,
			state: Arc::new(BroadcastState {
				started: AtomicBool::new(false),
				cache: RwLock::new(Vec::new()),
				complete: AtomicBool::new(false),
				error: RwLock::new(None),
				changed: tx,
			}),
		}
	}

	async fn run_producer(
		input: Arc<dyn OperatorPlan>,
		ctx: &ExecutionContext,
		state: &BroadcastState,
	) -> Result<(), anyhow::Error> {
		let mut stream =
			input.execute(ctx).map_err(|e| anyhow::anyhow!("Failed to execute input: {}", e))?;

		use futures::StreamExt;

		let mut batch_num = 0;
		while let Some(batch_result) = stream.next().await {
			let batch = match batch_result {
				Ok(batch) => batch,
				Err(crate::expr::ControlFlow::Err(e)) => {
					return Err(e);
				}
				Err(crate::expr::ControlFlow::Continue) => continue,
				Err(crate::expr::ControlFlow::Break) => break,
				Err(crate::expr::ControlFlow::Return(_)) => break,
			};

			// Append to cache
			{
				let mut cache = state.cache.write().await;
				cache.push(batch);
			}

			// Notify consumers that state changed (send the batch number to ensure value changes)
			batch_num += 1;
			let _ = state.changed.send(batch_num);
		}

		Ok(())
	}
}

#[async_trait]
impl OperatorPlan for BroadcastSource {
	fn name(&self) -> &'static str {
		"BroadcastSource"
	}

	fn required_context(&self) -> ContextLevel {
		// Forward the context requirement from the input plan
		self.input.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		// Forward the access mode from the input plan
		self.input.access_mode()
	}

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		let needs_spawn = !self.state.started.swap(true, Ordering::SeqCst);

		// Return a cursor stream into the shared cache
		Ok(make_cursor_stream(
			self.state.clone(),
			self.state.changed.subscribe(),
			needs_spawn,
			ctx.clone(),
			if needs_spawn {
				Some(self.input.clone())
			} else {
				None
			},
		))
	}

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		vec![&self.input]
	}
}

/// State for a cursor reading from a BroadcastSource
struct CursorState {
	state: Arc<BroadcastState>,
	position: usize,
	changed_rx: watch::Receiver<usize>,
}

/// Creates a stream that reads from the broadcast cache.
fn make_cursor_stream(
	state: Arc<BroadcastState>,
	changed_rx: watch::Receiver<usize>,
	needs_spawn: bool,
	ctx: ExecutionContext,
	input: Option<Arc<dyn OperatorPlan>>,
) -> ValueBatchStream {
	let cursor_state = CursorState {
		state: state.clone(),
		position: 0,
		changed_rx,
	};

	// Use unfold to properly handle async state machine
	let stream = futures::stream::unfold(
		(cursor_state, needs_spawn, Some(ctx), input),
		|(mut cursor, mut needs_spawn, ctx, input)| async move {
			// Spawn producer on first iteration if needed
			if needs_spawn {
				needs_spawn = false;
				if let (Some(input), Some(ctx)) = (input.as_ref(), ctx.as_ref()) {
					let state = cursor.state.clone();
					let ctx = ctx.clone();
					let input = input.clone();
					tokio::spawn(async move {
						let result = BroadcastSource::run_producer(input, &ctx, &state).await;
						if let Err(e) = result {
							*state.error.write().await = Some(e);
						}
						state.complete.store(true, Ordering::SeqCst);
						let _ = state.changed.send(usize::MAX);
					});
				}
			}

			loop {
				// Check cache first
				let maybe_batch = {
					let cache = cursor.state.cache.read().await;
					if cursor.position < cache.len() {
						Some(cache[cursor.position].clone())
					} else {
						None
					}
				};
				if let Some(batch) = maybe_batch {
					cursor.position += 1;
					return Some((Ok(batch), (cursor, needs_spawn, None, None)));
				}

				// Check for error (clone error message to avoid borrow issues)
				let error_msg = {
					let error_guard = cursor.state.error.read().await;
					error_guard.as_ref().map(|e| e.to_string())
				};
				if let Some(msg) = error_msg {
					return Some((
						Err(crate::expr::ControlFlow::Err(anyhow::anyhow!("{}", msg))),
						(cursor, needs_spawn, None, None),
					));
				}

				// Check if complete
				if cursor.state.complete.load(Ordering::Acquire) {
					// Final check for any remaining data (in case of race)
					let maybe_batch = {
						let cache = cursor.state.cache.read().await;
						if cursor.position < cache.len() {
							Some(cache[cursor.position].clone())
						} else {
							None
						}
					};
					if let Some(batch) = maybe_batch {
						cursor.position += 1;
						return Some((Ok(batch), (cursor, needs_spawn, None, None)));
					}
					return None;
				}

				// No data available, not complete - wait for notification
				if cursor.changed_rx.changed().await.is_err() {
					// Channel closed, producer dropped
					return None;
				}
			}
		},
	);

	Box::pin(stream)
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use tokio_util::sync::CancellationToken;

	use super::*;
	use crate::exec::RootContext;
	use crate::iam::Auth;
	use crate::val::Value;

	/// Mock execution plan that produces a known sequence of batches
	#[derive(Debug, Clone)]
	struct MockPlan {
		batches: Arc<Vec<Vec<Value>>>,
		delay_ms: Option<u64>,
	}

	impl MockPlan {
		fn new(batches: Vec<Vec<Value>>) -> Self {
			Self {
				batches: Arc::new(batches),
				delay_ms: None,
			}
		}

		fn with_delay(mut self, delay_ms: u64) -> Self {
			self.delay_ms = Some(delay_ms);
			self
		}
	}

	impl OperatorPlan for MockPlan {
		fn name(&self) -> &'static str {
			"MockPlan"
		}

		fn required_context(&self) -> ContextLevel {
			ContextLevel::Root
		}

		fn access_mode(&self) -> AccessMode {
			AccessMode::ReadOnly
		}

		fn execute(&self, _ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
			use futures::StreamExt;
			let batches = (*self.batches).clone();
			let delay_ms = self.delay_ms;

			let stream = futures::stream::iter(batches).then(move |batch_values| async move {
				if let Some(delay) = delay_ms {
					tokio::time::sleep(Duration::from_millis(delay)).await;
				}
				Ok(ValueBatch {
					values: batch_values,
				})
			});

			Ok(Box::pin(stream))
		}
	}

	/// Mock execution plan that produces an error
	#[derive(Debug, Clone)]
	struct ErrorPlan {
		error_msg: String,
	}

	impl OperatorPlan for ErrorPlan {
		fn name(&self) -> &'static str {
			"ErrorPlan"
		}

		fn required_context(&self) -> ContextLevel {
			ContextLevel::Root
		}

		fn access_mode(&self) -> AccessMode {
			AccessMode::ReadOnly
		}

		fn execute(&self, _ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
			let error_msg = self.error_msg.clone();
			let stream = futures::stream::once(async move {
				Err(crate::expr::ControlFlow::Err(anyhow::anyhow!("{}", error_msg)))
			});
			Ok(Box::pin(stream))
		}
	}

	async fn create_test_context() -> ExecutionContext {
		use crate::iam::Role;
		use crate::kvs::{Datastore, LockType, TransactionType};

		// Create a minimal root context for testing
		let ds = Datastore::new("memory").await.unwrap();
		let txn = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();

		let root_ctx = RootContext {
			datastore: None,
			params: Arc::new(std::collections::HashMap::new()),
			cancellation: CancellationToken::new(),
			auth: Arc::new(Auth::for_root(Role::Owner)),
			auth_enabled: false,
			txn: Arc::new(txn),
		};
		ExecutionContext::Root(root_ctx)
	}

	async fn collect_batches(stream: ValueBatchStream) -> Result<Vec<ValueBatch>, anyhow::Error> {
		use futures::StreamExt;
		let mut batches = Vec::new();
		let mut stream = stream;
		while let Some(batch_result) = stream.next().await {
			match batch_result {
				Ok(batch) => batches.push(batch),
				Err(crate::expr::ControlFlow::Err(e)) => return Err(e),
				Err(crate::expr::ControlFlow::Continue) => continue,
				Err(crate::expr::ControlFlow::Break) => break,
				Err(crate::expr::ControlFlow::Return(_)) => break,
			}
		}
		Ok(batches)
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_single_consumer() {
		let input = Arc::new(MockPlan::new(vec![
			vec![Value::from(1), Value::from(2)],
			vec![Value::from(3), Value::from(4)],
		]));

		let broadcast = Arc::new(BroadcastSource::new(input));
		let ctx = create_test_context().await;

		let stream = broadcast.execute(&ctx).unwrap();
		let batches = collect_batches(stream).await.unwrap();

		assert_eq!(batches.len(), 2);
		assert_eq!(batches[0].values.len(), 2);
		assert_eq!(batches[1].values.len(), 2);
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_multiple_concurrent_consumers() {
		let input = Arc::new(MockPlan::new(vec![
			vec![Value::from(1), Value::from(2)],
			vec![Value::from(3), Value::from(4)],
			vec![Value::from(5)],
		]));

		let broadcast = Arc::new(BroadcastSource::new(input));
		let ctx = create_test_context().await;

		// Spawn 3 consumers concurrently
		let mut handles = Vec::new();
		for _ in 0..3 {
			let broadcast = broadcast.clone();
			let ctx = ctx.clone();
			let handle = tokio::spawn(async move {
				let stream = broadcast.execute(&ctx).unwrap();
				collect_batches(stream).await.unwrap()
			});
			handles.push(handle);
		}

		// Wait for all consumers
		let results = futures::future::join_all(handles).await;

		// All consumers should get the same data
		for result in results {
			let batches = result.unwrap();
			assert_eq!(batches.len(), 3);
			assert_eq!(batches[0].values.len(), 2);
			assert_eq!(batches[1].values.len(), 2);
			assert_eq!(batches[2].values.len(), 1);
		}
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_late_starting_consumer() {
		let input = Arc::new(
			MockPlan::new(vec![vec![Value::from(1)], vec![Value::from(2)], vec![Value::from(3)]])
				.with_delay(10),
		);

		let broadcast = Arc::new(BroadcastSource::new(input));
		let ctx = create_test_context().await;

		// Start first consumer immediately
		let broadcast1 = broadcast.clone();
		let ctx1 = ctx.clone();
		let handle1 = tokio::spawn(async move {
			let stream = broadcast1.execute(&ctx1).unwrap();
			collect_batches(stream).await.unwrap()
		});

		// Wait a bit, then start second consumer
		tokio::time::sleep(Duration::from_millis(25)).await;

		let stream2 = broadcast.execute(&ctx).unwrap();
		let batches2 = collect_batches(stream2).await.unwrap();

		let batches1 = handle1.await.unwrap();

		// Both should get all batches
		assert_eq!(batches1.len(), 3);
		assert_eq!(batches2.len(), 3);
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_error_propagation() {
		let input = Arc::new(ErrorPlan {
			error_msg: "Test error".to_string(),
		});

		let broadcast = Arc::new(BroadcastSource::new(input));
		let ctx = create_test_context().await;

		let stream = broadcast.execute(&ctx).unwrap();
		let result = collect_batches(stream).await;

		assert!(result.is_err());
		assert!(result.unwrap_err().to_string().contains("Test error"));
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_empty_stream() {
		let input = Arc::new(MockPlan::new(vec![]));

		let broadcast = Arc::new(BroadcastSource::new(input));
		let ctx = create_test_context().await;

		let stream = broadcast.execute(&ctx).unwrap();
		let batches = collect_batches(stream).await.unwrap();

		assert_eq!(batches.len(), 0);
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_fast_consumer_waits() {
		// Producer that slowly produces batches
		let input = Arc::new(
			MockPlan::new(vec![vec![Value::from(1)], vec![Value::from(2)]]).with_delay(50),
		);

		let broadcast = Arc::new(BroadcastSource::new(input));
		let ctx = create_test_context().await;

		let start = std::time::Instant::now();
		let stream = broadcast.execute(&ctx).unwrap();
		let batches = collect_batches(stream).await.unwrap();
		let elapsed = start.elapsed();

		// Should get all batches
		assert_eq!(batches.len(), 2);

		// Should have waited for the producer (at least 100ms for 2 batches with 50ms delay)
		assert!(elapsed >= Duration::from_millis(90));
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_multiple_consumers_with_varying_speeds() {
		let input = Arc::new(
			MockPlan::new(vec![
				vec![Value::from(1)],
				vec![Value::from(2)],
				vec![Value::from(3)],
				vec![Value::from(4)],
			])
			.with_delay(20),
		);

		let broadcast = Arc::new(BroadcastSource::new(input));
		let ctx = create_test_context().await;

		// Fast consumer
		let broadcast1 = broadcast.clone();
		let ctx1 = ctx.clone();
		let handle1 = tokio::spawn(async move {
			let stream = broadcast1.execute(&ctx1).unwrap();
			collect_batches(stream).await.unwrap()
		});

		// Slow consumer that processes slowly
		let broadcast2 = broadcast.clone();
		let ctx2 = ctx.clone();
		let handle2 = tokio::spawn(async move {
			use futures::StreamExt;
			let mut stream = broadcast2.execute(&ctx2).unwrap();
			let mut batches = Vec::new();
			while let Some(batch_result) = stream.next().await {
				match batch_result {
					Ok(batch) => {
						batches.push(batch);
						// Simulate slow processing
						tokio::time::sleep(Duration::from_millis(30)).await;
					}
					Err(crate::expr::ControlFlow::Err(e)) => panic!("{}", e),
					_ => {}
				}
			}
			batches
		});

		let batches1 = handle1.await.unwrap();
		let batches2 = handle2.await.unwrap();

		// Both should get all batches
		assert_eq!(batches1.len(), 4);
		assert_eq!(batches2.len(), 4);
	}
}
