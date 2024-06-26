use crate::cf;
use crate::cf::ChangeSet;
use crate::dbs::{Options, Statement};
use crate::err::Error;
use crate::fflags::FFLAGS;
use crate::kvs::lq_cf::LiveQueryTracker;
use crate::kvs::lq_structs::{LqIndexKey, LqIndexValue, LqSelector};
use crate::kvs::lq_v2_doc::construct_document;
use crate::kvs::LockType::Optimistic;
use crate::kvs::TransactionType::Read;
use crate::kvs::{Datastore, Transaction};
use crate::sql::statements::show::ShowSince;
use crate::vs::conv;
use reblessive::tree::Stk;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Poll change feeds for live query notifications
pub async fn process_lq_notifications(
	ds: &Datastore,
	stk: &mut Stk,
	opt: &Options,
) -> Result<(), Error> {
	// Runtime feature gate, as it is not production-ready
	if !FFLAGS.change_feed_live_queries.enabled() {
		return Ok(());
	}
	// Return if there are no live queries
	if ds.notification_channel.is_none() {
		trace!("Channels is none, short-circuiting");
		return Ok(());
	}
	if ds.lq_cf_store.read().await.is_empty() {
		// This is safe - just a shortcut
		trace!("No live queries, short-circuiting");
		return Ok(());
	}

	// Change map includes a mapping of selector to changesets, ordered by versionstamp
	let mut relevant_changesets: BTreeMap<LqSelector, Vec<ChangeSet>> = BTreeMap::new();
	{
		let tx = ds.transaction(Read, Optimistic).await?;
		populate_relevant_changesets(
			tx,
			ds.lq_cf_store.clone(),
			ds.engine_options.live_query_catchup_size,
			&mut relevant_changesets,
		)
		.await?;
	};

	for (selector, change_sets) in relevant_changesets {
		// find matching live queries
		let lq_pairs = ds.lq_cf_store.read().await.live_queries_for_selector(&selector);

		// Find relevant changes
		#[cfg(debug_assertions)]
		trace!("There are {} change sets", change_sets.len());
		#[cfg(debug_assertions)]
		trace!(
			"\n{}",
			change_sets
				.iter()
				.enumerate()
				.map(|(i, x)| format!("[{i}] {:?}", x))
				.collect::<Vec<String>>()
				.join("\n")
		);
		for change_set in change_sets {
			process_change_set_for_notifications(ds, stk, opt, change_set, &lq_pairs).await?;
		}
	}
	trace!("Finished process lq successfully");
	Ok(())
}
async fn populate_relevant_changesets(
	mut tx: Transaction,
	live_query_tracker: Arc<RwLock<LiveQueryTracker>>,
	catchup_size: u32,
	relevant_changesets: &mut BTreeMap<LqSelector, Vec<ChangeSet>>,
) -> Result<(), Error> {
	let live_query_tracker = live_query_tracker.write().await;
	let tracked_cfs = live_query_tracker.get_watermarks().len();
	// We are going to track the latest observed versionstamp here
	for current in 0..tracked_cfs {
		// The reason we iterate this way (len+index) is because we "know" that the list won't change, but we
		// want mutable access to it so we can update it while iterating
		let (selector, vs) = live_query_tracker.get_watermark_by_enum_index(current).unwrap();

		// Read the change feed for the selector
		#[cfg(debug_assertions)]
		trace!(
			"Checking for new changes for ns={} db={} tb={} vs={:?}",
			selector.ns,
			selector.db,
			selector.tb,
			vs
		);
		let res = cf::read(
			&mut tx,
			&selector.ns,
			&selector.db,
			// Technically, we can not fetch by table and do the per-table filtering this side.
			// That is an improvement though
			Some(&selector.tb),
			ShowSince::versionstamp(vs),
			Some(catchup_size),
		)
		.await?;
		// Confirm we do need to change watermark - this is technically already handled by the cf range scan
		if res.is_empty() {
			#[cfg(debug_assertions)]
			trace!(
				"There were no changes in the change feed for {:?} from versionstamp {:?}",
				selector,
				conv::versionstamp_to_u64(vs)
			)
		}
		if let Some(change_set) = res.last() {
			if conv::versionstamp_to_u64(&change_set.0) > conv::versionstamp_to_u64(vs) {
				#[cfg(debug_assertions)]
				trace!("Adding a change set for lq notification processing");
				// This does not guarantee a notification, as a changeset an include many tables and many changes
				relevant_changesets.insert(selector.clone(), res);
			}
		}
	}
	tx.cancel().await
}

async fn process_change_set_for_notifications(
	ds: &Datastore,
	stk: &mut Stk,
	opt: &Options,
	change_set: ChangeSet,
	lq_pairs: &[(LqIndexKey, LqIndexValue)],
) -> Result<(), Error> {
	#[cfg(debug_assertions)]
	trace!("Moving to next change set, {:?}", change_set);
	for (lq_key, lq_value) in lq_pairs.iter() {
		#[cfg(debug_assertions)]
		trace!("Processing live query for notification key={:?} and value={:?}", lq_key, lq_value);
		let change_vs = change_set.0;
		let database_mutation = &change_set.1;
		for table_mutations in database_mutation.0.iter() {
			if table_mutations.0 == lq_key.selector.tb {
				// Create a doc of the table value
				// Run the 'lives' logic on the doc, while providing live queries instead of reading from storage
				// This will generate and send notifications
				#[cfg(debug_assertions)]
				trace!(
					"There are {} table mutations being prepared for notifications",
					table_mutations.1.len()
				);
				for (_i, mutation) in table_mutations.1.iter().enumerate() {
					#[cfg(debug_assertions)]
					trace!("[{} @ {:?}] Processing table mutation: {:?}   Constructing document from mutation", _i, change_vs, mutation);
					if let Some(doc) = construct_document(mutation)? {
						// We know we are only processing a single LQ at a time, so we can limit notifications to 1
						let notification_capacity = 1;
						// We track notifications as a separate channel in case we want to process
						// for the current state we only forward
						let (local_notification_channel_sender, local_notification_channel_recv) =
							channel::bounded(notification_capacity);
						doc.check_lqs_and_send_notifications(
							stk,
							opt,
							&Statement::Live(&lq_value.stm),
							[&lq_value.stm].as_slice(),
							&local_notification_channel_sender,
						)
						.await
						.map_err(|e| {
							Error::Internal(format!(
								"Error checking lqs for notifications: {:?}",
								e
							))
						})?;

						// Send the notifications to driver or api
						while let Ok(notification) = local_notification_channel_recv.try_recv() {
							#[cfg(debug_assertions)]
							trace!("Sending notification to client: {:?}", notification);
							ds.notification_channel
								.as_ref()
								.unwrap()
								.0
								.send(notification)
								.await
								.unwrap();
						}
					}
					// Progress the live query watermark
				}
			}
		}
		ds.lq_cf_store.write().await.update_watermark_live_query(lq_key, &change_vs).unwrap();
	}
	Ok(())
}
