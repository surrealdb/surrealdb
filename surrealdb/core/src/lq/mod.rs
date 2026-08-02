//! Live-query engine support.
//!
//! This module hosts the components of the inverted live-query pipeline, whose
//! goal is to move per-subscriber matching/permission/projection work off the
//! mutator's transaction path and onto the subscriber side, so that write
//! throughput no longer degrades as the number of live subscribers grows.
//!
//! Under the `Router` engine the write path captures before/after values into
//! the dedicated `lqe` keyspace ([`event`]) — gated on whether the
//! table has any subscriber, read durably from the committed `key::table::lq`
//! rows so the decision is consistent and cluster-wide. The per-node [`router`]
//! tails that keyspace off the write path and replays each event through the
//! [`subscriber`]-side compute; [`gc`] reclaims old events by retention.

pub(crate) mod event;
pub(crate) mod gc;
pub(crate) mod router;
pub(crate) mod subscriber;

pub(crate) use router::LiveQueryRouter;
