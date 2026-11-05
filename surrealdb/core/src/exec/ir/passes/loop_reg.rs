//! The loop optimiser's registration: it registers nothing, because the loop passes are listed
//! once, in order, by [`super::reg::lo_passes`] and registered by [`super::reg::register`].

use crate::exec::ir::registry::Registry;

/// Registers nothing with `_r`.
pub(crate) fn register(_r: &mut Registry) {}
