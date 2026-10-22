//! The optimisation passes over the logical and physical dialects.
//!
//! A pass is a named step that runs in one phase. It changes the plan only through the rewriter,
//! and the container must still verify after it runs. Passes read effects, fallibility and
//! legality from the analyses. They do not recompute those facts. The loop optimiser's passes run
//! through [`run_lo`], in the order that [`reg::lo_passes`] lists them.

pub(crate) mod cse;
pub(crate) mod dce;
pub(crate) mod driver;
pub(crate) mod fold;
pub(crate) mod inline;
pub(crate) mod licm;
pub(crate) mod loop_reg;
pub(crate) mod loop_unroll;
pub(crate) mod perm_where;
pub(crate) mod phys_cse;
pub(crate) mod push_subquery;
pub(crate) mod reg;
pub(crate) mod rehome;
pub(crate) mod unswitch;

#[cfg(test)]
mod tests_cse;
#[cfg(test)]
mod tests_dce;
#[cfg(test)]
mod tests_driver;
#[cfg(test)]
mod tests_fold;
#[cfg(test)]
mod tests_gates;
#[cfg(test)]
mod tests_layers;
#[cfg(test)]
mod tests_licm;
#[cfg(test)]
mod tests_perm_where;
#[cfg(test)]
mod tests_push_subquery;
#[cfg(test)]
mod tests_support;
#[cfg(test)]
mod tests_unroll;
#[cfg(test)]
mod tests_unswitch;

#[allow(unused_imports)]
pub(crate) use driver::{FoldObserver, NoObserver, PassConfig, PassReport, run_lo};
