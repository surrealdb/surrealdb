//! The order of `inet` and `cidr` values, as PostgreSQL sorts them: by address family, then by
//! the address bits both prefixes cover, then by prefix length, then by the whole address.
//!
//! SurrealQL and the SQL planner compare the stored text, so this order is not applied by the
//! planner yet. The comparison is exposed for a planner rank function to call, as `enum_rank`
//! does for enum labels.

use std::cmp::Ordering;

use surrealdb_expr::expr::statements::define::sql_types::text_shaped::network;

/// The order of two `inet` or `cidr` texts. `None` when either text does not read as a network.
pub fn compare_networks(left: &str, right: &str) -> Option<Ordering> {
	let left = network::parse(left).ok()?;
	let right = network::parse(right).ok()?;
	Some(network::compare(&left, &right))
}

#[cfg(test)]
mod tests {
	use std::cmp::Ordering;

	use super::compare_networks;

	fn sorted(texts: &[&str]) -> Vec<String> {
		let mut owned: Vec<String> = texts.iter().map(|t| (*t).to_string()).collect();
		owned.sort_by(|a, b| compare_networks(a, b).expect("valid networks"));
		owned
	}

	#[test]
	fn networks_sort_by_family_then_network_then_prefix() {
		assert_eq!(
			sorted(&["::1", "10.0.0.2", "10.0.0.0/8", "9.255.255.255", "10.0.0.0/16", "10.0.0.1"]),
			vec!["9.255.255.255", "10.0.0.0/8", "10.0.0.0/16", "10.0.0.1", "10.0.0.2", "::1",]
		);
	}

	#[test]
	fn a_cidr_and_an_inet_compare_by_the_same_rule() {
		assert_eq!(compare_networks("10.1.2.0/24", "10.1.2.0/24"), Some(Ordering::Equal));
		assert_eq!(compare_networks("10.1.2.3/32", "10.1.2.3"), Some(Ordering::Equal));
		assert_eq!(compare_networks("10.1.0.0/16", "10.1.2.0/24"), Some(Ordering::Less));
	}

	#[test]
	fn a_text_that_is_not_a_network_has_no_order() {
		assert_eq!(compare_networks("10.0.0.1", "not an address"), None);
		assert_eq!(compare_networks("nope", "10.0.0.1"), None);
	}
}
