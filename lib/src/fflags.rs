#[allow(dead_code)]
pub(crate) static FFLAGS: FFlags = FFlags {
    change_feed_live_queries: FFlagEnabledStatus {
        enabled_release: false,
        enabled_debug: false,
        enabled_test: false,
        env_override: "SURREALDB_CHANGE_FEED_LIVE_QUERIES",
        owner: "Hugh Kaznowski",
        description: "Disables live queries as a separate feature and moves to using change feeds as the underlying mechanism",
        date_enabled_test: None,
        date_enabled_debug: None,
        date_enabled_release: None,
        release_version: None,
    }
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
#[allow(dead_code)]
pub(crate) struct FFlags {
	pub(crate) change_feed_live_queries: FFlagEnabledStatus,
}

/// This struct is not used in the implementation;
/// All the fields are here as information for people investigating the feature flag.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct FFlagEnabledStatus {
	pub(crate) enabled_release: bool,
	pub(crate) enabled_debug: bool,
	pub(crate) enabled_test: bool,
	pub(crate) owner: &'static str,
	pub(crate) description: &'static str,
	pub(crate) env_override: &'static str,
	pub(crate) date_enabled_test: Option<&'static str>,
	pub(crate) date_enabled_debug: Option<&'static str>,
	pub(crate) date_enabled_release: Option<&'static str>,
	pub(crate) release_version: Option<&'static str>,
}

impl FFlagEnabledStatus {
	#[allow(dead_code)]
	pub(crate) fn enabled(&self) -> bool {
		let mut enabled = false;
		if let Ok(env_var) = std::env::var(self.env_override) {
			if env_var.trim() == "true" {
				return true;
			}
			return false;
		}
		// Test check
		#[cfg(test)]
		{
			enabled = enabled || self.enabled_test;
		}
		// Debug build check
		#[cfg(debug_assertions)]
		{
			enabled = enabled || self.enabled_debug;
		}
		// Release build check
		#[cfg(not(debug_assertions))]
		{
			enabled = enabled || self.enabled_release;
		}
		enabled
	}
}
