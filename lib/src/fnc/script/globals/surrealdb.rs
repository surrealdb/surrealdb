#[js::bind(object, public)]
#[quickjs(rename = "surrealdb")]
#[allow(non_upper_case_globals)]
pub mod package {
	pub const version: &str = env!("CARGO_PKG_VERSION");
}
