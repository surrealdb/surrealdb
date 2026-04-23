/// One registered callable function (name + invoke/args/returns).
/// Submitted by `#[surrealism]`; collected in the surrealism crate.
///
/// `name` is `None` for the default export, `Some("foo")` for a named export.
#[doc(hidden)]
pub struct SurrealismEntry {
	pub name: Option<&'static str>,
	pub comment: Option<&'static str>,
	pub invoke: fn(&[u8]) -> Result<Vec<u8>, String>,
	pub args: fn() -> Result<Vec<u8>, String>,
	pub returns: fn() -> Result<Vec<u8>, String>,
	pub writeable: bool,
}

/// Registered init function. Submitted by `#[surrealism(init)]`.
#[doc(hidden)]
pub struct SurrealismInit(pub fn() -> Result<(), String>);
