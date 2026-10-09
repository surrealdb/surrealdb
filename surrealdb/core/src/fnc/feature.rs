//! Stand-ins for functions whose implementation a cargo feature left out.

/// The error a stand-in answers a call with: it names the function and the
/// cargo feature the build lacks.
pub(crate) fn disabled_error(name: &str, feature: &str) -> anyhow::Error {
	anyhow::Error::new(crate::exec::Error::InvalidFunction {
		name: String::from(name),
		message: format!(
			"This build was compiled without the `{feature}` feature, which this function requires."
		),
	})
}

/// Defines each listed function with the signature of the implementation it
/// stands in for, failing every call with an error that names the function
/// and the cargo feature the build lacks.
macro_rules! disabled_functions {
	($feature:literal; $($(#[$attr:meta])* $fn_name:ident($args:ty) => $name:expr;)+) => {
		$(
			$(#[$attr])*
			pub fn $fn_name(_: $args) -> ::anyhow::Result<$crate::val::Value> {
				Err($crate::fnc::feature::disabled_error($name, $feature))
			}
		)+
	};
}

/// [`disabled_functions`] for the asynchronous walk a function is metered
/// through: each is defined with the signature of the walk it stands in for and
/// fails every call the same way.
#[cfg(not(feature = "collate"))]
macro_rules! disabled_walks {
	($feature:literal; $($(#[$attr:meta])* $fn_name:ident($args:ty) => $name:expr;)+) => {
		$(
			$(#[$attr])*
			pub async fn $fn_name(_: $args) -> ::anyhow::Result<$crate::val::Value> {
				Err($crate::fnc::feature::disabled_error($name, $feature))
			}
		)+
	};
}

pub(crate) use disabled_functions;
#[cfg(not(feature = "collate"))]
pub(crate) use disabled_walks;
