//! The ABIs this runtime runs, and what each one gives a guest.
//!
//! An archive's `package.toml` names the ABI it was built for. The runtime
//! runs every ABI in [`Abi::SUPPORTED`] side by side: each one is the set of
//! [`Features`] a guest built for it may use, and the linker gives a component
//! only the host imports its ABI has. An import that changes shape between two
//! ABIs is a feature of its own, with the older shape kept for the ABIs that
//! lack it.
//!
//! `surreal ext build` stamps the oldest supported ABI whose features cover
//! what the component imports and the package declares
//! ([`Features::needed_by`]), so an archive runs on every server that knows
//! that ABI, not only on servers as new as the SDK that built it.

use wasmtime::Engine;
use wasmtime::component::Component;
use wasmtime::component::types::ComponentItem;

use crate::SurrealismError;
use crate::config::{AbiVersion, SurrealismConfig};

/// The instance the guest imports host functions from.
pub(crate) const HOST_INSTANCE: &str = "surrealism:extension/host";

/// The features an ABI gives a guest: host imports it may use, and package
/// clauses the server reads.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Features(u32);

impl Features {
	/// What every supported ABI has: the `transaction` resource with `sql` and
	/// `run`, and the exports of the `extension` world.
	pub const BASE: Self = Self(0);
	/// The `binding` import, and the `[bindings]` table a definition's
	/// `BINDINGS` answers.
	pub const BINDINGS: Self = Self(1);

	/// The host imports only some ABIs have, each with the feature that gives
	/// it. An import missing here is one every supported ABI has.
	const GATED_IMPORTS: &[(&str, Self)] = &[("binding", Self::BINDINGS)];

	pub const fn union(self, other: Self) -> Self {
		Self(self.0 | other.0)
	}

	pub const fn contains(self, other: Self) -> bool {
		self.0 & other.0 == other.0
	}

	/// The features a guest uses: the gated host imports `component` makes,
	/// and the package clauses `config` fills.
	pub fn needed_by(engine: &Engine, component: &Component, config: &SurrealismConfig) -> Self {
		let mut needed = Self::BASE;
		for name in host_imports(engine, component) {
			if let Some((_, feature)) = Self::GATED_IMPORTS.iter().find(|(n, _)| *n == name) {
				needed = needed.union(*feature);
			}
		}
		if !config.bindings.is_empty() {
			needed = needed.union(Self::BINDINGS);
		}
		needed
	}
}

/// One ABI: its number in `package.toml` and the features it gives a guest.
/// A new ABI spells its features as the previous ABI's plus what it adds.
pub trait SurrealismAbi {
	const VERSION: AbiVersion;
	const FEATURES: Features;
}

/// The 4.0 world as `v4.0.0-beta.1` shipped it.
pub struct Abi4;

impl SurrealismAbi for Abi4 {
	const VERSION: AbiVersion = AbiVersion(4);
	const FEATURES: Features = Features::BASE;
}

/// [`Abi4`] plus the `binding` import and `[bindings]`.
pub struct Abi5;

impl SurrealismAbi for Abi5 {
	const VERSION: AbiVersion = AbiVersion(5);
	const FEATURES: Features = Abi4::FEATURES.union(Features::BINDINGS);
}

/// An ABI this runtime runs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Abi {
	V4,
	V5,
}

impl Abi {
	/// Every ABI this runtime runs, oldest first.
	pub const SUPPORTED: &[Self] = &[Self::V4, Self::V5];
	pub const OLDEST: Self = Self::V4;
	pub const LATEST: Self = Self::V5;

	pub const fn version(self) -> AbiVersion {
		match self {
			Self::V4 => Abi4::VERSION,
			Self::V5 => Abi5::VERSION,
		}
	}

	pub const fn features(self) -> Features {
		match self {
			Self::V4 => Abi4::FEATURES,
			Self::V5 => Abi5::FEATURES,
		}
	}

	/// The oldest supported ABI that has every feature in `needed`.
	pub fn oldest_with(needed: Features) -> Self {
		Self::SUPPORTED
			.iter()
			.copied()
			.find(|abi| abi.features().contains(needed))
			.unwrap_or(Self::LATEST)
	}

	/// Refuse `component` when it imports a host function this ABI does not
	/// have, naming the import, before the linker reports it as unresolved.
	pub(crate) fn check_imports(
		self,
		engine: &Engine,
		component: &Component,
	) -> Result<(), SurrealismError> {
		let have = self.features();
		for name in host_imports(engine, component) {
			if let Some((_, feature)) = Features::GATED_IMPORTS.iter().find(|(n, _)| *n == name)
				&& !have.contains(*feature)
			{
				return Err(SurrealismError::Other(anyhow::anyhow!(
					"the component imports `{name}`, which ABI {} does not have, so its \
					 package.toml names an older ABI than the SDK that built it; rebuild it with \
					 `surreal ext build`",
					self.version().0
				)));
			}
		}
		Ok(())
	}
}

impl TryFrom<AbiVersion> for Abi {
	type Error = SurrealismError;

	fn try_from(version: AbiVersion) -> Result<Self, Self::Error> {
		Self::SUPPORTED.iter().copied().find(|abi| abi.version() == version).ok_or(
			SurrealismError::UnsupportedAbi {
				got: version.0,
				oldest: Self::OLDEST.version().0,
				latest: Self::LATEST.version().0,
			},
		)
	}
}

/// The names `component` imports from the host instance.
fn host_imports(engine: &Engine, component: &Component) -> Vec<String> {
	let ty = component.component_type();
	let Some(import) = ty.get_import(engine, HOST_INSTANCE) else {
		return Vec::new();
	};
	let ComponentItem::ComponentInstance(instance) = import.ty else {
		return Vec::new();
	};
	instance.exports(engine).map(|(name, _)| name.to_owned()).collect()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn every_abi_has_the_features_of_the_one_before() {
		for pair in Abi::SUPPORTED.windows(2) {
			assert!(pair[1].features().contains(pair[0].features()), "{pair:?}");
			assert!(pair[0].version().0 < pair[1].version().0, "{pair:?}");
		}
		assert_eq!(Abi::SUPPORTED.first(), Some(&Abi::OLDEST));
		assert_eq!(Abi::SUPPORTED.last(), Some(&Abi::LATEST));
	}

	#[test]
	fn a_guest_gets_the_oldest_abi_that_covers_it() {
		assert_eq!(Abi::oldest_with(Features::BASE), Abi::V4);
		assert_eq!(Abi::oldest_with(Features::BINDINGS), Abi::V5);
	}

	/// A component that imports `binding` and nothing else from the host.
	fn imports_binding(engine: &Engine) -> Component {
		Component::new(
			engine,
			r#"(component (import "surrealism:extension/host" (instance (export "binding" (func)))))"#,
		)
		.unwrap()
	}

	#[test]
	fn an_import_decides_the_abi_a_guest_needs() {
		let engine = Engine::default();
		let config = crate::package_config::PackageConfig::parse(
			"[package]\norganisation = \"t\"\nname = \"t\"\nversion = \"1.0.0\"\n",
		)
		.unwrap()
		.into_config();
		let plain = Component::new(&engine, "(component)").unwrap();
		assert_eq!(Features::needed_by(&engine, &plain, &config), Features::BASE);
		assert_eq!(
			Features::needed_by(&engine, &imports_binding(&engine), &config),
			Features::BINDINGS
		);
		let declares = crate::package_config::PackageConfig::parse(
			"[package]\norganisation = \"t\"\nname = \"t\"\nversion = \"1.0.0\"\n\n[bindings]\nkey = \"string\"\n",
		)
		.unwrap()
		.into_config();
		assert_eq!(Features::needed_by(&engine, &plain, &declares), Features::BINDINGS);
	}

	#[test]
	fn an_abi_refuses_an_import_it_does_not_have() {
		let engine = Engine::default();
		let component = imports_binding(&engine);
		let err = Abi::V4.check_imports(&engine, &component).unwrap_err().to_string();
		assert!(err.contains("imports `binding`, which ABI 4 does not have"), "{err}");
		Abi::V5.check_imports(&engine, &component).unwrap();
	}

	#[test]
	fn only_supported_versions_resolve() {
		assert_eq!(Abi::try_from(AbiVersion(4)).unwrap(), Abi::V4);
		assert_eq!(Abi::try_from(AbiVersion(5)).unwrap(), Abi::V5);
		match Abi::try_from(AbiVersion(3)) {
			Err(SurrealismError::UnsupportedAbi {
				got: 3,
				oldest: 4,
				latest: 5,
			}) => {}
			other => panic!("unexpected result: {other:?}"),
		}
	}
}
