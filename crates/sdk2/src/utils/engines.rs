use std::{any::TypeId, collections::HashMap, sync::Arc};
use surrealdb_types::SurrealBridge;
use url::Url;

use crate::events::EngineEvents;
use crate::utils::Publisher;

#[derive(Clone)]
pub(crate) struct Engines {
	engines: HashMap<TypeId, EngineMetadata>,
	protocol_map: HashMap<&'static str, TypeId>,
}

impl Engines {
	pub fn new() -> Self {
		Self {
			engines: HashMap::new(),
			protocol_map: HashMap::new(),
		}
	}

	pub fn attach<E: ConstructableEngine>(&mut self) {
		let type_id = TypeId::of::<E>();
		let protocols = E::protocols();

		self.engines.insert(
			type_id,
			EngineMetadata {
				protocols,
				constructor: || Arc::new(E::construct()),
			},
		);

		for protocol in protocols {
			self.protocol_map.insert(protocol, type_id);
		}
	}

	pub fn constructor(&self, protocol: &str) -> Option<BoxedConstructor> {
		let Some(&type_id) = self.protocol_map.get(protocol) else {
			return None;
		};

		let Some(engine) = self.engines.get(&type_id) else {
			return None;
		};

		Some(engine.constructor)
	}
}

pub trait Engine: SurrealBridge {
	fn connect(&self, url: Url);
	fn disconnect(&self);
	fn publisher(&self) -> &Publisher<EngineEvents>;
}

pub trait ConstructableEngine: Engine + Sized + Send + 'static {
	fn protocols() -> &'static [&'static str];
	fn construct() -> Self;
}

type BoxedConstructor =
	fn() -> Arc<dyn Engine>;

#[derive(Clone)]
struct EngineMetadata {
	#[allow(dead_code)]
	protocols: &'static [&'static str],
	constructor: BoxedConstructor,
}
