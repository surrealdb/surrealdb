use fst::map::Stream;
use fst::{Map, MapBuilder};
use serde::{Deserialize, Serialize};

pub(super) struct FstMap {
	map: Map<Vec<u8>>,
	bytes: Vec<u8>,
}

impl FstMap {
	pub(super) fn size(&self) -> usize {
		self.bytes.len()
	}

	pub(super) fn len(&self) -> usize {
		self.map.len()
	}

	pub(super) fn builder() -> MapBuilder<Vec<u8>> {
		MapBuilder::memory()
	}

	pub(super) fn get(&self, key: &str) -> Option<u64> {
		self.map.get(key)
	}

	pub(super) fn stream(&self) -> Stream<'_> {
		self.map.stream()
	}
}

impl TryFrom<MapBuilder<Vec<u8>>> for FstMap {
	type Error = fst::Error;

	fn try_from(builder: MapBuilder<Vec<u8>>) -> Result<Self, Self::Error> {
		Self::try_from(builder.into_inner()?)
	}
}

impl TryFrom<Vec<u8>> for FstMap {
	type Error = fst::Error;

	fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
		let map = Map::new(bytes.clone())?;
		Ok(Self {
			map,
			bytes,
		})
	}
}

impl Serialize for FstMap {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		serializer.serialize_bytes(&self.bytes)
	}
}

impl<'de> Deserialize<'de> for FstMap {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
		let map = Map::new(bytes.clone()).map_err(serde::de::Error::custom)?;
		Ok(Self {
			map,
			bytes,
		})
	}
}
