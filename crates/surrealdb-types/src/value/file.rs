#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct File {
	pub bucket: String,
	pub key: String,
}