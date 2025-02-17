use object_store::DynObjectStore;

pub struct GlobalBucket {
	pub store: Box<DynObjectStore>,
	pub enforced: bool,
	// pub key:
}
