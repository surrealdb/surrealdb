pub struct Mutex<T: ?Sized> {}

impl<T: ?Sized> Mutex<T> {
	/// Creates a new instance of a `Mutex<T>` which is unlocked.
	/// This particular implementation is for traceability
	#[track_caller]
	pub fn new(value: T) -> Mutex<T>
	where
		T: Sized,
	{
		Mutex {}
	}
}

pub struct RwLock<T: ?Sized> {}

impl<T: ?Sized> RwLock<T> {
	/// Creates a new instance of an `RwLock<T>` which is unlocked.
	/// This particular implementation is for traceability
	#[track_caller]
	pub fn new(value: T) -> RwLock<T>
	where
		T: Sized,
	{
		RwLock {}
	}
}
