use std::num::NonZero;

/// A wrapper around a primitive indicating it cannot be it's max value.
///
/// This type works similar to NonZero and is infact a small wrapper around it enabling niche
/// optimizations for types containing this type.

macro_rules! impl_non_max {
    ($signed:tt, $name:ident, $zeroable:ty) => {

		#[doc = "A wrapper type around a primitive indicating it cannot be it's maximum value."]
		#[doc = ""]
		#[doc = "This type works similar to NonZero and is infact a wrapper around nonzero enabling niche optimizations for `Option` and similar types."]
		#[derive(Clone,Copy, PartialEq, Eq, Hash)]
		pub struct $name(NonZero<$zeroable>);

		impl $name{
			impl_non_max!{@const $signed, $zeroable}
			pub const ZERO: Self = const { Self::new(0).unwrap() };

			#[doc = "Create a new NonMax value. Returns None if the value given is the maximum"]
			pub const fn new(v: $zeroable) -> Option<Self>{
				if let Some(x) = NonZero::<$zeroable>::new(v ^ impl_non_max!(@max $signed, $zeroable)){
					return Some($name(x))
				}
				None
			}

			pub const unsafe fn new_unchecked(v: $zeroable) -> Self{
				unsafe{ $name(NonZero::<$zeroable>::new_unchecked(v ^ impl_non_max!(@max $signed, $zeroable))) }
			}

			pub const fn get(self) -> $zeroable{
				self.0.get() ^ impl_non_max!(@max $signed, $zeroable)
			}
		}


		impl std::fmt::Display for $name {
			fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
				self.get().fmt(f)
			}
		}
		impl std::fmt::Debug for $name {
			fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
				self.get().fmt(f)
			}
		}
    };


	(@max -,  $t:ty) => { <$t>::MIN };
	(@max +, $t:ty) => { <$t>::MAX };
	(@const -,  $t:ty) => {
		pub const MAX: Self = const { Self::new(<$t>::MAX).unwrap() };
		pub const MIN: Self = const { Self::new(<$t>::MIN + 1).unwrap() };
	};
	(@const +, $t:ty) => {
		pub const MAX: Self = const { Self::new(<$t>::MAX - 1).unwrap() };
		pub const MIN: Self = const { Self::new(<$t>::MIN).unwrap() };
	};
}

impl_non_max!(+, NonMaxU8,  u8);
impl_non_max!(-, NonMaxI8,  i8);
impl_non_max!(+, NonMaxU16,  u16);
impl_non_max!(-, NonMaxI16,  i16);
impl_non_max!(+, NonMaxU32,  u32);
impl_non_max!(-, NonMaxI32,  i32);
impl_non_max!(+, NonMaxU64,  u64);
impl_non_max!(-, NonMaxI64,  i64);
impl_non_max!(+, NonMaxUsize,  usize);
impl_non_max!(-, NonMaxIsize,  isize);
