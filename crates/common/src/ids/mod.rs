mod id_set;
pub use id_set::IdSet;

pub trait Id: Sized + Copy {
	fn idx(self) -> usize;

	fn from_idx(idx: usize) -> Option<Self>;
}

impl Id for usize {
	#[inline]
	fn idx(self) -> usize {
		self
	}

	#[inline]
	fn from_idx(idx: usize) -> Option<Self> {
		Some(idx)
	}
}

impl Id for u32 {
	#[inline]
	fn idx(self) -> usize {
		self as usize
	}

	#[inline]
	fn from_idx(idx: usize) -> Option<Self> {
		u32::try_from(idx).ok()
	}
}

/// A wrapper for quickly creating a new type index.
///
/// Wraps around a NonMaxU32, making the resulting new-type index niche optimized in options and
/// similar.
#[macro_export]
macro_rules! id {
    ($name:ident $( < $($gen:ident),* $(,)? > )? ) => {
        pub struct $name $( < $( $gen, )* > )?{
            id: $crate::non_max::NonMaxU32,
            $(
                _marker: ::std::marker::PhantomData< $($gen),*>
            )?
        }

        impl$( <$($gen),* > )? $crate::ids::Id for $name$( <$($gen),* > )?{
            fn idx(self) -> usize{
                self.into_u32() as usize
            }

            fn from_idx(idx: usize) -> Option<Self>{
                u32::try_from(idx).ok().and_then(Self::from_u32)
            }
        }

        impl$( <$($gen),* > )? $name $( < $($gen),* > )? {
            pub const MIN: Self = Self{
				id: $crate::non_max::NonMaxU32::MIN,
				$(
					_marker: ::std::marker::PhantomData::<$($gen),*>,
				)?
            };

            pub const MAX : Self = Self{
				id: $crate::non_max::NonMaxU32::MAX,
				$(
					_marker: std::marker::PhantomData::<$($gen),*>,
				)?
            };

            pub const fn from_u32(index: u32) -> Option<Self> {
				if let Some(id) = $crate::non_max::NonMaxU32::new(index) {
					Some(Self{
						id,
						$(
							_marker: std::marker::PhantomData::<$($gen),*>
						)?
					})
				}else{
					None
				}

            }

            pub const unsafe fn from_u32_unchecked(index: u32) -> Self {
                unsafe {
					Self{
						id: $crate::non_max::NonMaxU32::new_unchecked(index),
						$(
								_marker: std::marker::PhantomData::<$($gen),*>
						)?
					}
                }
            }

            pub const fn into_u32(self) -> u32 {
				self.id.get()
            }

            pub fn next(self) -> Option<Self>{
                Self::from_u32(self.id.get() + 1)
            }
        }

        impl$( <$($gen),* > )? Clone for $name $( < $($gen),* > )? {
            fn clone(&self) -> Self {
                *self
            }
        }
        impl$( <$($gen),* > )? Copy for $name $( < $($gen),* > )? { }
        impl$( <$($gen),* > )? PartialEq for $name $( < $($gen),* > )? {
            fn eq(&self, other: &Self) -> bool {
                self.id == other.id
            }
        }
        impl$( <$($gen),* > )? Eq for $name $( < $($gen),* > )? { }
        impl$( <$($gen),* > )? ::std::hash::Hash for $name $( < $($gen),* > )? {
            fn hash<H: ::std::hash::Hasher>(&self, state: &mut H) {
                self.id.hash(state)
            }
        }
        impl$( <$($gen),* > )? ::std::fmt::Debug for $name $( < $($gen),* > )? {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.debug_struct(stringify!($name))
                    .field("id", &self.into_u32())
                    .finish()
            }
        }

    };
}
