macro_rules! key {
	(
		$(#[$m:meta])*
		$v:vis struct $name:ident$(<$lt:lifetime>)? $(for $format:ty)?{
			$($t:tt)*
		}
	) => {
		key!{@struct ($([$m])*,$v,$name,$($lt)?) => { $($t)* } => {}}

		impl$(<$lt>)? ::storekey::Encode$(<$format>)? for $name $(<$lt>)?{
			fn encode<W>(&self, w: &mut ::storekey::Writer<W>) -> Result<(), ::storekey::EncodeError>
				where W: ::std::io::Write
			{
				key!(@encode (w,self,key!(@format $($format)?)) => { $($t)* });
				Ok(())
			}
		}

		key!{@decode_impl ($name,key!(@format $($format)?),$($lt)?) => { $($t)* }}

	};

    (@format $format:ty) => {
        $format
    };
    (@format ) => {
        ()
    };


	// struct definition construction, filtering out the literal and only leaving the actual named fields.
	// Structured as a token muncher that builds up a struct body in the `{ $($accum:tt)* }` part.

	// Base case, when we have eaten all the fields.
	(@struct ($([$m:meta])*,$v:vis,$name:ident,$($lt:lifetime)?) => {} => { $($accum:tt)*}) =>  {
		$(#[$m])*
		$v struct $name $(<$lt>)? { $($accum)* }
	};

	// Next is a field.
	(@struct ($([$m:meta])*,$v:vis,$name:ident,$($lt:lifetime)?) => {
		$(#[$fm:meta])*
		$p:vis $field:ident: $ty:ty,
		$($rest:tt)*
	} => { $($accum:tt)*}) =>  {
		key!{
			@struct
			($([$m])*,$v,$name,$($lt)?) => {
				$($rest)*
			} => {
				$($accum)*
				$(#[$fm])*
				$p $field: $ty,
			}
		}
	};

	// Next is a literal case
	(@struct ($([$m:meta])*,$v:vis,$name:ident,$($lt:lifetime)?) => {
		$(#[$fm:meta])*
		$l:literal,
		$($rest:tt)*
	} => { $($accum:tt)*}) =>  {
		key!{
			@struct
			($([$m])*,$v,$name,$($lt)?) => {
				$($rest)*
			} => { $($accum)* }
		}
	};


	// Construct the encode method body.

	// Case for when a literal needs to be written.
	(@encode ($writer:expr,$self:expr,$($format:ty)?) => {
		$(#[$fm:meta])*
		$l:literal, $($rest:tt)*
	}) => {
		::storekey::Encode$(::<$format>)?::encode(&$l,$writer)?;
		key!(@encode ($writer,$self, $($format)?) => { $($rest)* });
	};

	// Case for when a field needs to be written.
	(@encode ($writer:expr,$self:expr,$($format:ty)?) => {
		$(#[$fm:meta])*
		$v:vis $field:ident: $type:ty, $($rest:tt)*
	}) => {
		::storekey::Encode$(::<$format>)?::encode(&$self.$field,$writer)?;
		key!(@encode ($writer,$self, $($format)?) => { $($rest)* })
	};

	// Base case for when we are done.
	(@encode ($writer:expr,$self:expr,$($format:ty)?) => {}) => {};


	// Construct the decode trait.

	// Decode signature with a lifetime
	(@decode_impl ($name:ident,$format:ty,$l:lifetime) => { $($t:tt)* }) => {
		impl<$l> ::storekey::BorrowDecode<$l,$format> for $name<$l>{
			fn borrow_decode(r: &mut ::storekey::BorrowReader<$l>) -> Result<Self, ::storekey::DecodeError>
			{
				key!{@decode ($format,r) => { $($t)* } => {}}
			}
		}

	};

	// Decode signature with a lifetime
	(@decode_impl ($name:ident,$format:ty,) => { $($t:tt)* }) => {
		impl<'de> ::storekey::BorrowDecode<'de,$format> for $name{
			fn borrow_decode(r: &mut ::storekey::BorrowReader<'de>) -> Result<Self, ::storekey::DecodeError>
			{
				key!{@decode ($format,r) => { $($t)* } => {}}
			}
		}

	};


	// Construct the decode method body.

	// Literal is next
	(@decode ($format:ty,$reader:expr) => {
		$(#[$fm:meta])*
		$l:literal, $($rest:tt)*
	} => { $($fields:ident)* }) => {
		{
            fn decode_eq<'a, T: ::storekey::BorrowDecode<'a,$format> + std::cmp::Eq>(
                r: &mut ::storekey::BorrowReader<'a>,
                b: T,
            ) -> Result<bool, ::storekey::DecodeError> {
                Ok(T::borrow_decode(r)? == b)
            }

			if !decode_eq($reader, $l)? {
				return Err(::storekey::DecodeError::InvalidFormat)
			}
		}
		key!{@decode ($format,$reader) => { $($rest)* } => { $($fields)* }}
	};

	// Normal field is next
	(@decode ($format:ty,$reader:expr) => {
		$(#[$fm:meta])*
		$v:vis $field:ident: $type:ty, $($rest:tt)*
	} => { $($fields:ident)* }) => {
		let $field: $type = ::storekey::BorrowDecode::<$format>::borrow_decode($reader)?;
		key!(@decode ($format,$reader) => { $($rest)* } => { $field $($fields)* })
	};

	// Done
	(@decode ($format:ty,$reader:expr) => {} => { $($fields:ident)* }) => {
		Ok(Self{
			$($fields),*
		})
	};
}
pub(crate) use key;

/// Implement `KVKey` for `$t` with `$v` as the associated value type.
///
/// Compile-fails for value types whose `KVValue::KeyContext` is not `()`
/// (currently only `Record`, with `KeyContext = RecordId`) — those need a
/// hand-written impl that supplies the context.
macro_rules! impl_kv_range_storekey{
	($(<$($tt:tt)*>)? $t:ty) => {
		impl$(<$($tt)*>)? crate::key::KVRange for $t {

			fn encode_bound(&self) -> ::anyhow::Result<crate::key::Key<'static>> {
				let key = ::storekey::encode_vec(self).map_err(|_| crate::key::Error::Unencodable)?;
				Ok(crate::key::Key::from(key))
			}

			fn encode_range(&self) -> ::anyhow::Result<crate::key::KeyRange<'static>> {
				self.encode_bound().map(|x| x.prefix_expect())
			}
		}
	};
}
pub(crate) use impl_kv_range_storekey;

/// Implement `KVKey` for `$t` with `$v` as the associated value type.
///
/// Compile-fails for value types whose `KVValue::KeyContext` is not `()`
/// (currently only `Record`, with `KeyContext = RecordId`) — those need a
/// hand-written impl that supplies the context.
macro_rules! impl_kv_key_storekey {
	($t:ident<$lt:lifetime> => $v:ty) => {
		impl<$lt> crate::key::KVKey for $t<$lt> {
			type Value = $v;

			fn encode_buffer(&self, buffer: &mut Vec<u8>) -> ::anyhow::Result<()> {
				::storekey::encode(buffer, self).map_err(|_| crate::key::Error::Unencodable)?;
				Ok(())
			}

			fn value_context(&self) -> <$v as crate::key::KVValue>::KeyContext {}
		}

		impl<$lt> crate::key::KVKeyDecode<$lt> for $t<$lt> {
			fn decode_key(bytes: & $lt [u8]) -> ::anyhow::Result<Self> {
				Ok(::storekey::decode_borrow(bytes)
					.map_err(|_| crate::key::Error::Corrupted("Cannot decode kv key"))?)
			}
		}
	};

	($t:ident => $v:ty) => {
		impl crate::key::KVKey for $t {
			type Value = $v;

			fn encode_buffer(&self, buffer: &mut Vec<u8>) -> ::anyhow::Result<()> {
				::storekey::encode(buffer, self).map_err(|_| crate::key::Error::Unencodable)?;
				Ok(())
			}

			fn value_context(&self) -> <$v as crate::key::KVValue>::KeyContext {}
		}

		impl crate::key::KVKeyDecode<'_> for $t {
			fn decode_key(bytes: &[u8]) -> ::anyhow::Result<Self> {
				Ok(::storekey::decode_borrow(bytes)
					.map_err(|_| crate::key::Error::Corrupted("Cannot decode kv key"))?)
			}
		}
	};
}
pub(crate) use impl_kv_key_storekey;
pub(crate) use surrealdb_kvs::impl_kv_value_revisioned;
