macro_rules! define_key {
	(struct $name:ident $(<$l:lifetime>)? {
		$($inner:tt)*
	}) => {
		define_key!{@define $name $($l)? { $($inner)* } => ()}
		define_key!{@impl_encode $name $($l)? { $($inner)* }}
	};

	(@define $name:ident $($l:lifetime)? { $field:ident: $type:ty, $($rest:tt)* } => ($($def:tt)*)) => {
		define_key!{@define $name $($l)? { $($rest)* } => ( $field: $type, $($def)* )}
	};

	(@define $name:ident $($l:lifetime)? { $lit:expr, $($rest:tt)* } => ($($def:tt)*)) => {
		define_key!{@define $name $($l)? { $($rest)* } => ( $($def)* )}
	};

	(@define $name:ident $($l:lifetime)? { } => ($($def:tt)*)) => {
		struct $name $(<$l>)? {
			$($def)*
		}
	};


	(@impl_encode $name:ident $($l:lifetime)? { $($inner:tt)* }) => {
		impl<$($l)?, F> ::storekey::Encode<F> for $name $(<$l>)? {
			fn encode<W: ::std::io::Write>(&self, w: ::storekey::Writer<W>) -> Result<(), ::storekey::EncodeError>{
				define_key!{@impl_encode_fields  $($inner)* }
			}
		}
	};


	(@impl_encode_fields $field:ident: $type:ty, $($rest:tt)* ) => {
		::storekey::Encode::<F>::encode(&self.$field)?;
		define_key!{@impl_encode_fields  $($rest)* }
	};

	(@impl_encode_fields $e:expr, $($rest:tt)* ) => {
		::storekey::Encode::<F>::encode(&$e)?;
		define_key!{@impl_encode_fields  $($rest)* }
	};

	(@impl_encode_fields ) => {
		Ok(())
	};
}
pub(crate) use define_key;
