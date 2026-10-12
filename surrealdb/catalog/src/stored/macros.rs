//! The `KVValue` impls of catalog types.
//!
//! Both macros set `LEVELLED`, so a transaction writes the value through
//! `KVValue::kv_encode_value_at` with the store's feature level and the bytes it replaces.

/// Implements `KVValue` for a catalog type written in either grammar: `revision` at the Legacy
/// level, the stored grammar above it.
///
/// `kv_encode_value` is the Legacy encoding, which a 3.x reader can read, and is refused for a
/// value that `revision` cannot hold. Reads accept either grammar. The type must derive
/// `Stored` and have a `revision` form.
#[macro_export]
macro_rules! impl_kv_value_catalog {
	($t:ident) => {
		impl $crate::stored::private::KVValue for $t {
			type KeyContext = ();

			const LEVELLED: bool = true;

			fn kv_encode_value(&self) -> $crate::stored::private::anyhow::Result<Vec<u8>> {
				Ok($crate::stored::encode_entry(
					self,
					&$crate::stored::private::EncodeContext::LEGACY,
				)
				.map_err($crate::stored::write_error)?
				.bytes)
			}

			fn kv_encode_value_at(
				&self,
				ctx: &$crate::stored::private::EncodeContext<'_>,
			) -> $crate::stored::private::anyhow::Result<$crate::stored::private::Encoded> {
				$crate::stored::encode_entry(self, ctx).map_err($crate::stored::write_error)
			}

			fn kv_canonical(&self) -> $crate::stored::private::anyhow::Result<Vec<u8>> {
				Ok($crate::stored::canonical(self).map_err($crate::Error::Entry)?)
			}

			fn kv_decode_value(
				bytes: &[u8],
				_: (),
			) -> $crate::stored::private::anyhow::Result<Self> {
				$crate::stored::decode_entry_or_revision::<$t>(bytes)
			}
		}
	};
}

/// Implements `KVValue` for a catalog type that has no `revision` form: the stored grammar only,
/// refused at the Legacy level, and written only through the levelled transaction path.
///
/// `kv_encode_value`, which has no store level to consult, is always refused with
/// `EntryError::Unlevelled`.
#[macro_export]
macro_rules! impl_kv_value_stored {
	($t:ident) => {
		impl $crate::stored::private::KVValue for $t {
			type KeyContext = ();

			const LEVELLED: bool = true;

			fn kv_encode_value(&self) -> $crate::stored::private::anyhow::Result<Vec<u8>> {
				Err($crate::Error::Entry($crate::stored::EntryError::Unlevelled {
					ty: <$t as $crate::stored::StoredType>::SCHEMA.name,
				})
				.into())
			}

			fn kv_encode_value_at(
				&self,
				ctx: &$crate::stored::private::EncodeContext<'_>,
			) -> $crate::stored::private::anyhow::Result<$crate::stored::private::Encoded> {
				$crate::stored::encode_stored(self, ctx).map_err($crate::stored::write_error)
			}

			fn kv_canonical(&self) -> $crate::stored::private::anyhow::Result<Vec<u8>> {
				Ok($crate::stored::canonical(self).map_err($crate::Error::Entry)?)
			}

			fn kv_decode_value(
				bytes: &[u8],
				_: (),
			) -> $crate::stored::private::anyhow::Result<Self> {
				Ok($crate::stored::decode_entry::<$t>(bytes).map_err($crate::Error::Entry)?)
			}
		}
	};
}

/// Implements `StoredField` for an identifier newtype over a `u32`: an `INT`, written and read
/// exactly as a `u32` is, so the wire type name is `int:u32`.
///
/// An identifier is bound to no key of its own and is held only inside the entries that name it.
macro_rules! impl_stored_id {
	($t:ident) => {
		impl $crate::stored::StoredField for $t {
			fn encode_item(
				&self,
				enc: &mut ::surrealdb_expr::val::stored::Encoder,
				prior: Option<::surrealdb_expr::val::stored::Item<'_>>,
			) -> Result<(), $crate::stored::SchemaError> {
				$crate::stored::StoredField::encode_item(&self.0, enc, prior)
			}

			fn decode_item(
				item: ::surrealdb_expr::val::stored::Item<'_>,
				depth: u32,
			) -> Result<Self, $crate::stored::SchemaError> {
				<u32 as $crate::stored::StoredField>::decode_item(item, depth).map($t)
			}

			fn into_vm_value(
				self,
			) -> Result<::surrealdb_expr::val::Value, $crate::stored::SchemaError> {
				$crate::stored::StoredField::into_vm_value(self.0)
			}

			fn from_vm_value(
				value: ::surrealdb_expr::val::Value,
			) -> Result<Self, $crate::stored::SchemaError> {
				<u32 as $crate::stored::StoredField>::from_vm_value(value).map($t)
			}

			fn write_type_name(out: &mut String) {
				<u32 as $crate::stored::StoredField>::write_type_name(out);
			}
		}
	};
}

pub(crate) use impl_stored_id;
