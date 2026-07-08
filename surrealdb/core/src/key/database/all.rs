//! Stores the key prefix for all keys under a database

use ::storekey::{BorrowDecode, BorrowReader, DecodeError, Encode, EncodeError, Writer};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::impl_kv_range_storekey;

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub(crate) struct DatabaseRoot {
	pub ns: NamespaceId,
	pub db: DatabaseId,
}
impl<T> Encode<T> for DatabaseRoot {
	fn encode<W>(&self, w: &mut Writer<W>) -> Result<(), EncodeError>
	where
		W: ::std::io::Write,
	{
		Encode::<T>::encode(&b'/', w)?;
		Encode::<T>::encode(&b'*', w)?;
		Encode::<T>::encode(&self.ns, w)?;
		Encode::<T>::encode(&b'*', w)?;
		Encode::<T>::encode(&self.db, w)?;
		Ok(())
	}
}

impl<'de, F> BorrowDecode<'de, F> for DatabaseRoot {
	fn borrow_decode(r: &mut BorrowReader<'de>) -> Result<Self, DecodeError> {
		if r.read_u8()? != b'/' {
			return Err(DecodeError::InvalidFormat);
		}
		if r.read_u8()? != b'*' {
			return Err(DecodeError::InvalidFormat);
		}
		let ns: NamespaceId = BorrowDecode::<F>::borrow_decode(r)?;
		if r.read_u8()? != b'*' {
			return Err(DecodeError::InvalidFormat);
		}
		let db: DatabaseId = BorrowDecode::<F>::borrow_decode(r)?;
		Ok(Self {
			db,
			ns,
		})
	}
}
impl_kv_range_storekey!(DatabaseRoot);

impl Categorise for DatabaseRoot {
	fn categorise(&self) -> Category {
		Category::DatabaseRoot
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVRange;

	#[test]
	fn key() {
		let val = DatabaseRoot {
			ns: NamespaceId(1),
			db: DatabaseId(2),
		};
		let enc = val.encode_range().unwrap();
		assert_eq!(enc.start.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02\0");
	}
}
