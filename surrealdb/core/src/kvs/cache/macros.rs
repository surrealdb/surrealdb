/// Macro to implement the `CacheKey` trait for a cache key type.
///
/// # Usage
///
/// With default policy (Standard):
/// ```ignore
/// impl_cache_key!(MyKey, Arc<MyValue>);
/// ```
///
/// With explicit policy:
/// ```ignore
/// impl_cache_key!(MyKey, Arc<MyValue>, Critical);
/// impl_cache_key!(MyKey, Arc<MyValue>, Volatile);
/// ```
macro_rules! impl_cache_key {
	($key:ty, $value:ty) => {
		impl priority_lfu::CacheKey for $key {
			type Value = $value;
		}
	};
	($key:ty, $value:ty, $policy:ident) => {
		impl priority_lfu::CacheKey for $key {
			type Value = $value;
			fn policy(&self) -> priority_lfu::CachePolicy {
				priority_lfu::CachePolicy::$policy
			}
		}
	};
}

/// Macro to implement the `CacheKeyLookup` trait for borrowed cache key types.
///
/// This macro generates the `CacheKeyLookup` implementation for a borrowed key type
/// that can be used to look up entries stored with an owned key type.
///
/// # Conversion Strategies
///
/// - `copy` - for `Copy` types (NamespaceId, DatabaseId, Uuid, etc.)
/// - `to_string` - for String fields (borrowed as &str)
/// - `clone` - for Clone types that aren't Copy
/// - `deref` - for dereferencing borrowed types (e.g., &RecordIdKey -> RecordIdKey)
///
/// # Usage
///
/// ```ignore
/// #[derive(Clone, Hash, Eq, PartialEq)]
/// pub struct DbCacheKey(pub String, pub String);
///
/// #[derive(Clone, Hash, Eq, PartialEq)]
/// pub struct DbCacheKeyRef<'a>(pub &'a str, pub &'a str);
///
/// impl_cache_key_lookup!(DbCacheKeyRef<'a> => DbCacheKey {
///     0 => to_owned,
///     1 => to_owned,
/// });
/// ```
///
/// For keys with mixed field types:
/// ```ignore
/// #[derive(Clone, Hash, Eq, PartialEq)]
/// pub struct EventsCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub Uuid);
///
/// #[derive(Clone, Hash, Eq, PartialEq)]
/// pub struct EventsCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str, pub Uuid);
///
/// impl_cache_key_lookup!(EventsCacheKeyRef<'a> => EventsCacheKey {
///     0 => copy,      // NamespaceId
///     1 => copy,      // DatabaseId
///     2 => to_owned,  // String
///     3 => copy,      // Uuid
/// });
/// ```
///
/// For keys with borrowed non-Copy types:
/// ```ignore
/// #[derive(Clone, Hash, Eq, PartialEq)]
/// pub struct RecordCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub RecordIdKey);
///
/// #[derive(Clone, Hash, Eq, PartialEq)]
/// pub struct RecordCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str, pub &'a RecordIdKey);
///
/// impl_cache_key_lookup!(RecordCacheKeyRef<'a> => RecordCacheKey {
///     0 => copy,      // NamespaceId
///     1 => copy,      // DatabaseId
///     2 => to_owned,  // String
///     3 => to_owned,  // RecordIdKey
/// });
/// ```
macro_rules! impl_cache_key_lookup {
	($ref:ident<$lt:lifetime> => $owned:ident { $($idx:tt => $conv:ident),* $(,)? }) => {
		impl<$lt> priority_lfu::CacheKeyLookup<$owned> for $ref<$lt> {
			fn eq_key(&self, key: &$owned) -> bool {
				$(self.$idx.eq(&key.$idx))&&*
			}
			fn to_owned_key(self) -> $owned {
				$owned($(impl_cache_key_lookup!(@conv $conv self.$idx)),*)
			}
		}
	};
	(@conv copy $e:expr) => { $e };
	(@conv to_owned $e:expr) => { $e.to_owned() };
}
