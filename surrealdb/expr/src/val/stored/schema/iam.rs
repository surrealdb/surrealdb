//! `StoredField` for the identity types of `surrealdb-iam`.
//!
//! The grammar sits above `surrealdb-iam`, so the derive cannot be applied to its types. Each type
//! is stored through a *representation* declared in the private module [`repr`]: a type with the
//! same shape as the iam type, carrying the derive's attribute table. The iam type is converted to
//! its representation to be written and built from it when read, so the bytes, the floors, the
//! VM shapes and the schema lock entry are exactly the derive's.
//!
//! A conversion into the representation matches every variant of the iam type, and reads a struct
//! through accessors that bind every field of the struct by name. A variant or field added to an
//! iam type therefore fails to compile, here or in `surrealdb-iam`, until it has a number.
//! `Role` is `#[non_exhaustive]`, so its conversion refuses a role it does not know with
//! [`SchemaError::Unencodable`], which no build before the role was numbered can write.

use surrealdb_iam::{
	Actor, Auth, ConfigKind, Level, Resource, ResourceKind, Role, ScramCredential,
};

use super::{Floor, SchemaError, StoredField, StoredType, TypeSchema};
use crate::val::Value;
use crate::val::stored::{Encoder, Item};

/// The shapes the derive numbers, one per iam type. The names are the iam types' own: the schema
/// lock keys on them.
mod repr {
	use super::super::Stored;

	#[derive(Clone, Debug, PartialEq, Stored)]
	#[stored(since = "4.0")]
	pub struct Auth {
		#[stored(n = 2)]
		pub actor: Actor,
	}

	#[derive(Clone, Debug, PartialEq, Stored)]
	#[stored(since = "4.0")]
	pub struct Actor {
		#[stored(n = 2)]
		pub res: Resource,
		#[stored(n = 4)]
		pub roles: Vec<Role>,
	}

	#[derive(Clone, Debug, PartialEq, Stored)]
	#[stored(since = "4.0")]
	pub struct Resource {
		#[stored(n = 2)]
		pub id: String,
		#[stored(n = 4)]
		pub kind: ResourceKind,
		#[stored(n = 6)]
		pub level: Level,
	}

	#[derive(Clone, Debug, PartialEq, Stored)]
	#[stored(since = "4.0")]
	pub enum ResourceKind {
		#[stored(n = 1)]
		Any,
		#[stored(n = 2)]
		Namespace,
		#[stored(n = 3)]
		Database,
		#[stored(n = 4)]
		Record,
		#[stored(n = 5)]
		Table,
		#[stored(n = 6)]
		Document,
		#[stored(n = 7)]
		Option,
		#[stored(n = 8)]
		Function,
		#[stored(n = 9)]
		Module,
		#[stored(n = 10)]
		Analyzer,
		#[stored(n = 11)]
		Parameter,
		#[stored(n = 12)]
		Model,
		#[stored(n = 13)]
		Event,
		#[stored(n = 14)]
		Field,
		#[stored(n = 15)]
		Index,
		#[stored(n = 16)]
		Access,
		#[stored(n = 17)]
		Config(ConfigKind),
		#[stored(n = 18)]
		Api,
		#[stored(n = 19)]
		Bucket,
		#[stored(n = 20)]
		Sequence,
		#[stored(n = 21)]
		Actor,
	}

	#[derive(Clone, Debug, PartialEq, Stored)]
	#[stored(since = "4.0")]
	pub enum ConfigKind {
		#[stored(n = 1)]
		GraphQL,
		#[stored(n = 2)]
		Api,
		#[stored(n = 3)]
		Default,
	}

	#[derive(Clone, Debug, PartialEq, Stored)]
	#[stored(since = "4.0")]
	pub enum Level {
		#[stored(n = 1)]
		No,
		#[stored(n = 2)]
		Root,
		#[stored(n = 3)]
		Namespace(String),
		#[stored(n = 4)]
		Database(String, String),
		#[stored(n = 5)]
		Record(String, String, String),
	}

	#[derive(Clone, Debug, PartialEq, Stored)]
	#[stored(since = "4.0")]
	pub enum Role {
		#[stored(n = 1)]
		Viewer,
		#[stored(n = 2)]
		Editor,
		#[stored(n = 3)]
		Owner,
	}

	#[derive(Clone, Debug, PartialEq, Stored)]
	#[stored(since = "4.0")]
	pub struct ScramCredential {
		#[stored(n = 2)]
		pub iterations: u32,
		#[stored(n = 4)]
		pub salt: Vec<u8>,
		#[stored(n = 6)]
		pub stored_key: Vec<u8>,
		#[stored(n = 8)]
		pub server_key: Vec<u8>,
	}
}

/// An iam type and the representation it is stored as.
trait Mirrored: Sized {
	type Repr: StoredType;

	/// The representation of `self`, or why it cannot be stored.
	fn to_repr(&self) -> Result<Self::Repr, SchemaError>;

	/// The iam value a representation read from stored bytes denotes.
	fn from_repr(repr: Self::Repr) -> Self;
}

/// Implements `StoredField` and `StoredType` for an iam type by delegating to its representation.
macro_rules! delegate {
	($($iam:ty),* $(,)?) => {
		$(
			impl StoredField for $iam {
				fn encode_item(
					&self,
					enc: &mut Encoder,
					prior: Option<Item<'_>>,
				) -> Result<(), SchemaError> {
					self.to_repr()?.encode_item(enc, prior)
				}

				fn decode_item(item: Item<'_>, depth: u32) -> Result<Self, SchemaError> {
					<<$iam as Mirrored>::Repr as StoredField>::decode_item(item, depth)
						.map(<$iam as Mirrored>::from_repr)
				}

				fn floor(&self, acc: &mut Floor) -> Result<(), SchemaError> {
					self.to_repr()?.floor(acc)
				}

				fn into_vm_value(self) -> Result<Value, SchemaError> {
					self.to_repr()?.into_vm_value()
				}

				fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
					<<$iam as Mirrored>::Repr as StoredField>::from_vm_value(value)
						.map(<$iam as Mirrored>::from_repr)
				}

				fn write_type_name(out: &mut String) {
					<<$iam as Mirrored>::Repr as StoredField>::write_type_name(out);
				}

				fn collect_schemas(out: &mut Vec<&'static TypeSchema>) {
					<<$iam as Mirrored>::Repr as StoredField>::collect_schemas(out);
				}
			}

			impl StoredType for $iam {
				const SCHEMA: TypeSchema = <<$iam as Mirrored>::Repr as StoredType>::SCHEMA;
			}
		)*
	};
}

delegate!(Auth, Actor, Resource, ResourceKind, ConfigKind, Level, Role, ScramCredential);

impl Mirrored for Auth {
	type Repr = repr::Auth;

	fn to_repr(&self) -> Result<repr::Auth, SchemaError> {
		Ok(repr::Auth {
			actor: self.actor().to_repr()?,
		})
	}

	fn from_repr(repr: repr::Auth) -> Self {
		let repr::Auth {
			actor,
		} = repr;
		Auth::new(Actor::from_repr(actor))
	}
}

impl Mirrored for Actor {
	type Repr = repr::Actor;

	fn to_repr(&self) -> Result<repr::Actor, SchemaError> {
		Ok(repr::Actor {
			res: self.resource().to_repr()?,
			roles: self.roles().iter().map(Role::to_repr).collect::<Result<_, _>>()?,
		})
	}

	fn from_repr(repr: repr::Actor) -> Self {
		let repr::Actor {
			res,
			roles,
		} = repr;
		Actor::from_parts(
			Resource::from_repr(res),
			roles.into_iter().map(Role::from_repr).collect(),
		)
	}
}

impl Mirrored for Resource {
	type Repr = repr::Resource;

	fn to_repr(&self) -> Result<repr::Resource, SchemaError> {
		Ok(repr::Resource {
			id: self.id().to_owned(),
			kind: self.kind().to_repr()?,
			level: self.level().to_repr()?,
		})
	}

	fn from_repr(repr: repr::Resource) -> Self {
		let repr::Resource {
			id,
			kind,
			level,
		} = repr;
		Resource::from_parts(id, ResourceKind::from_repr(kind), Level::from_repr(level))
	}
}

impl Mirrored for ResourceKind {
	type Repr = repr::ResourceKind;

	fn to_repr(&self) -> Result<repr::ResourceKind, SchemaError> {
		Ok(match self {
			ResourceKind::Any => repr::ResourceKind::Any,
			ResourceKind::Namespace => repr::ResourceKind::Namespace,
			ResourceKind::Database => repr::ResourceKind::Database,
			ResourceKind::Record => repr::ResourceKind::Record,
			ResourceKind::Table => repr::ResourceKind::Table,
			ResourceKind::Document => repr::ResourceKind::Document,
			ResourceKind::Option => repr::ResourceKind::Option,
			ResourceKind::Function => repr::ResourceKind::Function,
			ResourceKind::Module => repr::ResourceKind::Module,
			ResourceKind::Analyzer => repr::ResourceKind::Analyzer,
			ResourceKind::Parameter => repr::ResourceKind::Parameter,
			ResourceKind::Model => repr::ResourceKind::Model,
			ResourceKind::Event => repr::ResourceKind::Event,
			ResourceKind::Field => repr::ResourceKind::Field,
			ResourceKind::Index => repr::ResourceKind::Index,
			ResourceKind::Access => repr::ResourceKind::Access,
			ResourceKind::Config(kind) => repr::ResourceKind::Config(kind.to_repr()?),
			ResourceKind::Api => repr::ResourceKind::Api,
			ResourceKind::Bucket => repr::ResourceKind::Bucket,
			ResourceKind::Sequence => repr::ResourceKind::Sequence,
			ResourceKind::Actor => repr::ResourceKind::Actor,
		})
	}

	fn from_repr(repr: repr::ResourceKind) -> Self {
		match repr {
			repr::ResourceKind::Any => ResourceKind::Any,
			repr::ResourceKind::Namespace => ResourceKind::Namespace,
			repr::ResourceKind::Database => ResourceKind::Database,
			repr::ResourceKind::Record => ResourceKind::Record,
			repr::ResourceKind::Table => ResourceKind::Table,
			repr::ResourceKind::Document => ResourceKind::Document,
			repr::ResourceKind::Option => ResourceKind::Option,
			repr::ResourceKind::Function => ResourceKind::Function,
			repr::ResourceKind::Module => ResourceKind::Module,
			repr::ResourceKind::Analyzer => ResourceKind::Analyzer,
			repr::ResourceKind::Parameter => ResourceKind::Parameter,
			repr::ResourceKind::Model => ResourceKind::Model,
			repr::ResourceKind::Event => ResourceKind::Event,
			repr::ResourceKind::Field => ResourceKind::Field,
			repr::ResourceKind::Index => ResourceKind::Index,
			repr::ResourceKind::Access => ResourceKind::Access,
			repr::ResourceKind::Config(kind) => ResourceKind::Config(ConfigKind::from_repr(kind)),
			repr::ResourceKind::Api => ResourceKind::Api,
			repr::ResourceKind::Bucket => ResourceKind::Bucket,
			repr::ResourceKind::Sequence => ResourceKind::Sequence,
			repr::ResourceKind::Actor => ResourceKind::Actor,
		}
	}
}

impl Mirrored for ConfigKind {
	type Repr = repr::ConfigKind;

	fn to_repr(&self) -> Result<repr::ConfigKind, SchemaError> {
		Ok(match self {
			ConfigKind::GraphQL => repr::ConfigKind::GraphQL,
			ConfigKind::Api => repr::ConfigKind::Api,
			ConfigKind::Default => repr::ConfigKind::Default,
		})
	}

	fn from_repr(repr: repr::ConfigKind) -> Self {
		match repr {
			repr::ConfigKind::GraphQL => ConfigKind::GraphQL,
			repr::ConfigKind::Api => ConfigKind::Api,
			repr::ConfigKind::Default => ConfigKind::Default,
		}
	}
}

impl Mirrored for Level {
	type Repr = repr::Level;

	fn to_repr(&self) -> Result<repr::Level, SchemaError> {
		Ok(match self {
			Level::No => repr::Level::No,
			Level::Root => repr::Level::Root,
			Level::Namespace(ns) => repr::Level::Namespace(ns.clone()),
			Level::Database(ns, db) => repr::Level::Database(ns.clone(), db.clone()),
			Level::Record(ns, db, id) => repr::Level::Record(ns.clone(), db.clone(), id.clone()),
		})
	}

	fn from_repr(repr: repr::Level) -> Self {
		match repr {
			repr::Level::No => Level::No,
			repr::Level::Root => Level::Root,
			repr::Level::Namespace(ns) => Level::Namespace(ns),
			repr::Level::Database(ns, db) => Level::Database(ns, db),
			repr::Level::Record(ns, db, id) => Level::Record(ns, db, id),
		}
	}
}

impl Mirrored for Role {
	type Repr = repr::Role;

	fn to_repr(&self) -> Result<repr::Role, SchemaError> {
		// `Role` is `#[non_exhaustive]`: a role added to it that has no number here cannot be
		// stored, and is refused rather than written as another role.
		match self {
			Role::Viewer => Ok(repr::Role::Viewer),
			Role::Editor => Ok(repr::Role::Editor),
			Role::Owner => Ok(repr::Role::Owner),
			_ => Err(SchemaError::Unencodable {
				ty: "Role",
				what: "a role that has no stored number",
			}),
		}
	}

	fn from_repr(repr: repr::Role) -> Self {
		match repr {
			repr::Role::Viewer => Role::Viewer,
			repr::Role::Editor => Role::Editor,
			repr::Role::Owner => Role::Owner,
		}
	}
}

impl Mirrored for ScramCredential {
	type Repr = repr::ScramCredential;

	fn to_repr(&self) -> Result<repr::ScramCredential, SchemaError> {
		let ScramCredential {
			iterations,
			salt,
			stored_key,
			server_key,
		} = self;
		Ok(repr::ScramCredential {
			iterations: *iterations,
			salt: salt.clone(),
			stored_key: stored_key.clone(),
			server_key: server_key.clone(),
		})
	}

	fn from_repr(repr: repr::ScramCredential) -> Self {
		let repr::ScramCredential {
			iterations,
			salt,
			stored_key,
			server_key,
		} = repr;
		ScramCredential {
			iterations,
			salt,
			stored_key,
			server_key,
		}
	}
}
