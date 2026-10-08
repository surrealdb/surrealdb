pub(crate) mod api;
pub(crate) mod defaults;

use crate::catalog::base::Base;
use crate::iam::ConfigKind;

/// Map a config kind to the base at which it is administered.
pub(crate) fn config_kind_base(kind: &ConfigKind) -> Base {
	match kind {
		ConfigKind::Default => Base::Root,
		ConfigKind::GraphQL | ConfigKind::Api => Base::Db,
	}
}
