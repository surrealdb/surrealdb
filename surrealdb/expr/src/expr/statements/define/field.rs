use super::DefineKind;
use crate::expr::permission::Permissions;
use crate::expr::reference::Reference;
use crate::expr::{Expr, Kind, KindLiteral, Literal};

/// Returns true if this type contains an `object` anywhere (including literal
/// object types, `array<object>` and `option<object>`).
pub fn kind_contains_object(kind: &Kind) -> bool {
	match kind {
		Kind::Object => true,
		Kind::Either(kinds) => kinds.iter().any(kind_contains_object),
		Kind::Array(inner, _) | Kind::Set(inner, _) => kind_contains_object(inner),
		Kind::Literal(KindLiteral::Object(_)) => true,
		Kind::Literal(KindLiteral::Array(kinds)) => kinds.iter().any(kind_contains_object),
		_ => false,
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum DefineDefault {
	#[default]
	None,
	Always(Expr),
	Set(Expr),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineFieldStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub what: Expr,
	pub field_kind: Option<Kind>,
	pub flexible: bool,
	pub readonly: bool,
	pub value: Option<Expr>,
	pub assert: Option<Expr>,
	pub computed: Option<Expr>,
	pub default: DefineDefault,
	pub permissions: Permissions,
	pub comment: Expr,
	pub reference: Option<Reference>,
	pub graphql_alias: Option<String>,
	pub graphql_deprecated: Option<String>,
}

impl Default for DefineFieldStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			field_kind: None,
			flexible: false,
			readonly: false,
			value: None,
			assert: None,
			computed: None,
			default: DefineDefault::None,
			permissions: Permissions::default(),
			comment: Expr::Literal(Literal::None),
			reference: None,
			graphql_alias: None,
			graphql_deprecated: None,
		}
	}
}
