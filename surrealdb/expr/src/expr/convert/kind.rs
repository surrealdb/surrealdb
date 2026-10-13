//! `sql` -> `expr` conversions for [`crate::sql::kind`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::kind::*;

impl From<GeometryKind> for crate::expr::kind::GeometryKind {
	fn from(v: GeometryKind) -> Self {
		match v {
			GeometryKind::Point => crate::expr::kind::GeometryKind::Point,
			GeometryKind::Line => crate::expr::kind::GeometryKind::Line,
			GeometryKind::Polygon => crate::expr::kind::GeometryKind::Polygon,
			GeometryKind::MultiPoint => crate::expr::kind::GeometryKind::MultiPoint,
			GeometryKind::MultiLine => crate::expr::kind::GeometryKind::MultiLine,
			GeometryKind::MultiPolygon => crate::expr::kind::GeometryKind::MultiPolygon,
			GeometryKind::Collection => crate::expr::kind::GeometryKind::Collection,
		}
	}
}

impl From<crate::expr::kind::GeometryKind> for GeometryKind {
	fn from(v: crate::expr::kind::GeometryKind) -> Self {
		match v {
			crate::expr::kind::GeometryKind::Point => GeometryKind::Point,
			crate::expr::kind::GeometryKind::Line => GeometryKind::Line,
			crate::expr::kind::GeometryKind::Polygon => GeometryKind::Polygon,
			crate::expr::kind::GeometryKind::MultiPoint => GeometryKind::MultiPoint,
			crate::expr::kind::GeometryKind::MultiLine => GeometryKind::MultiLine,
			crate::expr::kind::GeometryKind::MultiPolygon => GeometryKind::MultiPolygon,
			crate::expr::kind::GeometryKind::Collection => GeometryKind::Collection,
		}
	}
}

impl From<Kind> for crate::expr::Kind {
	fn from(v: Kind) -> Self {
		match v {
			Kind::Any => crate::expr::Kind::Any,
			Kind::None => crate::expr::Kind::None,
			Kind::Null => crate::expr::Kind::Null,
			Kind::Bool => crate::expr::Kind::Bool,
			Kind::Bytes => crate::expr::Kind::Bytes,
			Kind::Datetime => crate::expr::Kind::Datetime,
			Kind::Decimal => crate::expr::Kind::Decimal,
			Kind::Duration => crate::expr::Kind::Duration,
			Kind::Float => crate::expr::Kind::Float,
			Kind::Int => crate::expr::Kind::Int,
			Kind::Number => crate::expr::Kind::Number,
			Kind::Object => crate::expr::Kind::Object,
			Kind::String => crate::expr::Kind::String,
			Kind::Uuid => crate::expr::Kind::Uuid,
			Kind::Regex => crate::expr::Kind::Regex,
			Kind::Table(tables) => {
				crate::expr::Kind::Table(tables.into_iter().map(Into::into).collect())
			}
			Kind::Record(tables) => {
				crate::expr::Kind::Record(tables.into_iter().map(Into::into).collect())
			}
			Kind::Geometry(geometries) => {
				crate::expr::Kind::Geometry(geometries.into_iter().map(Into::into).collect())
			}
			Kind::Either(kinds) => {
				crate::expr::Kind::Either(kinds.into_iter().map(Into::into).collect())
			}
			Kind::Set(k, l) => crate::expr::Kind::Set(Box::new(k.as_ref().clone().into()), l),
			Kind::Array(k, l) => crate::expr::Kind::Array(Box::new(k.as_ref().clone().into()), l),
			Kind::Function(args, ret) => crate::expr::Kind::Function(
				args.map(|args| args.into_iter().map(Into::into).collect()),
				ret.map(|ret| Box::new((*ret).into())),
			),
			Kind::Range => crate::expr::Kind::Range,
			Kind::Literal(l) => crate::expr::Kind::Literal(l.into()),
			Kind::File(k) => crate::expr::Kind::File(k),
		}
	}
}

impl From<crate::expr::Kind> for Kind {
	fn from(v: crate::expr::Kind) -> Self {
		match v {
			crate::expr::Kind::Any => Kind::Any,
			crate::expr::Kind::None => Kind::None,
			crate::expr::Kind::Null => Kind::Null,
			crate::expr::Kind::Bool => Kind::Bool,
			crate::expr::Kind::Bytes => Kind::Bytes,
			crate::expr::Kind::Datetime => Kind::Datetime,
			crate::expr::Kind::Decimal => Kind::Decimal,
			crate::expr::Kind::Duration => Kind::Duration,
			crate::expr::Kind::Float => Kind::Float,
			crate::expr::Kind::Int => Kind::Int,
			crate::expr::Kind::Number => Kind::Number,
			crate::expr::Kind::Object => Kind::Object,
			crate::expr::Kind::String => Kind::String,
			crate::expr::Kind::Uuid => Kind::Uuid,
			crate::expr::Kind::Regex => Kind::Regex,
			crate::expr::Kind::Table(tables) => {
				Kind::Table(tables.into_iter().map(Into::into).collect())
			}
			crate::expr::Kind::Record(tables) => {
				Kind::Record(tables.into_iter().map(Into::into).collect())
			}
			crate::expr::Kind::Geometry(geometries) => {
				Kind::Geometry(geometries.into_iter().map(Into::into).collect())
			}
			crate::expr::Kind::Either(kinds) => {
				let kinds: Vec<Kind> = kinds.into_iter().map(Into::into).collect();
				if kinds.is_empty() {
					return Self::Either(vec![Self::Any]);
				}
				Self::Either(kinds)
			}
			crate::expr::Kind::Set(k, l) => Self::Set(Box::new((*k).into()), l),
			crate::expr::Kind::Array(k, l) => Self::Array(Box::new((*k).into()), l),
			crate::expr::Kind::Function(args, ret) => Self::Function(
				args.map(|args| args.into_iter().map(Into::into).collect()),
				ret.map(|ret| Box::new((*ret).into())),
			),
			crate::expr::Kind::Range => Self::Range,
			crate::expr::Kind::Literal(l) => Self::Literal(l.into()),
			crate::expr::Kind::File(k) => Kind::File(k),
		}
	}
}

impl From<KindLiteral> for crate::expr::kind::KindLiteral {
	fn from(v: KindLiteral) -> Self {
		match v {
			KindLiteral::String(s) => crate::expr::kind::KindLiteral::String(s),
			KindLiteral::Integer(i) => crate::expr::kind::KindLiteral::Integer(i),
			KindLiteral::Float(f) => crate::expr::kind::KindLiteral::Float(f),
			KindLiteral::Decimal(d) => crate::expr::kind::KindLiteral::Decimal(d),
			KindLiteral::Duration(d) => crate::expr::kind::KindLiteral::Duration(d.into()),
			KindLiteral::Array(a) => {
				crate::expr::kind::KindLiteral::Array(a.into_iter().map(Into::into).collect())
			}
			KindLiteral::Object(o) => crate::expr::kind::KindLiteral::Object(
				o.into_iter().map(|(k, v)| (k, v.into())).collect(),
			),
			KindLiteral::Bool(b) => crate::expr::kind::KindLiteral::Bool(b),
		}
	}
}

impl From<crate::expr::kind::KindLiteral> for KindLiteral {
	fn from(v: crate::expr::kind::KindLiteral) -> Self {
		match v {
			crate::expr::kind::KindLiteral::String(s) => Self::String(s),
			crate::expr::kind::KindLiteral::Integer(i) => Self::Integer(i),
			crate::expr::kind::KindLiteral::Float(f) => Self::Float(f),
			crate::expr::kind::KindLiteral::Decimal(d) => Self::Decimal(d),
			crate::expr::kind::KindLiteral::Duration(d) => Self::Duration(d.into()),
			crate::expr::kind::KindLiteral::Array(a) => {
				Self::Array(a.into_iter().map(Into::into).collect())
			}
			crate::expr::kind::KindLiteral::Object(o) => {
				Self::Object(o.into_iter().map(|(k, v)| (k, v.into())).collect())
			}
			crate::expr::kind::KindLiteral::Bool(b) => Self::Bool(b),
		}
	}
}
