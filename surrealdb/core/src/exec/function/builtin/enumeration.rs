//! Enum functions

use anyhow::Result;

use crate::exec::EvalContext;
use crate::exec::function::{ScalarFunction, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::expr::Kind;
use crate::expr::logical::EffectSet;
use crate::fnc::args::FromArgs;
use crate::val::Value;

macro_rules! enum_function {
	($ty:ident, $name:literal, $sig:expr, $run:path) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $ty;

		impl ScalarFunction for $ty {
			fn name(&self) -> &'static str {
				$name
			}

			fn fallibility(&self) -> Fallibility {
				crate::declare_fallibility!(Scalar, $name, Fallibility::DATA)
			}

			fn effects(&self) -> EffectSet {
				EffectSet::READS_CATALOG
			}

			fn signature(&self) -> Signature {
				$sig
			}

			fn is_pure(&self) -> bool {
				false
			}

			fn is_async(&self) -> bool {
				true
			}

			fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
				Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
			}

			fn invoke_async<'a>(
				&'a self,
				ctx: &'a EvalContext<'_>,
				args: Vec<Value>,
			) -> crate::exec::BoxFut<'a, Result<Value>> {
				Box::pin(async move {
					let args = FromArgs::from_args($name, args)?;
					let frozen = ctx.exec_ctx.ctx();
					let opt = ctx.exec_ctx.options();
					$run((frozen, opt), args).await
				})
			}
		}
	};
}

enum_function!(
	EnumValues,
	"enum::values",
	Signature::new().arg("name", Kind::Any).returns(Kind::Array(Box::new(Kind::String), None)),
	crate::fnc::enumeration::values
);
enum_function!(
	EnumRank,
	"enum::rank",
	Signature::new().arg("name", Kind::Any).arg("value", Kind::Any).returns(Kind::Any),
	crate::fnc::enumeration::rank
);
enum_function!(
	EnumMin,
	"enum::min",
	Signature::new().arg("name", Kind::Any).arg("values", Kind::Any).returns(Kind::Any),
	crate::fnc::enumeration::min
);
enum_function!(
	EnumMax,
	"enum::max",
	Signature::new().arg("name", Kind::Any).arg("values", Kind::Any).returns(Kind::Any),
	crate::fnc::enumeration::max
);

/// `enum::cast(type, value)`: `value` cast to the type `type` spells, judged by every named type
/// it holds. This is what a cast to a type that names a type of the catalog evaluates to.
#[derive(Debug, Clone, Copy, Default)]
pub struct EnumCast;

impl ScalarFunction for EnumCast {
	fn name(&self) -> &'static str {
		"enum::cast"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Scalar, "enum::cast", Fallibility::DATA)
	}

	fn effects(&self) -> EffectSet {
		EffectSet::READS_CATALOG
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("name", Kind::Any).arg("value", Kind::Any).returns(Kind::Any)
	}

	fn is_pure(&self) -> bool {
		false
	}

	fn is_async(&self) -> bool {
		true
	}

	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let (name, value) = FromArgs::from_args("enum::cast", args)?;
			crate::fnc::enumeration::cast_in(ctx.exec_ctx, (name, value)).await
		})
	}
}

/// The `enum::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] =
	&[&EnumCast, &EnumMax, &EnumMin, &EnumRank, &EnumValues];
