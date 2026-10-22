use crate::val::stored;

/// Why a schema type could not be read from stored bytes, written, or converted to or from a VM
/// value.
///
/// An error names the Rust type and the field number it concerns, and never a value: catalog
/// entries hold secrets. A field number of 0 names the item itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
	#[error("{0}")]
	Grammar(stored::Error),
	#[error("{ty} holds the wrong kind of item in field {n}")]
	WrongKind {
		ty: &'static str,
		n: u32,
	},
	#[error("the fields of {ty} are out of order or repeated")]
	FieldOrder {
		ty: &'static str,
	},
	#[error("{ty} has field {n}, which this build does not read and may not ignore")]
	UnknownCriticalField {
		ty: &'static str,
		n: u32,
	},
	#[error("{ty} has variant {n}, which this build does not read")]
	UnknownVariant {
		ty: &'static str,
		n: u32,
	},
	#[error("{ty} must hold exactly one variant")]
	NotOneVariant {
		ty: &'static str,
	},
	#[error("{ty} is missing its required field {n}")]
	MissingField {
		ty: &'static str,
		n: u32,
	},
	#[error("field {n} of {ty} is out of range")]
	OutOfRange {
		ty: &'static str,
		n: u32,
	},
	#[error("{ty} nests deeper than the schema allows")]
	TooDeep {
		ty: &'static str,
	},
	#[error("{ty} holds a legacy variant that has no current form")]
	LegacyVariant {
		ty: &'static str,
	},
	#[error("{ty} cannot be stored: {what}")]
	Unencodable {
		ty: &'static str,
		what: &'static str,
	},
	#[error("the VM value for {ty} has the wrong shape: {what}")]
	VmShape {
		ty: &'static str,
		what: &'static str,
	},
}

impl From<stored::Error> for SchemaError {
	fn from(e: stored::Error) -> Self {
		Self::Grammar(e)
	}
}

impl SchemaError {
	/// A refusal of a scalar, which names neither the type nor the field it sits in, located at
	/// field `n` of `ty`. Every other error already names its own type and is returned as it is.
	pub fn at(self, ty: &'static str, n: u32) -> Self {
		if matches!(
			self,
			Self::WrongKind {
				ty: "",
				n: 0,
			}
		) {
			Self::WrongKind {
				ty,
				n,
			}
		} else if matches!(
			self,
			Self::OutOfRange {
				ty: "",
				n: 0,
			}
		) {
			Self::OutOfRange {
				ty,
				n,
			}
		} else {
			self
		}
	}

	/// A scalar of the wrong kind: located by [`SchemaError::at`].
	pub const fn wrong_kind() -> Self {
		Self::WrongKind {
			ty: "",
			n: 0,
		}
	}

	/// A scalar out of its field's range: located by [`SchemaError::at`].
	pub const fn out_of_range() -> Self {
		Self::OutOfRange {
			ty: "",
			n: 0,
		}
	}

	/// A VM value of the wrong shape for `ty`.
	pub const fn vm(ty: &'static str, what: &'static str) -> Self {
		Self::VmShape {
			ty,
			what,
		}
	}
}
