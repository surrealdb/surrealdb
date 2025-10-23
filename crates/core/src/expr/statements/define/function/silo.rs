use std::fmt::{self, Write};

use crate::catalog::Permission;
use crate::expr::statements::define::DefineKind;
use crate::expr::{Expr, SiloExecutable};
use crate::fmt::{is_pretty, pretty_indent};

#[allow(dead_code)]
pub struct DefineSiloFunction {
	pub kind: DefineKind,
	pub comment: Option<Expr>,
	pub permissions: Permission,
	pub executable: SiloExecutable,
}

impl fmt::Display for DefineSiloFunction {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"DEFINE FUNCTION silo::{}::{}<{}.{}.{}>",
			self.executable.organisation,
			self.executable.package,
			self.executable.major,
			self.executable.minor,
			self.executable.patch
		)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", v)?
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		write!(f, "PERMISSIONS {}", self.permissions)?;
		Ok(())
	}
}
