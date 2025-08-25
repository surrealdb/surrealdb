use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Explain(pub bool);

impl fmt::Display for Explain {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("EXPLAIN")?;
		if self.0 {
			f.write_str(" FULL")?;
		}
		Ok(())
	}
}
