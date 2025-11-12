use std::fmt;

pub fn format_seperated<'a, I>(i: &'a [I], seperator: &'a str) -> Seperated<'a, I> {
	Seperated {
		items: i,
		seperator,
	}
}

pub struct Seperated<'a, I> {
	items: &'a [I],
	seperator: &'a str,
}

impl<'a, I> fmt::Display for Seperated<'a, I>
where
	I: fmt::Display,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		for (idx, i) in self.items.iter().enumerate() {
			if idx != 0 {
				f.write_str(self.seperator)?;
			}
			i.fmt(f)?;
		}
		Ok(())
	}
}
