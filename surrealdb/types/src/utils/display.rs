use crate::{SqlFormat, ToSql};

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

impl<'a, I> ToSql for Seperated<'a, I>
where
	I: ToSql,
{
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		for (idx, i) in self.items.iter().enumerate() {
			if idx != 0 {
				f.push_str(self.seperator);
			}
			i.fmt_sql(f, fmt);
		}
	}
}
