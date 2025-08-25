use std::fmt::Display;

pub trait JoinDisplayable<T> {
	fn join_displayable(self, separator: &str) -> String;
}

impl<T> JoinDisplayable<T> for &Vec<T>
where
	T: Display,
{
	fn join_displayable(self, separator: &str) -> String {
		self.iter().map(|x| x.to_string()).collect::<Vec<String>>().join(separator)
	}
}
