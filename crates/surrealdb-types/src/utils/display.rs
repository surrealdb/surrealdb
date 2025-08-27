use std::fmt::Display;

pub fn join_displayable<T: Display>(slice: &[T], separator: &str) -> String {
	slice.iter().map(|x| x.to_string()).collect::<Vec<String>>().join(separator)
}
