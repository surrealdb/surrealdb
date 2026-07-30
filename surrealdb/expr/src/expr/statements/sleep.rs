use crate::val::Duration;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct SleepStatement {
	pub duration: Duration,
}
