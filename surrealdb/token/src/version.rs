use logos::Logos;

use crate::Joined;

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
#[logos(extras = Joined)]
pub enum VersionTokenKind {
	#[regex("[0-9]")]
	Digit,
	#[token(".")]
	Dot,
}
