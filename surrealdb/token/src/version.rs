use logos::Logos;

use crate::Joined;

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
#[logos(extras = Joined)]
pub enum VersionToken {
	#[regex("[0-9]+")]
	Digits,
	#[token(".")]
	Dot,
}
