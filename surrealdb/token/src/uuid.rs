use logos::Logos;

use crate::Joined;

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
#[logos(extras = Joined)]
pub enum UuidToken {
	#[regex("[0-9a-fA-F]+")]
	Digits,
	#[token("-")]
	Dash,
}
