use logos::Logos;

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
pub enum UuidToken {
	#[regex("[0-9a-fA-F]+")]
	Digits,
	#[token("-")]
	Dash,
}
