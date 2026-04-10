use logos::Logos;

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
pub enum DateTimeToken {
	#[regex("[0-9]+")]
	Digits,
	#[token(".")]
	Dot,
	#[token("T")]
	#[token("t")]
	#[token(" ")]
	T,
	#[token("Z")]
	#[token("z")]
	Z,
	#[token("+")]
	Plus,
	#[token("-")]
	Dash,
	#[token(":")]
	Colon,
}
