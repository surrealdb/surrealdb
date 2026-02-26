use logos::Logos;

use crate::Joined;

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
#[logos(extras = Joined)]
pub enum RegexToken {
	#[regex("(?m)[^/]+")]
	Source,
	#[token("\\/")]
	Escape,
	#[token("/")]
	End,
}
