use logos::Logos;

use crate::Joined;

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
#[logos(extras = Joined)]
pub enum RegexToken {
	#[regex("[^/]+")]
	Source,
	#[token("\\/")]
	Escape,
	#[token("/")]
	End,
}
