use logos::Logos;

use crate::Joined;

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
#[logos(extras = Joined)]
#[logos(subpattern source = r#"[^\]\[\{\}\(\)"'`]|"([^"]|\\")*"|'([^']|\\')*'|\\\{|\\\}|\\\(|\\\)|\\\[|\\\]"#)]
pub enum JsFunctionToken {
	#[regex(r"(?&source)*\{")]
	BraceOpen,
	#[regex(r"(?&source)*\}")]
	BraceClose,
	#[regex(r"(?&source)*\[")]
	BracketOpen,
	#[regex(r"(?&source)*\]")]
	BracketClose,
	#[regex(r"(?&source)*\(")]
	ParenOpen,
	#[regex(r"(?&source)*\)")]
	ParenClose,
	#[regex(r"(?&source)*\`")]
	TemplateOpen,
}

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
#[logos(extras = Joined)]
pub enum JsFunctionTemplateToken {
	#[regex(r"[^`\$]*`")]
	End,
	#[regex(r"[^`\$]*\$")]
	Dollar,
	#[regex(r"[^`\$]*\$\{")]
	TemplateOpen,
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn test() {
		let tokens = JsFunctionToken::lexer("a\nb}").map(|x| x.unwrap()).collect::<Vec<_>>();
		assert_eq!(tokens, &[JsFunctionToken::BraceClose]);
	}
}
