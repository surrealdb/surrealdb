use logos::Logos;

use crate::Joined;

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
#[logos(extras = Joined)]
pub enum JsFunctionToken {
	#[token("{", priority = 10)]
	BraceOpen,
	#[token(r"}", priority = 10)]
	BraceClose,
	#[token(r"[", priority = 10)]
	BracketOpen,
	#[token(r"]", priority = 10)]
	BracketClose,
	#[token(r"(", priority = 10)]
	ParenOpen,
	#[token(r")", priority = 10)]
	ParenClose,
	#[token("`", priority = 10)]
	TemplateOpen,
	#[regex(r"(?s).")]
	#[regex(r"(?s)\\.", priority = 11)]
	#[regex(r#""([^\\"]|\\.)*""#)]
	#[regex(r#"'([^\\']|\\.)*'"#)]
	#[regex(r#"/*([^*]|\*[^\\])*\*/'"#)]
	#[regex(r#"//[^\n\u2028\u2029]*"#)]
	Characters,
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
		assert_eq!(
			tokens,
			&[
				JsFunctionToken::Characters,
				JsFunctionToken::Characters,
				JsFunctionToken::Characters,
				JsFunctionToken::BraceClose
			]
		);
	}
}
