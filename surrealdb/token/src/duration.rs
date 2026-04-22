use logos::Logos;

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
pub enum DurationToken {
	#[regex("[0-9]+")]
	Digits,
	#[token("y")]
	Year,
	#[token("w")]
	Week,
	#[token("d")]
	Day,
	#[token("h")]
	Hour,
	#[token("m")]
	Minute,
	#[token("s")]
	Second,
	#[token("ms")]
	MiliSecond,
	#[token("us")]
	#[token("µs")]
	MicroSecond,
	#[token("ns")]
	NanoSecond,
}
