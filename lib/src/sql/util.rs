use nom::{error::ParseError, Err, IResult, InputLength, Parser};

/// Parses a delimited list with an option trailing seperator in the form of:
///
///```text
/// PREFIX $(PARSER)SEPERATOR* $(SEPERATOR)? TERMINATOR
///```
///
/// Which parsers productions like
/// (a,b,c,) or [a,b]
///
/// First parses the prefix and returns it's error if there is one.
/// The tries to parse the terminator. If there is one the parser completes else it tries to parse
/// the value, else it returns the parsed values.
/// Then it tries to parse the seperator, if there is one it start again trying to parse the
/// terminator followed by a value if there is no terminator. Else it tries to parse the terminator
/// and if there is none it returns a failure. Otherwise completes with an vec of the parsed
/// values.
///
pub fn delimited_list0<I, E, D, S, V, T, O, O1, O2, O3>(
	mut prefix: D,
	mut seperator: S,
	mut value: V,
	mut terminator: T,
) -> impl FnMut(I) -> IResult<I, Vec<O>, E>
where
	I: Clone + InputLength,
	V: Parser<I, O, E>,
	D: Parser<I, O1, E>,
	S: Parser<I, O2, E>,
	T: Parser<I, O3, E>,
	E: ParseError<I>,
{
	move |i| {
		let (i, _) = prefix.parse(i)?;
		let mut res = Vec::new();
		let mut input = i;
		loop {
			match terminator.parse(input.clone()) {
				Err(Err::Error(_)) => {}
				Err(e) => return Err(e),
				Ok((i, _)) => {
					input = i;
					break;
				}
			}
			let (i, value) = value.parse(input)?;
			res.push(value);
			match seperator.parse(i.clone()) {
				Ok((i, _)) => {
					input = i;
				}
				Err(Err::Error(_)) => match terminator.parse(i) {
					Ok((i, _)) => {
						input = i;
						break;
					}
					Result::Err(Err::Error(e)) => return Err(Err::Failure(e)),
					Result::Err(e) => return Err(e),
				},
				Err(e) => return Err(e),
			}
		}
		Ok((input, res))
	}
}

/// Parses a delimited list with an option trailing seperator in the form of:
///
///```text
/// PREFIX $(PARSER)SEPERATOR+ $(SEPERATOR)? TERMINATOR
///```
///
/// Which parsers productions like
/// (a,b,c,) or [a,b] but not empty lists
///
/// First parses the prefix and returns it's error if there is one.
/// The tries to parse the terminator. If there is one the parser completes else it tries to parse
/// the value, else it returns the parsed values.
/// Then it tries to parse the seperator, if there is one it start again trying to parse the
/// terminator followed by a value if there is no terminator. Else it tries to parse the terminator
/// and if there is none it returns a failure. Otherwise completes with an vec of the parsed
/// values.
///
pub fn delimited_list1<I, E, D, S, V, T, O, O1, O2, O3>(
	mut prefix: D,
	mut seperator: S,
	mut value: V,
	mut terminator: T,
) -> impl FnMut(I) -> IResult<I, Vec<O>, E>
where
	I: Clone + InputLength,
	V: Parser<I, O, E>,
	D: Parser<I, O1, E>,
	S: Parser<I, O2, E>,
	T: Parser<I, O3, E>,
	E: ParseError<I>,
{
	move |i| {
		let (i, _) = prefix.parse(i)?;
		let mut res = Vec::new();
		let mut input = i;
		loop {
			match terminator.parse(input.clone()) {
				Err(Err::Error(_)) => {}
				Err(e) => return Err(e),
				Ok((i, _)) => {
					input = i;
					break;
				}
			}
			let (i, value) = value.parse(input)?;
			res.push(value);
			match seperator.parse(i.clone()) {
				Ok((i, _)) => {
					input = i;
				}
				Err(Err::Error(_)) => match terminator.parse(i) {
					Ok((i, _)) => {
						input = i;
						break;
					}
					Result::Err(Err::Error(e)) => return Err(Err::Failure(e)),
					Result::Err(e) => return Err(e),
				},
				Err(e) => return Err(e),
			}
		}
		Ok((input, res))
	}
}
