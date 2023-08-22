use nom::{error::ParseError, Err, IResult, InputLength, Parser};

pub fn delimited_list<I, E, D, S, P, T, O, O1, O2, O3>(
	mut delimitor: D,
	mut seperator: S,
	mut parser: P,
	mut terminator: T,
) -> impl FnMut(I) -> IResult<I, Vec<O>, E>
where
	I: Clone + InputLength,
	P: Parser<I, O, E>,
	D: Parser<I, O1, E>,
	S: Parser<I, O2, E>,
	T: Parser<I, O3, E>,
	E: ParseError<I>,
{
	move |i| {
		let (i, _) = delimitor.parse(i)?;
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
			let (i, value) = parser.parse(input).map_err(|e| match e {
				Err::Error(e) => Err::Failure(e),
				e => e,
			})?;
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
