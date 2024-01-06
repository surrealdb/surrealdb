use super::{literal::ident_raw, ParseError};
use crate::sql::constant;
use nom::{
	bytes::complete::{tag, tag_no_case},
	combinator::{opt, peek, value},
	Err, IResult,
};

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum BuiltinName<I> {
	Function(I),
	Constant(constant::Constant),
}

/// A macro to generate a parser which is able to parse all the different functions, returning an
/// error of the function does not exists.
macro_rules! impl_builtins {
	($($name:ident$( ( $s:ident ) )? $(= $rename:expr)? => { $($t:tt)* }),*$(,)?) => {
		fn _parse_builtin_name(i: &str) -> IResult<&str, BuiltinName<&str>, ParseError<&str>> {
			$(
				impl_builtins!{
					@variant,
					impl_builtins!(@rename, $name, $($rename)?),
					$name,
					$($s)?,
					$($rename)?,
					{ $($t)* }
				}
			)*
			$(
				if let (i, Some(x)) = opt($name)(i)?{
					return Ok((i,x))
				}
			)*
			Err(Err::Error(ParseError::Base(i)))
		}
	};

	(@variant, $full:expr, $name:ident, $($s:ident)?,$($rename:expr)?, { fn }) => {
		fn $name(i: &str) -> IResult<&str, BuiltinName<&str>, ParseError<&str>>{
			let parser = tag_no_case(impl_builtins!(@rename,$name,$($rename)?));
			let res = value(BuiltinName::Function($full),parser)(i)?;
			Ok(res)
		}
	};
	(@variant, $full:expr, $name:ident,$($s:ident)?,$($rename:expr)?, { const = $value:expr}) => {
		#[allow(non_snake_case)]
		fn $name(i: &str) -> IResult<&str, BuiltinName<&str>, ParseError<&str>>{
			let parser = tag_no_case(impl_builtins!(@rename,$name,$($rename)?));
			let res = value(BuiltinName::Constant($value),parser)(i)?;
			Ok(res)
		}
	};
	(@variant, $full:expr, $name:ident,$($s:ident)*,$($rename:expr)?, { $($t:tt)* }) => {
		fn $name(i: &str) -> IResult<&str, BuiltinName<&str>, ParseError<&str>>{
			let (i,_) = tag_no_case(impl_builtins!(@rename,$name,$($rename)?))(i)?;
			let (i,_) = impl_builtins!(@sep, i,$full, $($s)*);

			let (i,_) = impl_builtins!{@block,i, $full, { $($t)* }};

			if let Ok((i, Some(_))) = peek(opt(ident_raw))(i){
				Err(Err::Failure(ParseError::InvalidPath{
					tried: i,
					parent: $full
				}))
			}else{
				Err(Err::Failure(ParseError::Expected{
					tried: i,
					expected: "a identifier"
				}))
			}
		}
	};

	(@block, $i:ident, $full:expr, { $($name:ident $(($s:ident))? $(= $rename:expr)? => { $($t:tt)* }),* $(,)? }) => {
		{
			$(
				impl_builtins!{@variant,
					concat!($full,"::",impl_builtins!(@rename, $name, $($rename)?)),
					$name,
					$($s)?,
					$($rename)?,
					{ $($t) * }
				}
			)*

			$(
				if let Ok((i, x)) = $name($i){
					return Ok((i,x))
				}
			)*
			($i,())
		}
	};

	(@sep, $input:expr, $full:expr, func) => {
		match tag::<_,_,ParseError<&str>>("::")($input) {
			Ok(x) => x,
			Err(_) => {
				return Ok(($input, BuiltinName::Function($full)))
			}
		}
	};
	(@sep, $input:expr, $full:expr, cons) => {
		match tag::<_,_,ParseError<&str>>("::")($input) {
			Ok(x) => x,
			Err(_) => {
				return Ok(($input, BuiltinName::Constant($full)))
			}
		}
	};
	(@sep, $input:expr,$full:expr, ) => {{
		match tag::<_,_,ParseError<&str>>("::")($input) {
			Ok(x) => x,
			Err(_) => {
				return Err(Err::Error(ParseError::Expected{
					tried: $input,
					expected: "a path separator `::`"
				}))
			}
		}
	}};

	(@rename, $name:ident, $rename:expr) => {
		$rename
	};

	(@rename, $name:ident,) => {
		stringify!($name)
	};
}

pub(crate) fn builtin_name(i: &str) -> IResult<&str, BuiltinName<&str>, ParseError<&str>> {
	impl_builtins! {
		array => {
			add => { fn },
			all => { fn },
			any => { fn },
			append => { fn },
			at => { fn },
			boolean_and => { fn },
			boolean_not => { fn },
			boolean_or => { fn },
			boolean_xor => { fn },
			clump => { fn },
			combine => { fn },
			complement => { fn },
			concat => { fn },
			difference => { fn },
			distinct => { fn },
			filter_index => { fn },
			find_index => { fn },
			first => { fn },
			flatten => { fn },
			group => { fn },
			insert => { fn },
			intersect=> { fn },
			join => { fn },
			last=> { fn },
			len => { fn },
			logical_and => { fn },
			logical_or => { fn },
			logical_xor => { fn },
			matches => { fn },
			max => { fn },
			min => { fn },
			pop => { fn },
			prepend => { fn },
			push => { fn },
			remove => { fn },
			reverse => { fn },
			slice => { fn },
			// says that sort is also itself a function
			sort(func) => {
				asc => {fn },
				desc => {fn },
			},
			transpose => { fn },
			r#union = "union" => { fn },
		},
		bytes => {
			len => { fn }
		},
		crypto => {
			argon2 => {
				compare => { fn },
				generate => { fn }
			},
			bcrypt => {
				compare => { fn },
				generate => { fn }
			},
			pbkdf2 => {
				compare => { fn },
				generate => { fn }
			},
			scrypt => {
				compare => { fn },
				generate => { fn }
			},
			md5 => { fn },
			sha1 => { fn },
			sha256 => { fn },
			sha512 => { fn }
		},
		duration => {
			days => { fn },
			hours => { fn },
			micros => { fn },
			millis => { fn },
			mins => { fn },
			nanos => { fn },
			secs => { fn },
			weeks => { fn },
			years => { fn },
			from => {
				days => { fn },
				hours => { fn },
				micros => { fn },
				millis => { fn },
				mins => { fn },
				nanos => { fn },
				secs => { fn },
				weeks => { fn },
			},
		},
		encoding => {
			base64 => {
				decode => { fn },
				encode => { fn },
			}
		},
		geo => {
			area => { fn },
			bearing => { fn },
			centroid => { fn },
			distance => { fn },
			hash => {
				decode => { fn },
				encode => { fn },
			},
		},
		http => {
			head => { fn },
			get => { fn },
			put => { fn },
			post => { fn },
			patch => { fn },
			delete => { fn },
		},
		math => {
			abs => { fn },
			bottom => { fn },
			ceil => { fn },
			fixed => { fn },
			floor => { fn },
			interquartile => { fn },
			max => { fn },
			mean => { fn },
			median => { fn },
			midhinge => { fn },
			min => { fn },
			mode => { fn },
			nearestrank => { fn },
			percentile => { fn },
			pow => { fn },
			product => { fn },
			round => { fn },
			spread => { fn },
			sqrt => { fn },
			stddev => { fn },
			sum => { fn },
			top => { fn },
			trimean => { fn },
			variance => { fn },
			E => { const = constant::Constant::MathE },
			FRAC_1_PI => { const = constant::Constant::MathFrac1Pi },
			FRAC_1_SQRT_2 => { const = constant::Constant::MathFrac1Sqrt2 },
			FRAC_2_PI => { const = constant::Constant::MathFrac2Pi },
			FRAC_2_SQRT_PI => { const = constant::Constant::MathFrac2SqrtPi },
			FRAC_PI_2 => { const = constant::Constant::MathFracPi2 },
			FRAC_PI_3 => { const = constant::Constant::MathFracPi3 },
			FRAC_PI_4 => { const = constant::Constant::MathFracPi4 },
			FRAC_PI_6 => { const = constant::Constant::MathFracPi6 },
			FRAC_PI_8 => { const = constant::Constant::MathFracPi8 },
			INF => { const = constant::Constant::MathInf },
			LN_10 => { const = constant::Constant::MathLn10 },
			LN_2 => { const = constant::Constant::MathLn2 },
			LOG10_2 => { const = constant::Constant::MathLog102 },
			LOG10_E => { const = constant::Constant::MathLog10E },
			LOG2_10 => { const = constant::Constant::MathLog210 },
			LOG2_E => { const = constant::Constant::MathLog2E },
			PI => { const = constant::Constant::MathPi },
			SQRT_2 => { const = constant::Constant::MathSqrt2 },
			TAU => { const = constant::Constant::MathTau },
		},
		meta => {
			id => { fn },
			table => { fn },
			tb => { fn },
		},
		object => {
			entries => { fn },
			from_entries => { fn },
			keys => { fn },
			len => { fn },
			values => { fn },
		},
		parse => {
			email => {
				host => { fn },
				user => { fn },
			},
			url => {
				domain => { fn },
				fragment => { fn },
				host => { fn },
				path => { fn },
				port => { fn },
				query => { fn },
				scheme => { fn },
			}
		},
		rand(func) => {
			r#bool = "bool" => { fn },
			r#enum = "enum" => { fn },
			float => { fn },
			guid => { fn },
			int => { fn },
			string => { fn },
			time => { fn },
			ulid => { fn },
			uuid(func) => {
				v4 => { fn },
				v7 => { fn },
			},
		},
		search => {
			analyze => { fn },
			score => { fn },
			highlight => { fn },
			offsets => { fn },
		},
		session => {
			db => { fn },
			id => { fn },
			ip => { fn },
			ns => { fn },
			origin => { fn },
			sc => { fn },
			sd => { fn },
			token => { fn },
		},
		string => {
			concat => { fn },
			contains => { fn },
			ends_with = "endsWith" => { fn },
			join => { fn },
			len => { fn },
			lowercase => { fn },
			matches => {fn},
			repeat => { fn },
			replace => { fn },
			reverse => { fn },
			slice => { fn },
			slug => { fn },
			split => { fn },
			starts_with = "startsWith" => { fn },
			trim => { fn },
			uppercase => { fn },
			words => { fn },
			distance => {
				hamming => { fn },
				levenshtein => { fn },
			},
			similarity => {
				fuzzy => { fn },
				jaro => { fn },
				smithwaterman => { fn },
			},
			is => {
				alphanum => { fn },
				alpha => { fn },
				ascii => { fn },
				datetime => { fn },
				domain => { fn },
				email => { fn },
				hexadecimal => { fn },
				latitude => { fn },
				longitude => { fn },
				numeric => { fn },
				semver => { fn },
				url => { fn },
				uuid => { fn },
			},
			semver => {
				compare => { fn },
				major => { fn },
				minor => { fn },
				patch => { fn },
				inc => {
					major => { fn },
					minor => { fn },
					patch => { fn },
				},
				set => {
					major => { fn },
					minor => { fn },
					patch => { fn },
				}
			}
		},
		time => {
			ceil => { fn },
			day => { fn },
			floor => { fn },
			format => { fn },
			group => { fn },
			hour => { fn },
			minute => { fn },
			max => { fn },
			min => { fn },
			month => { fn },
			nano => { fn },
			micros => { fn },
			millis => { fn },
			now => { fn },
			round => { fn },
			second => { fn },
			timezone => { fn },
			unix => { fn },
			wday => { fn },
			week => { fn },
			yday => { fn },
			year => { fn },
			from => {
				nanos => {fn},
				micros => {fn},
				millis => {fn},
				unix => {fn},
				secs => {fn},
			}
		},
		r#type = "type" => {
			r#bool = "bool" => { fn },
			datetime => { fn },
			decimal => { fn },
			duration => { fn },
			fields => { fn },
			field => { fn },
			float => { fn },
			int => { fn },
			number => { fn },
			point => { fn },
			string => { fn },
			table => { fn },
			thing => { fn },
			is => {
				array => { fn },
				r#bool = "bool" => { fn },
				bytes => { fn },
				collection => { fn },
				datetime => { fn },
				decimal => { fn },
				duration => { fn },
				float => { fn },
				geometry => { fn },
				int => { fn },
				line => { fn },
				none => { fn },
				null => { fn },
				multiline => { fn },
				multipoint => { fn },
				multipolygon => { fn },
				number => { fn },
				object => { fn },
				point => { fn },
				polygon => { fn },
				record => { fn },
				string => { fn },
				uuid => { fn },
			}
		},
		vector => {
			add => { fn },
			angle => { fn },
			divide => { fn },
			cross => { fn },
			dot => { fn },
			magnitude => { fn },
			multiply => { fn },
			normalize => { fn },
			project => { fn },
			subtract => { fn },
			distance => {
				chebyshev => { fn },
				euclidean => { fn },
				hamming => { fn },
				mahalanobis => { fn },
				manhattan => { fn },
				minkowski => { fn },
			},
			similarity => {
				cosine => {fn },
				jaccard => {fn },
				pearson => {fn },
				spearman => {fn },
			}
		},
		count => { fn },
		not => { fn },
		sleep => { fn },
	}
	_parse_builtin_name(i)
}

#[cfg(test)]
mod tests {
	use crate::sql::constant::Constant;

	use super::*;

	#[test]
	fn constant_lowercase() {
		let sql = "math::pi";
		let res = builtin_name(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, BuiltinName::Constant(Constant::MathPi));
	}

	#[test]
	fn constant_uppercase() {
		let sql = "MATH::PI";
		let res = builtin_name(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, BuiltinName::Constant(Constant::MathPi));
	}

	#[test]
	fn constant_mixedcase() {
		let sql = "math::PI";
		let res = builtin_name(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, BuiltinName::Constant(Constant::MathPi));
	}
}
