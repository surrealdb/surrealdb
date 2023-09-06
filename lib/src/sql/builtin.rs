use crate::sql::{error::ParseError, ident::ident_raw};
use nom::{
	bytes::complete::tag,
	combinator::{opt, peek, value},
	Err, IResult,
};

#[derive(Clone, Copy)]
pub enum BuiltinName<I> {
	Function(I),
	Constant(I),
}

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
				if let Ok((i, x)) = $name(i){
					return Ok((i,x))
				}
			)*
			Err(Err::Error(ParseError::Base(i)))
		}
	};

	(@variant, $full:expr, $name:ident, $($s:ident)?,$($rename:expr)?, { fn }) => {
		fn $name<'a>(i: &'a str) -> IResult<&'a str, BuiltinName<&'a str>, ParseError<&'a str>>{
			let parser = tag(impl_builtins!(@rename,$name,$($rename)?));
			let res = value(BuiltinName::Function($full),parser)(i)?;
			Ok(res)
		}
	};
	(@variant, $full:expr, $name:ident,$($s:ident)?,$($rename:expr)?, { const }) => {
		fn $name<'a>(i: &'a str) -> IResult<&'a str, BuiltinName<&'a str>, ParseError<&'a str>>{
			let parser = tag(impl_builtins!(@rename,$name,$($rename)?));
			let res = value(BuiltinName::Constant($full),parser)(i)?;
			Ok(res)
		}
	};
	(@variant, $full:expr, $name:ident,$($s:ident)*,$($rename:expr)?, { $($t:tt)* }) => {
		fn $name<'a>(i: &'a str) -> IResult<&'a str, BuiltinName<&'a str>, ParseError<&'a str>>{
			let (i,_) = tag(impl_builtins!(@rename,$name,$($rename)?))(i)?;
			let (i,_) = impl_builtins!(@sep, i,$full, $($s)*);

			let (i,_) = impl_builtins!{@block,i, $full, { $($t)* }};

			if let Ok((i, Some(_))) = peek(opt(ident_raw))(i){
				Err(Err::Error(ParseError::InvalidPath{
					tried: i,
					parent: $full
				}))
			}else{
				Err(Err::Error(ParseError::Expected{
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
					expected: "a path seperator `::`"
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

pub(crate) fn parse_builtin_name(i: &str) -> IResult<&str, BuiltinName<&str>, ParseError<&str>> {
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
			// says that sort is also itself a function
			sort(func) => {
				asc => {fn },
				desc => {fn },
			},
			transpose => { fn },
			union => { fn },
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
		},
		meta => {
			id => { fn },
			table => { fn },
			tb => { fn },
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
		rand => {
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
				micros => {fn},
				millies => {fn},
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
		}
	}
	_parse_builtin_name(i)
}
