use crate::sql::filter::Filter as SqlFilter;
use crate::sql::language::Language;
use deunicode::deunicode;
use rust_stemmers::{Algorithm, Stemmer};

pub(super) enum Filter {
	Stemmer(Stemmer),
	Ascii,
	EdgeNgram(u16, u16),
	Lowercase,
	Uppercase,
}

impl From<SqlFilter> for Filter {
	fn from(f: SqlFilter) -> Self {
		match f {
			SqlFilter::Ascii => Filter::Ascii,
			SqlFilter::EdgeNgram(min, max) => Filter::EdgeNgram(min, max),
			SqlFilter::Lowercase => Filter::Lowercase,
			SqlFilter::Snowball(l) => {
				let a = match l {
					Language::Arabic => Stemmer::create(Algorithm::Arabic),
					Language::Danish => Stemmer::create(Algorithm::Danish),
					Language::Dutch => Stemmer::create(Algorithm::Dutch),
					Language::English => Stemmer::create(Algorithm::English),
					Language::French => Stemmer::create(Algorithm::French),
					Language::German => Stemmer::create(Algorithm::German),
					Language::Greek => Stemmer::create(Algorithm::Greek),
					Language::Hungarian => Stemmer::create(Algorithm::Hungarian),
					Language::Italian => Stemmer::create(Algorithm::Italian),
					Language::Norwegian => Stemmer::create(Algorithm::Norwegian),
					Language::Portuguese => Stemmer::create(Algorithm::Portuguese),
					Language::Romanian => Stemmer::create(Algorithm::Romanian),
					Language::Russian => Stemmer::create(Algorithm::Russian),
					Language::Spanish => Stemmer::create(Algorithm::Spanish),
					Language::Swedish => Stemmer::create(Algorithm::Swedish),
					Language::Tamil => Stemmer::create(Algorithm::Tamil),
					Language::Turkish => Stemmer::create(Algorithm::Turkish),
				};
				Filter::Stemmer(a)
			}
			SqlFilter::Uppercase => Filter::Uppercase,
		}
	}
}

impl Filter {
	pub(super) fn from(f: Option<Vec<SqlFilter>>) -> Option<Vec<Filter>> {
		if let Some(f) = f {
			let mut r = Vec::with_capacity(f.len());
			for f in f {
				r.push(f.into());
			}
			Some(r)
		} else {
			None
		}
	}

	pub(super) fn filter(&self, c: &str) -> String {
		match self {
			Filter::EdgeNgram(_, _) => {
				todo!()
			}
			Filter::Lowercase => c.to_lowercase(),
			Filter::Stemmer(s) => s.stem(&c.to_lowercase()).into(),
			Filter::Ascii => deunicode(c),
			Filter::Uppercase => c.to_uppercase(),
		}
	}
}
