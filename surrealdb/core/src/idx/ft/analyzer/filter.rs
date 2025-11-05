use anyhow::Result;
use deunicode::deunicode;
use rust_stemmers::{Algorithm, Stemmer};

use crate::expr::filter::Filter as SqlFilter;
use crate::expr::language::Language;
use crate::idx::ft::Position;
use crate::idx::ft::analyzer::mapper::Mapper;
use crate::idx::ft::analyzer::tokenizer::Tokens;
use crate::idx::trees::store::IndexStores;

#[derive(Clone, Copy)]
pub(in crate::idx::ft) enum FilteringStage {
	Indexing,
	Querying,
}
pub(super) enum Filter {
	Stemmer(Stemmer),
	Ascii,
	Ngram(u16, u16),
	EdgeNgram(u16, u16),
	Lowercase,
	Uppercase,
	Mapper(Mapper),
}

impl Filter {
	fn new(ixs: &IndexStores, f: &SqlFilter) -> Result<Self> {
		let f = match f {
			SqlFilter::Ascii => Filter::Ascii,
			SqlFilter::EdgeNgram(min, max) => Filter::EdgeNgram(*min, *max),
			SqlFilter::Lowercase => Filter::Lowercase,
			SqlFilter::Ngram(min, max) => Filter::Ngram(*min, *max),
			SqlFilter::Snowball(l) => {
				let a = match l {
					Language::Arabic => Stemmer::create(Algorithm::Arabic),
					Language::Danish => Stemmer::create(Algorithm::Danish),
					Language::Dutch => Stemmer::create(Algorithm::Dutch),
					Language::English => Stemmer::create(Algorithm::English),
					Language::Finnish => Stemmer::create(Algorithm::Finnish),
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
			SqlFilter::Mapper(path) => Filter::Mapper(ixs.mappers().get(path)?),
		};
		Ok(f)
	}

	pub(super) fn try_from(
		ixs: &IndexStores,
		fs: &Option<Vec<SqlFilter>>,
	) -> Result<Option<Vec<Filter>>> {
		if let Some(fs) = fs {
			let mut r = Vec::with_capacity(fs.len());
			for f in fs {
				r.push(Self::new(ixs, f)?);
			}
			Ok(Some(r))
		} else {
			Ok(None)
		}
	}

	fn is_stage(&self, stage: FilteringStage) -> bool {
		if let FilteringStage::Querying = stage {
			!matches!(self, Filter::EdgeNgram(_, _) | Filter::Ngram(_, _))
		} else {
			true
		}
	}

	pub(super) fn apply_filters(
		mut t: Tokens,
		f: &Option<Vec<Filter>>,
		stage: FilteringStage,
	) -> Result<Tokens> {
		if let Some(filters) = f {
			for filter in filters {
				if filter.is_stage(stage) {
					t = t.filter(filter)?;
				}
			}
		}
		Ok(t)
	}

	pub(super) fn apply_filter(&self, c: &str) -> FilterResult {
		match self {
			Filter::Ascii => Self::deunicode(c),
			Filter::EdgeNgram(min, max) => Self::edgengram(c, *min, *max),
			Filter::Lowercase => Self::lowercase(c),
			Filter::Ngram(min, max) => Self::ngram(c, *min, *max),
			Filter::Stemmer(s) => Self::stem(s, c),
			Filter::Uppercase => Self::uppercase(c),
			Filter::Mapper(m) => m.map(c),
		}
	}

	#[inline]
	fn check_term(c: &str, s: String) -> FilterResult {
		if s.is_empty() {
			FilterResult::Ignore
		} else if s.eq(c) {
			FilterResult::Term(Term::Unchanged)
		} else {
			FilterResult::Term(Term::NewTerm(s, 0))
		}
	}

	#[inline]
	fn lowercase(c: &str) -> FilterResult {
		Self::check_term(c, c.to_lowercase())
	}

	#[inline]
	fn uppercase(c: &str) -> FilterResult {
		Self::check_term(c, c.to_uppercase())
	}

	#[inline]
	fn deunicode(c: &str) -> FilterResult {
		Self::check_term(c, deunicode(c))
	}

	#[inline]
	fn stem(s: &Stemmer, c: &str) -> FilterResult {
		Self::check_term(c, s.stem(&c.to_lowercase()).into())
	}

	#[inline]
	fn ngram(c: &str, min: u16, max: u16) -> FilterResult {
		let min = min as usize;
		let c: Vec<char> = c.chars().collect();
		let l = c.len();
		if l < min {
			return FilterResult::Ignore;
		}
		let mut ng = vec![];
		let r1 = 0..=(l - min);
		let max = max as usize;
		for s in r1 {
			let e = (s + max).min(l);
			let r2 = (s + min)..=e;
			for p in r2 {
				let n = &c[s..p];
				if c.eq(n) {
					ng.push(Term::Unchanged);
				} else {
					ng.push(Term::NewTerm(n.iter().collect(), s as Position));
				}
			}
		}
		FilterResult::Terms(ng)
	}

	#[inline]
	fn edgengram(c: &str, min: u16, max: u16) -> FilterResult {
		let min = min as usize;
		let c: Vec<char> = c.chars().collect();
		let l = c.len();
		if l < min {
			return FilterResult::Ignore;
		}
		let max = (max as usize).min(l);
		let ng = (min..=max)
			.map(|p| {
				let n = &c[0..p];
				if c.eq(n) {
					Term::Unchanged
				} else {
					Term::NewTerm(n.iter().collect(), 0)
				}
			})
			.collect();
		FilterResult::Terms(ng)
	}
}

pub(super) enum FilterResult {
	Ignore,
	Term(Term),
	Terms(Vec<Term>),
}

pub(super) enum Term {
	Unchanged,
	NewTerm(String, Position),
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::analyzer::tests::{test_analyzer, test_analyzer_tokens};
	use crate::idx::ft::analyzer::tokenizer::Token;

	#[tokio::test]
	async fn test_arabic_stemmer() {
		let input = "الكلاب تحب الجري في الحديقة، لكن كلبي الصغير يفضل النوم في سريره بدلاً من الجري";
		let output = vec![
			"كلاب", "تحب", "الجر", "في", "حديق", "لكن", "كلب", "صغير", "يفضل", "نوم", "في", "سرير",
			"بدل", "من", "الجر",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(arabic);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(ar);", input, &output)
			.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(ara);",
			input,
			&output,
		)
		.await;
	}

	#[tokio::test]
	async fn test_danish_stemmer() {
		let input = "Hunde elsker at løbe i parken, men min lille hund foretrækker at sove i sin kurv frem for at løbe.";
		let output = vec![
			"hund",
			"elsk",
			"at",
			"løb",
			"i",
			"park",
			",",
			"men",
			"min",
			"lil",
			"hund",
			"foretræk",
			"at",
			"sov",
			"i",
			"sin",
			"kurv",
			"frem",
			"for",
			"at",
			"løb",
			".",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(danish);",
			input,
			&output,
		)
		.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(dan);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(da);", input, &output)
			.await;
	}

	#[tokio::test]
	async fn test_dutch_stemmer() {
		let input = "Honden houden ervan om in het park te rennen, maar mijn kleine hond slaapt liever in zijn mand dan te rennen.";
		let output = vec![
			"hond", "houd", "ervan", "om", "in", "het", "park", "te", "renn", ",", "mar", "mijn",
			"klein", "hond", "slaapt", "liever", "in", "zijn", "mand", "dan", "te", "renn", ".",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(dutch);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(nl);", input, &output)
			.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(nld);",
			input,
			&output,
		)
		.await;
	}

	#[tokio::test]
	async fn test_english_stemmer() {
		let input = "Teachers are often teaching, but my favorite teacher prefers reading in her spare time rather than teaching.";
		let output = vec![
			"teacher", "are", "often", "teach", ",", "but", "my", "favorit", "teacher", "prefer",
			"read", "in", "her", "spare", "time", "rather", "than", "teach", ".",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(english);",
			input,
			&output,
		)
		.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(eng);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(en);", input, &output)
			.await;
	}

	#[tokio::test]
	async fn test_finnish_stemmer() {
		let input = "työ tekijäänsä kiittää";
		let output = ["työ", "tekij", "kiit"];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(finnish);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(fi);", input, &output)
			.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(fin);",
			input,
			&output,
		)
		.await;
	}

	#[tokio::test]
	async fn test_french_stemmer() {
		let input = "Les chiens adorent courir dans le parc, mais mon petit chien aime plutôt se blottir sur le canapé que de courir";
		let output = [
			"le", "chien", "adorent", "cour", "dan", "le", "parc", ",", "mais", "mon", "pet",
			"chien", "aim", "plutôt", "se", "blott", "sur", "le", "canap", "que", "de", "cour",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(french);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(fr);", input, &output)
			.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(fra);",
			input,
			&output,
		)
		.await;
	}

	#[tokio::test]
	async fn test_german_stemmer() {
		let input = "Hunde lieben es, im Park zu laufen, aber mein kleiner Hund zieht es vor, auf dem Sofa zu schlafen, statt zu laufen.";
		let output = [
			"hund", "lieb", "es", ",", "im", "park", "zu", "lauf", ",", "aber", "mein", "klein",
			"hund", "zieht", "es", "vor", ",", "auf", "dem", "sofa", "zu", "schlaf", ",", "statt",
			"zu", "lauf", ".",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(german);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(de);", input, &output)
			.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(deu);",
			input,
			&output,
		)
		.await;
	}

	#[tokio::test]
	async fn test_greek_stemmer() {
		let input = "Τα σκυλιά αγαπούν να τρέχουν στο πάρκο, αλλά ο μικρός μου σκύλος προτιμά να κοιμάται στο κρεβάτι του αντί να τρέχει.";
		let output = [
			"τα",
			"σκυλ",
			"αγαπ",
			"να",
			"τρεχ",
			"στ",
			"παρκ",
			",",
			"αλλ",
			"ο",
			"μικρ",
			"μ",
			"σκυλ",
			"προτιμ",
			"να",
			"κοιμ",
			"στ",
			"κρεβατ",
			"τ",
			"αντ",
			"να",
			"τρεχ",
			".",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(greek);",
			input,
			&output,
		)
		.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(ell);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(el);", input, &output)
			.await;
	}

	#[tokio::test]
	async fn test_hungarian_stemmer() {
		let input = "A kutyák szeretnek futni a parkban, de az én kicsi kutyám inkább alszik a kosarában, mintsem fut.";
		let output = [
			"a", "kutya", "szeret", "futn", "a", "par", ",", "de", "az", "én", "kics", "kutya",
			"inkább", "alsz", "a", "kosar", ",", "mints", "fu", ".",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(hungarian);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(hu);", input, &output)
			.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(hun);",
			input,
			&output,
		)
		.await;
	}

	#[tokio::test]
	async fn test_italian_stemmer() {
		let input = "I cani amano correre nel parco, ma il mio piccolo cane preferisce dormire nel suo cesto piuttosto che correre.";
		let output = [
			"i", "can", "aman", "corr", "nel", "parc", ",", "ma", "il", "mio", "piccol", "can",
			"prefer", "dorm", "nel", "suo", "cest", "piuttost", "che", "corr", ".",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(italian);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(it);", input, &output)
			.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(ita);",
			input,
			&output,
		)
		.await;
	}

	#[tokio::test]
	async fn test_norwegian_stemmer() {
		let input = "Hunder elsker å løpe i parken, men min lille hund foretrekker å sove i sengen sin heller enn å løpe.";
		let output = [
			"hund",
			"elsk",
			"å",
			"løp",
			"i",
			"park",
			",",
			"men",
			"min",
			"lill",
			"hund",
			"foretrekk",
			"å",
			"sov",
			"i",
			"seng",
			"sin",
			"hell",
			"enn",
			"å",
			"løp",
			".",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(norwegian);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(no);", input, &output)
			.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(nor);",
			input,
			&output,
		)
		.await;
	}

	#[tokio::test]
	async fn test_portuguese_stemmer() {
		let input = "Os cães adoram correr no parque, mas o meu pequeno cão prefere dormir na sua cama em vez de correr.";
		let output = [
			"os", "cã", "ador", "corr", "no", "parqu", ",", "mas", "o", "meu", "pequen", "cã",
			"prefer", "dorm", "na", "sua", "cam", "em", "vez", "de", "corr", ".",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(portuguese);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(pt);", input, &output)
			.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(por);",
			input,
			&output,
		)
		.await;
	}

	#[tokio::test]
	async fn test_romanian_stemmer() {
		let input = "Câinii adoră să alerge în parc, dar cățelul meu preferă să doarmă în coșul lui decât să alerge.";
		let output = [
			"câin", "ador", "să", "alerg", "în", "parc", ",", "dar", "cățel", "meu", "prefer",
			"să", "doarm", "în", "coș", "lui", "decât", "să", "alerg", ".",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(romanian);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(ro);", input, &output)
			.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(ron);",
			input,
			&output,
		)
		.await;
	}

	#[tokio::test]
	async fn test_russian_stemmer() {
		let input = "Собаки любят бегать в парке, но моя маленькая собака предпочитает спать в своей корзине, а не бегать.";
		let output = [
			"собак",
			"люб",
			"бега",
			"в",
			"парк",
			",",
			"но",
			"мо",
			"маленьк",
			"собак",
			"предпочита",
			"спат",
			"в",
			"сво",
			"корзин",
			",",
			"а",
			"не",
			"бега",
			".",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(russian);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(ru);", input, &output)
			.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(rus);",
			input,
			&output,
		)
		.await;
	}

	#[tokio::test]
	async fn test_spanish_stemmer() {
		let input = "Los perros aman correr en el parque, pero mi pequeño perro prefiere dormir en su cama en lugar de correr.";
		let output = [
			"los", "perr", "aman", "corr", "en", "el", "parqu", ",", "per", "mi", "pequeñ", "perr",
			"prefier", "dorm", "en", "su", "cam", "en", "lug", "de", "corr", ".",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(spanish);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(es);", input, &output)
			.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(spa);",
			input,
			&output,
		)
		.await;
	}

	#[tokio::test]
	async fn test_swedish_stemmer() {
		let input = "Hundar älskar att springa i parken, men min lilla hund föredrar att sova i sin säng istället för att springa.";
		let output = [
			"hund",
			"älsk",
			"att",
			"spring",
			"i",
			"park",
			",",
			"men",
			"min",
			"lill",
			"hund",
			"föredr",
			"att",
			"sov",
			"i",
			"sin",
			"säng",
			"istället",
			"för",
			"att",
			"spring",
			".",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(swedish);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(sv);", input, &output)
			.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(swe);",
			input,
			&output,
		)
		.await;
	}

	#[tokio::test]
	async fn test_tamil_stemmer() {
		let input = "நாய்கள் பூங்காவில் ஓடுவதை விரும்புகின்றன, ஆனால் என் சிறிய நாய் அதன் படுகையில் தூங்குவதை விரும்புகின்றது, ஓட இல்லை.";
		let output = [
			"ந\u{bbe}ய",
			"கள",
			"பூங",
			"க\u{bbe}வில",
			"ஓடுவதை",
			"விரும",
			"புகி",
			"றன",
			",",
			"ஆன\u{bbe}ல",
			"என",
			"சிறி",
			"ந\u{bbe}ய",
			"அதன",
			"படுகையில",
			"தூங",
			"குவதை",
			"விரும",
			"புகி",
			"றது",
			",",
			"ஓட",
			"இல",
			"லை",
			".",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(tamil);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(ta);", input, &output)
			.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(tam);",
			input,
			&output,
		)
		.await;
	}

	#[tokio::test]
	async fn test_turkish_stemmer() {
		let input = "Köpekler parkta koşmayı sever, ama benim küçük köpeğim koşmaktansa yatağında uyumayı tercih eder.";
		let output = [
			"köpek", "park", "koşma", "sever", ",", "am", "be", "küçük", "köpek", "koşmak",
			"yatak", "uyuma", "tercih", "eder", ".",
		];
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(turkish);",
			input,
			&output,
		)
		.await;
		test_analyzer("ANALYZER test TOKENIZERS blank,class FILTERS snowball(tr);", input, &output)
			.await;
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS snowball(tur);",
			input,
			&output,
		)
		.await;
	}

	#[tokio::test]
	async fn test_ngram() {
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS lowercase,ngram(2,3);",
			"Ālea iacta est",
			&[
				"āl", "āle", "le", "lea", "ea", "ia", "iac", "ac", "act", "ct", "cta", "ta", "es",
				"est", "st",
			],
		)
		.await;
	}

	#[tokio::test]
	async fn test_ngram_tokens() {
		test_analyzer_tokens(
			"ANALYZER test TOKENIZERS blank,class FILTERS lowercase,ngram(2,3);",
			"Ālea iacta",
			&vec![
				Token::String {
					chars: (0, 0, 4),
					bytes: (0, 5),
					term: "āl".to_string(),
					len: 2,
				},
				Token::String {
					chars: (0, 0, 4),
					bytes: (0, 5),
					term: "āle".to_string(),
					len: 3,
				},
				Token::String {
					chars: (0, 1, 4),
					bytes: (0, 5),
					term: "le".to_string(),
					len: 2,
				},
				Token::String {
					chars: (0, 1, 4),
					bytes: (0, 5),
					term: "lea".to_string(),
					len: 3,
				},
				Token::String {
					chars: (0, 2, 4),
					bytes: (0, 5),
					term: "ea".to_string(),
					len: 2,
				},
				Token::String {
					chars: (5, 5, 10),
					bytes: (6, 11),
					term: "ia".to_string(),
					len: 2,
				},
				Token::String {
					chars: (5, 5, 10),
					bytes: (6, 11),
					term: "iac".to_string(),
					len: 3,
				},
				Token::String {
					chars: (5, 6, 10),
					bytes: (6, 11),
					term: "ac".to_string(),
					len: 2,
				},
				Token::String {
					chars: (5, 6, 10),
					bytes: (6, 11),
					term: "act".to_string(),
					len: 3,
				},
				Token::String {
					chars: (5, 7, 10),
					bytes: (6, 11),
					term: "ct".to_string(),
					len: 2,
				},
				Token::String {
					chars: (5, 7, 10),
					bytes: (6, 11),
					term: "cta".to_string(),
					len: 3,
				},
				Token::String {
					chars: (5, 8, 10),
					bytes: (6, 11),
					term: "ta".to_string(),
					len: 2,
				},
			],
		)
		.await;
	}

	#[tokio::test]
	async fn test_edgengram() {
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS lowercase,edgengram(2,3);",
			"Ālea iacta est",
			&["āl", "āle", "ia", "iac", "es", "est"],
		)
		.await;
	}

	#[tokio::test]
	async fn test_lowercase_tokens() {
		test_analyzer_tokens(
			"ANALYZER test TOKENIZERS blank,class FILTERS lowercase",
			"Ālea IactA!",
			&[
				Token::String {
					chars: (0, 0, 4),
					bytes: (0, 5),
					term: "ālea".to_string(),
					len: 4,
				},
				Token::String {
					chars: (5, 5, 10),
					bytes: (6, 11),
					term: "iacta".to_string(),
					len: 5,
				},
				Token::Ref {
					chars: (10, 10, 11),
					bytes: (11, 12),
					len: 1,
				},
			],
		)
		.await;
	}

	#[tokio::test]
	async fn test_uppercase_tokens() {
		test_analyzer_tokens(
			"ANALYZER test TOKENIZERS blank,class FILTERS uppercase",
			"Ālea IactA!",
			&[
				Token::String {
					chars: (0, 0, 4),
					bytes: (0, 5),
					term: "ĀLEA".to_string(),
					len: 4,
				},
				Token::String {
					chars: (5, 5, 10),
					bytes: (6, 11),
					term: "IACTA".to_string(),
					len: 5,
				},
				Token::Ref {
					chars: (10, 10, 11),
					bytes: (11, 12),
					len: 1,
				},
			],
		)
		.await;
	}
}
