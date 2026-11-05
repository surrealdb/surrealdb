//! Dictionary-driven word segmentation for languages that do not delimit words
//! with spaces.
//!
//! The character-class tokenizers decide every boundary from the pair of
//! characters around it. That is enough for scripts that separate words with
//! whitespace or case changes, and useless for Chinese, Japanese and Korean:
//! Chinese and Japanese write a whole clause with no separator at all, and
//! Korean writes each word with its grammatical particles attached, so
//! `한국어를` is one span whose searchable part is `한국어`. Recovering words
//! from those spans needs a lattice search over dictionary entries, which needs
//! the whole span at once.
//!
//! A [`Segmenter`] therefore consumes one span and returns the words inside it,
//! each with the byte range it occupied in that span. Callers turn those ranges
//! into token offsets; nothing here knows about tokens.

// A wasm module is fetched whole before any of it runs, so a dictionary baked
// into one is paid for on every load by every visitor, whether or not a query
// ever segments anything. The smallest of the three is tens of megabytes and
// Korean's is over a hundred, which is not a size a wasm bundle can absorb.
// The engine reaches wasm through `surrealdb-idx -> core -> embedded ->
// surrealdb-wasm`, so nothing but this stops one of these features being
// turned on for such a build by a feature unification nobody inspected.
#[cfg(all(
	target_family = "wasm",
	any(
		feature = "cjk-embed-chinese",
		feature = "cjk-embed-japanese",
		feature = "cjk-embed-korean"
	)
))]
compile_error!(
	"The `cjk-embed-*` features cannot be enabled for a wasm target: each embeds a dictionary of \
	 tens to hundreds of megabytes into the module, which is then downloaded in full before \
	 anything runs. Build without them; on a wasm target with a filesystem, `SEGMENT` can still \
	 read a dictionary through the `segment_dictionary_path` config key."
);

use std::borrow::Cow;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, bail};
use lindera::dictionary::{Dictionary, load_dictionary, load_dictionary_with_options};
use lindera::mode::Mode;
use lindera::segmenter::Segmenter as LinderaSegmenter;
use tracing::info;

use crate::Error;
use crate::expr::tokenizer::SegmentLanguage;

/// One word found inside a span, and the byte range it occupied there.
pub(in crate::ft) struct Word {
	pub(in crate::ft) start: usize,
	pub(in crate::ft) end: usize,
}

/// A loaded morphological dictionary, ready to segment spans of one language.
///
/// Cloning shares the underlying dictionary: the handle is reference-counted
/// because a dictionary is far too large to copy per analyzer, and every
/// analyzer naming the same language must share one.
#[derive(Clone)]
pub(crate) struct Segmenter(Arc<LinderaSegmenter>);

impl Segmenter {
	/// Loads the dictionary for `language`.
	///
	/// `dictionary_path`, when set, names a directory holding one subdirectory
	/// per language, and overrides the built-in dictionary for each language it
	/// carries. A language it does not carry falls back to the built-in one, so
	/// replacing one language's dictionary does not withdraw the others.
	///
	/// The path is operator configuration, not a value any statement can
	/// reach, and the only names joined onto it are this module's own, so the
	/// set of files a caller can make this open is fixed at compile time. It is
	/// therefore not checked against `file_allowlist`, which exists to bound
	/// the paths a `DEFINE ANALYZER … mapper('<path>')` clause supplies.
	///
	/// Errors when the language has no dictionary either way: no subdirectory
	/// under `dictionary_path`, and no dictionary compiled in (its
	/// `cjk-embed-*` feature was not enabled at build time).
	pub(crate) fn new(language: SegmentLanguage, dictionary_path: Option<&Path>) -> Result<Self> {
		let from_path = match dictionary_path {
			Some(dir) => Self::load_from_path(language, dir)?,
			None => None,
		};
		// Which dictionary a node resolved is otherwise unrecoverable: the same
		// `SEGMENT(<language>)` definition replicates everywhere, while what it
		// resolves to comes from this node's build features, its configured
		// directory and the bytes in it. Two nodes that disagree produce
		// different terms for the same text and neither errors, so this line is
		// what makes such a divergence greppable after the fact. Logged once
		// per language, since a loaded dictionary is cached for the datastore.
		//
		// The identity reported is the one the dictionary states about itself,
		// read from the metadata it carries. Nothing else can state it
		// truthfully: a directory is named for the language it serves, and a
		// build feature names the dictionary it baked in when it was written.
		let dictionary = match from_path {
			Some(d) => {
				info!(
					language = %language,
					dictionary = %d.metadata.name,
					format_version = d.metadata.format_version,
					source = "directory",
					path = %dictionary_path.unwrap_or(Path::new("")).display(),
					"Loaded a segmentation dictionary from the configured directory"
				);
				d
			}
			None => {
				let d = Self::load_embedded(language, dictionary_path.is_some())?;
				info!(
					language = %language,
					dictionary = %d.metadata.name,
					format_version = d.metadata.format_version,
					source = "embedded",
					"Loaded the segmentation dictionary built into this binary"
				);
				d
			}
		};
		// `Mode::Normal` takes the single best segmentation, so a compound the
		// dictionary holds stays one token while one it does not is split into
		// the entries it is built from: `東京大学` is a single word, `東京都`
		// is `東京` + `都`. Which of the two a given compound gets is therefore
		// a property of the dictionary, not a rule — a search for `東京` finds
		// the second and not the first.
		//
		// Lindera can instead decompose compounds into their parts as well,
		// trading index size and precision for that recall. Nothing in the
		// grammar chooses between the two, so this applies to every `SEGMENT`
		// analyzer.
		Ok(Self(Arc::new(LinderaSegmenter::new(Mode::Normal, dictionary, None))))
	}

	/// Whether `path` is a usable directory, separating "nothing of that name"
	/// from "there, but not usable".
	///
	/// Every caller needs that distinction and none may blur it, so the rule
	/// lives here once. Two probes make it, because no single one can:
	///
	/// * `symlink_metadata` decides whether an entry exists, without following links, so a symlink
	///   whose target is gone counts as present. Only a `NotFound` from this probe is an absence.
	/// * `metadata` then decides whether what the entry names is usable. It follows links, so it
	///   reports a dangling one as `NotFound` - a broken deployment rather than an absence, which
	///   is why the first probe has to answer that question instead.
	///
	/// `Path::is_dir` can do neither job: it reports every error as `false`, so
	/// a directory that exists but cannot be read is indistinguishable from one
	/// that was never there.
	///
	/// `what` names the thing in words. The io errors' `Display` carries an
	/// errno and never the path, so surfacing them does not leak an
	/// operator-supplied path that this layer otherwise keeps to itself.
	fn probe_directory(path: &Path, what: &str) -> Result<bool> {
		match std::fs::symlink_metadata(path) {
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
			Err(e) => {
				bail!(Error::AnalyzerError(format!("{what} could not be looked for: {e}")))
			}
			Ok(_) => {}
		}
		match std::fs::metadata(path) {
			Err(e) => bail!(Error::AnalyzerError(format!("{what} could not be read: {e}"))),
			Ok(m) if !m.is_dir() => {
				bail!(Error::AnalyzerError(format!("{what} is not a directory")))
			}
			Ok(_) => Ok(true),
		}
	}

	/// Loads `language` from the configured directory, or `None` when that
	/// directory holds no dictionary for it.
	///
	/// A missing language entry, inside a root that is itself present, is the
	/// one condition that falls back to the built-in dictionary: the directory
	/// overrides the languages it actually carries and leaves the rest alone,
	/// so supplying a custom Korean dictionary does not take Japanese and
	/// Chinese away with it.
	///
	/// Everything else is an error — a missing or unusable root, a dictionary
	/// present but unreadable. Both are misconfigurations, and quietly
	/// segmenting with a different dictionary than the operator named would
	/// hide them.
	fn load_from_path(language: SegmentLanguage, dir: &Path) -> Result<Option<Dictionary>> {
		// The configured root must be there. Absence here is not "no override
		// for this language" but a broken deployment - an unmounted volume
		// reports exactly this - and treating it as absence would switch every
		// language to its embedded dictionary without a word.
		if !Self::probe_directory(dir, "The configured segmentation dictionary directory")? {
			bail!(Error::AnalyzerError(
				"The configured segmentation dictionary directory does not exist".to_string()
			));
		}
		let path = dir.join(Self::directory_name(language));
		// Inside a root that is present, a missing entry does mean this language
		// is not overridden.
		if !Self::probe_directory(&path, &format!("The {language} segmentation dictionary"))? {
			return Ok(None);
		}
		// Whether to memory-map is a runtime choice; the crate's `mmap` feature
		// only decides whether the mapped loader exists at all, and a
		// read-backed one sits beside it. `load_dictionary` would pick mapping
		// whenever the feature is on, which is everywhere, but `memmap2` has an
		// implementation only for unix and windows and compiles to a stub that
		// always fails elsewhere. Choosing per platform is what keeps a
		// filesystem-backed build on a target without mapping — a wasm one —
		// able to load a dictionary at all, rather than failing at every load.
		let use_mmap = cfg!(any(unix, windows));
		// The path is operator-supplied, so its content must not reach the
		// caller; report only the language that could not be loaded.
		load_dictionary_with_options(&path.to_string_lossy(), use_mmap).map(Some).map_err(|_| {
			anyhow::Error::new(Error::AnalyzerError(format!(
				"Could not load the {language} segmentation dictionary from the configured \
					 directory"
			)))
		})
	}

	/// `configured` says whether a dictionary directory was set, which decides
	/// the advice: an operator who configured one and misspelled the
	/// subdirectory must not be told to set a key they already set.
	///
	/// Which directory it was is deliberately not named. This error reaches the
	/// caller of the statement that triggered the load, who need not be the
	/// operator, and the rest of this module keeps an operator-supplied path
	/// out of what it returns for that reason. Whether a directory is
	/// configured is enough to give the right advice, and the operator who set
	/// it already knows the path.
	fn load_embedded(language: SegmentLanguage, configured: bool) -> Result<Dictionary> {
		if !Self::is_embedded(language) {
			let name = Self::directory_name(language);
			bail!(Error::AnalyzerError(if configured {
				format!(
					"No {language} segmentation dictionary. The configured directory holds no \
					 `{name}` subdirectory, and this build has none compiled in. Add `{name}` \
					 there, or rebuild with the matching `cjk-embed-*` feature."
				)
			} else {
				format!(
					"No {language} segmentation dictionary. This build has none compiled in, and \
					 no directory is configured. Rebuild with the matching `cjk-embed-*` feature, \
					 or set the `segment_dictionary_path` config key to a directory containing a \
					 `{name}` dictionary."
				)
			}));
		}
		let uri = format!("embedded://{}", Self::embedded_name(language));
		load_dictionary(&uri).map_err(|e| {
			anyhow::Error::new(Error::AnalyzerError(format!(
				"Could not load the embedded {language} segmentation dictionary: {e}"
			)))
		})
	}

	/// Whether this build baked in `language`'s dictionary.
	pub(crate) fn is_embedded(language: SegmentLanguage) -> bool {
		match language {
			SegmentLanguage::Chinese => cfg!(feature = "cjk-embed-chinese"),
			SegmentLanguage::Japanese => cfg!(feature = "cjk-embed-japanese"),
			SegmentLanguage::Korean => cfg!(feature = "cjk-embed-korean"),
		}
	}

	/// The subdirectory a language's dictionary is read from, under the
	/// configured dictionary directory.
	///
	/// Named for the language, not for the dictionary that currently
	/// implements it. Nothing in the load path derives anything from this name:
	/// a compiled dictionary declares its own identity in the metadata it
	/// carries, which is what [`Self::new`] logs. Naming the directory after a
	/// dictionary instead would make replacing that dictionary a breaking
	/// change — the directory an operator already deployed would stop matching
	/// the name looked up, which reads as "this language is not overridden"
	/// and falls back to the built-in one without a word.
	fn directory_name(language: SegmentLanguage) -> &'static str {
		match language {
			SegmentLanguage::Chinese => "chinese",
			SegmentLanguage::Japanese => "japanese",
			SegmentLanguage::Korean => "korean",
		}
	}

	/// The `embedded://` host naming the dictionary a `cjk-embed-*` feature
	/// bakes in for a language.
	///
	/// These are lindera's names for its own dictionaries, fixed by the crate
	/// rather than chosen here, and they reach nothing an operator sees.
	fn embedded_name(language: SegmentLanguage) -> &'static str {
		match language {
			SegmentLanguage::Chinese => "cc-cedict",
			SegmentLanguage::Japanese => "ipadic",
			SegmentLanguage::Korean => "ko-dic",
		}
	}

	/// Returns the words inside `span`, in order, each with its byte range in
	/// `span`.
	///
	/// Ranges are the segmenter's own byte offsets into the text it was given,
	/// so they index `span` directly and callers can slice with them.
	pub(in crate::ft) fn segment(&self, span: &str) -> Result<Vec<Word>> {
		let tokens = self.0.segment(Cow::Borrowed(span)).map_err(|e| {
			anyhow::Error::new(Error::AnalyzerError(format!("Segmentation failed: {e}")))
		})?;
		Ok(tokens
			.into_iter()
			.map(|t| Word {
				start: t.byte_start,
				end: t.byte_end,
			})
			.collect())
	}
}

#[cfg(test)]
mod tests {
	// Every test that stages a dictionary directory needs a loadable dictionary
	// to fall back to, so all of them are gated on an embedded one. Only the
	// naming test runs without that feature, and it touches no filesystem.
	#[cfg(feature = "cjk-embed-korean")]
	use temp_dir::TempDir;

	use super::*;

	/// A configured directory overrides only the languages it carries.
	///
	/// The directory exists to replace a dictionary with a custom or updated
	/// one, so a language it does not hold must keep using the built-in one
	/// rather than becoming unavailable: supplying a Korean dictionary should
	/// not take Japanese and Chinese away with it.
	#[cfg(feature = "cjk-embed-korean")]
	#[test]
	fn a_language_absent_from_the_directory_uses_the_embedded_dictionary() {
		let dir = TempDir::new().unwrap();
		// The directory exists, but holds no `korean`.
		let loaded = Segmenter::new(SegmentLanguage::Korean, Some(dir.path()));
		assert!(
			loaded.is_ok(),
			"an absent dictionary must fall back to the embedded one, got: {:?}",
			loaded.err()
		);
	}

	/// Absence falls back; something present under that name but unusable does
	/// not.
	///
	/// The distinction matters because `Path::is_dir` cannot make it: it reports
	/// a permission error and a missing file identically, which would let a
	/// dictionary the operator placed but the process cannot read pass silently
	/// as "not overridden". A plain file standing where the dictionary should be
	/// exercises the same branch portably, where a permission test would not
	/// (root ignores the mode).
	#[cfg(feature = "cjk-embed-korean")]
	#[test]
	fn a_non_directory_in_the_dictionary_slot_is_an_error_rather_than_a_fallback() {
		let dir = TempDir::new().unwrap();
		std::fs::write(dir.path().join("korean"), b"not a dictionary").unwrap();
		let Err(err) = Segmenter::new(SegmentLanguage::Korean, Some(dir.path())) else {
			panic!("a file standing in for the dictionary must not fall back to the embedded one");
		};
		assert!(
			err.to_string().contains("is not a directory"),
			"expected the error to say what is wrong, got: {err}"
		);
	}

	/// A configured directory that is not there at all is a broken deployment,
	/// not an absence of overrides.
	///
	/// A volume that failed to mount looks exactly like this, and the entry
	/// probe alone cannot tell the difference: a missing root makes every
	/// language's entry report missing too, which would switch the whole
	/// server to its embedded dictionaries without a word.
	#[cfg(feature = "cjk-embed-korean")]
	#[test]
	fn a_configured_directory_that_does_not_exist_is_an_error_rather_than_a_fallback() {
		let dir = TempDir::new().unwrap();
		let missing = dir.path().join("never-mounted");
		let Err(err) = Segmenter::new(SegmentLanguage::Korean, Some(&missing)) else {
			panic!("a missing configured directory must not fall back to the embedded dictionary");
		};
		assert!(
			err.to_string().contains("does not exist"),
			"expected the missing-directory error, got: {err}"
		);
	}

	/// A dictionary symlinked to a target that is gone is a broken deployment,
	/// not an absence.
	///
	/// Symlinking each dictionary at a versioned directory is a normal way to
	/// deploy one, so a mistyped or removed target is a realistic mistake. It
	/// must not resolve to "this language was never overridden": the index
	/// would fill with terms from a different dictionary than the operator
	/// configured, and nothing would say so.
	#[cfg(all(unix, feature = "cjk-embed-korean"))]
	#[test]
	fn a_dangling_dictionary_symlink_is_an_error_rather_than_a_fallback() {
		let dir = TempDir::new().unwrap();
		std::os::unix::fs::symlink(dir.path().join("gone"), dir.path().join("korean")).unwrap();
		let Err(err) = Segmenter::new(SegmentLanguage::Korean, Some(dir.path())) else {
			panic!("a dangling symlink must not fall back to the embedded dictionary");
		};
		assert!(
			err.to_string().contains("could not be read"),
			"expected the unreadable-dictionary error, got: {err}"
		);
	}

	/// The subdirectory an operator deploys is named for the language, and no
	/// dictionary's own name is ever looked for on disk.
	///
	/// This is what keeps replacing a language's dictionary from being a
	/// breaking change. It cannot be asserted by loading one — a directory
	/// named for a dictionary is simply absent, which falls back silently, so
	/// the only observable difference is which name was searched for.
	#[test]
	fn the_directory_searched_is_named_for_the_language() {
		for (language, expected) in [
			(SegmentLanguage::Chinese, "chinese"),
			(SegmentLanguage::Japanese, "japanese"),
			(SegmentLanguage::Korean, "korean"),
		] {
			assert_eq!(
				Segmenter::directory_name(language),
				expected,
				"the {language} dictionary directory must be named for the language"
			);
		}
	}
}
