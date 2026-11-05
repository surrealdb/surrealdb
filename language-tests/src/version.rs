use semver::{Prerelease, Version, VersionReq};

/// Check a [`VersionReq`] against a [`Version`] while ignoring the semver
/// "do not match pre-release versions unless explicitly named" rule — except
/// where that rule is the point.
///
/// Standard semver matches `>=3.0.1` against `3.0.5` (true) but rejects
/// `3.1.0-beta.1`, because a prerelease only satisfies a requirement when some
/// comparator names that same `major.minor.patch` *and* carries a prerelease of
/// its own. The rule exists so a `cargo` dependency on `>=3.0.1` cannot
/// silently pull in `4.0.0-rc1`. Upgrade tests want the opposite: a test gated
/// `importing-version = ">=3.0.1"` to skip 3.0.0 should run on every later
/// release, prerelease tags included. So a version that fails as-is is retried
/// with its prerelease identifier stripped.
///
/// The retry is withheld in exactly one case: when a prerelease comparator
/// names this version's own `major.minor.patch`. There the prerelease
/// identifier is the thing being compared, and stripping it would promote
/// `3.1.0-beta.1` to `3.1.0` and let it satisfy `>=3.1.0-beta.3`. Withholding
/// it more broadly — for any requirement that mentions a prerelease anywhere —
/// silently drops coverage: `3.3.0-beta.3` is unambiguously later than
/// `3.1.0-beta.3`, and a test gated on the latter must still run when the
/// former is the source.
pub fn version_matches(req: &VersionReq, version: &Version) -> bool {
	if req.matches(version) {
		return true;
	}
	if version.pre.is_empty() {
		return false;
	}
	let anchored_to_this_release = req.comparators.iter().any(|c| {
		!c.pre.is_empty()
			&& c.major == version.major
			&& c.minor == Some(version.minor)
			&& c.patch == Some(version.patch)
	});
	if anchored_to_this_release {
		return false;
	}
	let mut stripped = version.clone();
	stripped.pre = Prerelease::EMPTY;
	req.matches(&stripped)
}
