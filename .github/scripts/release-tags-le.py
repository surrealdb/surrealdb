#!/usr/bin/env python3
"""Filter release tags to those at or below a semver threshold.

Reads candidate tags (one per line) on stdin, takes the threshold version as the
sole argument, and prints to stdout the input tags whose semantic version is at
or below the threshold — preserving the original tag text (so the caller can push
it). Used by sync-upstream.yml to bound a releases sync to a version: pushing the
`vX.Y.Z` release must not leak newer tags (`vX.Y.(Z+1)`, `v(X).(Y+1).0`, …) that
already exist in the private repo.

Comparison follows Semantic Versioning 2.0.0 precedence, which `sort -V` does NOT
get right for pre-releases: a pre-release version (e.g. 3.1.6-rc.2) has LOWER
precedence than its associated normal version (3.1.6). Numeric pre-release
identifiers rank below alphanumeric ones and compare numerically; a larger set of
pre-release identifiers ranks higher when all preceding identifiers are equal.
Build metadata (`+...`) is ignored. Tags may carry an optional leading `v`.

Exit status is 0 on success (including when nothing matches). A malformed
threshold is a hard error (exit 2). Individual tags that do not parse as a
version are skipped with a warning on stderr (never silently included).
"""
import re
import sys

# major.minor.patch, optional -prerelease, optional +build, optional leading v.
_SEMVER = re.compile(
    r"^v?(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)"
    r"(?:-(?P<pre>[0-9A-Za-z.-]+))?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)


def _pre_key(pre):
    """Precedence key for the pre-release segment (None means a normal release).

    A normal release outranks any pre-release of the same major.minor.patch, so
    None sorts ABOVE every pre-release. Within pre-releases, identifiers compare
    field-by-field: numeric identifiers (encoded 0) rank below alphanumeric ones
    (encoded 1) and compare numerically; a longer identifier list outranks a
    shorter one that is otherwise its prefix.
    """
    if pre is None:
        return (1,)  # normal release: highest precedence
    idents = []
    for ident in pre.split("."):
        if ident.isdigit():
            idents.append((0, int(ident), ""))
        else:
            idents.append((1, 0, ident))
    return (0, tuple(idents))


def parse(tag):
    """Return a semver sort key for `tag`, or None if it is not a version."""
    m = _SEMVER.match(tag.strip())
    if not m:
        return None
    return (
        int(m.group("major")),
        int(m.group("minor")),
        int(m.group("patch")),
        _pre_key(m.group("pre")),
    )


def main(argv):
    if len(argv) != 2:
        print("usage: release-tags-le.py <threshold-version>   (tags on stdin)", file=sys.stderr)
        return 2
    threshold_key = parse(argv[1])
    if threshold_key is None:
        print(f"release-tags-le.py: threshold '{argv[1]}' is not a valid version", file=sys.stderr)
        return 2

    for line in sys.stdin:
        tag = line.rstrip("\n")
        if not tag.strip():
            continue
        key = parse(tag)
        if key is None:
            print(f"release-tags-le.py: skipping unparseable tag '{tag}'", file=sys.stderr)
            continue
        if key <= threshold_key:
            print(tag)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
