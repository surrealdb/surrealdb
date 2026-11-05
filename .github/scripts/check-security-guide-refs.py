#!/usr/bin/env python3
"""Verify that every code reference in SECURITY_GUIDE.md still resolves.

The guide names modules, files, functions, types and constants so that a reviewer
can find the enforcement point for an invariant. When code is renamed or moved and
the guide is not updated, the invariant silently stops being reviewable. This check
fails CI on that drift.

Two kinds of reference are checked, both taken from inline-code spans:

  paths        anything ending in `.rs`, resolved against the tree. The guide omits
               the crate's `src/` segment, so `dbs/options.rs` matches
               `surrealdb/core/src/dbs/options.rs`.
  identifiers  SCREAMING_SNAKE_CASE, snake_case, and multi-hump CamelCase tokens,
               which must appear somewhere in a .rs file.

Tokens that are prose, SurrealQL syntax, config keys or external names are listed in
.github/security-guide-refs-allow.txt with a reason. Deliberate exceptions go there
too; the point is that skipping a reference is a recorded decision, not an accident.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GUIDE = ROOT / "SECURITY_GUIDE.md"
ALLOW = ROOT / ".github" / "security-guide-refs-allow.txt"
SEARCH_ROOTS = [ROOT / "surrealdb", ROOT / "surrealism"]
SKIP_DIRS = {"target", "worktrees", ".git", "node_modules"}

CODE_SPAN = re.compile(r"`([^`\n]+)`")
PATH_RE = re.compile(r"^[\w./-]+\.rs$")
SCREAMING = re.compile(r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+$")
SNAKE = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)+$")
CAMEL = re.compile(r"^[A-Z][a-z0-9]+(?:[A-Z][a-z0-9]+)+$")


def load_allowlist():
    if not ALLOW.exists():
        return set()
    out = set()
    for line in ALLOW.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            out.add(line)
    return out


def candidates(text):
    """Yield (kind, token) for every checkable reference in the guide."""
    for span in CODE_SPAN.findall(text):
        span = span.strip()
        if PATH_RE.match(span):
            yield "path", span
            continue
        # A span may hold an expression; split it into identifier-ish atoms.
        for raw in re.split(r"[^\w:.$]+", span):
            for atom in raw.split("::"):
                atom = atom.strip().rstrip("()").lstrip("$").strip(".")
                if not atom:
                    continue
                if SCREAMING.match(atom) or SNAKE.match(atom) or CAMEL.match(atom):
                    yield "ident", atom


WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def index_tree():
    """One pass over the Rust sources: every identifier token, every file path.

    Cheaper and more portable than shelling out per reference, and it keeps the
    check dependency-free (no ripgrep on the runner).
    """
    idents, paths = set(), set()
    for root in SEARCH_ROOTS:
        if not root.is_dir():
            continue
        for path in root.rglob("*.rs"):
            rel = path.relative_to(ROOT).as_posix()
            # Match against the repo-relative parts only: the checkout itself may
            # sit under a directory named like one of the skips (a git worktree).
            if SKIP_DIRS & set(rel.split("/")):
                continue
            paths.add(rel)
            try:
                idents.update(WORD.findall(path.read_text(errors="replace")))
            except OSError:
                continue
    return idents, paths


def path_matches(rel, paths):
    """Resolve a guide path against the tree.

    The guide omits the crate's `src/` segment, and that segment sits after the
    crate name rather than at the front (`gql/lower/mutation.rs` is really
    `surrealdb/gql/src/lower/mutation.rs`), so compare against each real path with
    its `src/` component removed as well as against the path itself.
    """
    for p in paths:
        stripped = "/".join(part for part in p.split("/") if part != "src")
        for cand in (p, stripped):
            if cand == rel or cand.endswith("/" + rel):
                return True
    return False


def main():
    if not GUIDE.exists():
        print(f"error: {GUIDE} not found", file=sys.stderr)
        return 1

    allow = load_allowlist()
    text = GUIDE.read_text()
    idents, paths = index_tree()

    seen, missing = set(), []
    for kind, token in candidates(text):
        if token in allow or (kind, token) in seen:
            continue
        seen.add((kind, token))
        ok = path_matches(token, paths) if kind == "path" else token in idents
        if not ok:
            missing.append((kind, token))

    print(f"checked {len(seen)} references from SECURITY_GUIDE.md "
          f"({len(allow)} allowlisted)")

    if missing:
        print("\nThese references do not resolve in the tree:\n", file=sys.stderr)
        for kind, token in sorted(missing):
            print(f"  {kind:5}  {token}", file=sys.stderr)
        print(
            "\nEither the code moved and the guide needs updating, or the reference is "
            "correct but unresolvable (a planned symbol, an external name, prose that "
            "looks like code). In the second case add it to "
            ".github/security-guide-refs-allow.txt with a reason.",
            file=sys.stderr,
        )
        return 1

    print("all references resolve")
    return 0


if __name__ == "__main__":
    sys.exit(main())
