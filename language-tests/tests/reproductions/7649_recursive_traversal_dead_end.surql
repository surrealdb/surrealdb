/**
[test]
reason = "Recursive traversal preserves reachable records when another branch ends"
issue = 7649
results = [
    "[]", "[]", "[]",
    "[p:b, p:c]", "[p:b, p:c]", "[p:b, p:c]", "[p:b, p:c]",
    "[]", "[p:b, p:c]", "[]", "[p:b, p:c]",
    "[]", "[]", "[]", "[]", "[]", "[]", "[]",
    "[g:b, g:c]", "[g:b, g:c]",
    "[]", "[s:a]", "[s:a]", "[s:a]",
    "[]", "[n:b, n:c]", "[n:d]", "[]"
]
*/

CREATE p:b SET k = 1 RETURN NONE;
CREATE p:c SET e = [{ target: p:b }, { target: p:c }] RETURN NONE;
CREATE p:a SET e = [{ target: p:b }, { target: p:c }] RETURN NONE;
RETURN array::sort(p:a.{1}(.e.target) ?? []);
RETURN array::sort(p:a.{2}(.e.target) ?? []);
RETURN array::sort(p:a.{3}(.e.target) ?? []);
RETURN array::sort(p:a.{2..4}(.e.target) ?? []);
UPDATE p:b SET e = [] RETURN NONE;
RETURN array::sort(p:a.{2}(.e.target) ?? []);
UPDATE p:b SET e = NULL RETURN NONE;
RETURN array::sort(p:a.{3..4}(.e.target) ?? []);

CREATE g:a RETURN NONE;
CREATE g:b RETURN NONE;
CREATE g:c RETURN NONE;
RELATE g:a->ge->g:b RETURN NONE;
RELATE g:a->ge->g:c RETURN NONE;
RELATE g:c->ge->g:b RETURN NONE;
RELATE g:c->ge->g:c RETURN NONE;
RETURN array::sort(g:a.{2}(->ge->g) ?? []);
RETURN array::sort(g:a->ge->g->ge->g);

-- Repeated record arrays also count toward the minimum without a dead sibling.
CREATE s:a SET e = [{ target: s:a }] RETURN NONE;
RETURN s:a.{1}(.e.target);
RETURN s:a.{2}(.e.target);
RETURN s:a.{5..7}(.e.target);

-- A dead sibling must not stop a surviving branch whose frontier changes.
INSERT [
    { id: n:a, e: [{ target: n:b }, { target: n:c }] },
    { id: n:b },
    { id: n:c, e: [{ target: n:d }] },
    { id: n:d }
] RETURN NONE;
RETURN array::sort(n:a.{1}(.e.target));
RETURN n:a.{2}(.e.target);
RETURN n:a.{3}(.e.target) ?? [];
