/**
[test]
reason = "Repeated results execute the body to the minimum, without assuming future evaluations succeed"
issue = 7649
results = [
    "[]", "[]", "[s:x]", "3",
    "[]", "[s:x]", "2",
    "[]", "[]", "3",
    "[]", "[s:z]"
]
*/

CREATE counter:one SET visits = 0 RETURN NONE;
CREATE s:x SET e = [s:x] RETURN NONE;

-- The body runs to the minimum and then stops on equality, before the maximum.
RETURN s:x.{3..5}(.e.map(|$id| {
    UPDATE counter:one SET visits += 1;
    RETURN $id;
}));
RETURN counter:one.visits;

-- An unbounded self-link still terminates once the minimum is satisfied.
UPDATE counter:one SET visits = 0 RETURN NONE;
RETURN s:x.{..}(.e.map(|$id| {
    UPDATE counter:one SET visits += 1;
    RETURN $id;
}));
RETURN counter:one.visits;

-- Equal earlier results do not bypass a later real dead end before the minimum.
UPDATE counter:one SET visits = 0 RETURN NONE;
RETURN s:x.{4..5}(.e.map(|$id| {
    UPDATE counter:one SET visits += 1;
    RETURN IF counter:one.visits = 3 { NONE } ELSE { $id };
}));
RETURN counter:one.visits;

-- A real dead end after the minimum still returns the last valid result.
CREATE s:y SET e = [s:z] RETURN NONE;
RETURN s:y.{1..5}(.e);
