/**
[test]
reason = "DEFINE API duplicate detection and consistent error messages"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The api '/duptest' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The api '/duptest' does not exist"

*/

-- Initial define succeeds
DEFINE API "/duptest" FOR get THEN { RETURN { status: 200 }; };

-- Duplicate define errors
DEFINE API "/duptest" FOR get THEN { RETURN { status: 200 }; };

-- IF NOT EXISTS silently succeeds on duplicate
DEFINE API IF NOT EXISTS "/duptest" FOR get THEN { RETURN { status: 200 }; };

-- OVERWRITE succeeds on duplicate
DEFINE API OVERWRITE "/duptest" FOR get THEN { RETURN { status: 201 }; };

-- Cleanup
REMOVE API "/duptest";

-- Remove non-existent errors
REMOVE API "/duptest";
