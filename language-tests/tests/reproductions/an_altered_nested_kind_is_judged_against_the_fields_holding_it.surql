/**
[env]
namespace = true
database = true

[test]
reason = """
ALTER FIELD … TYPE on a nested field judges the new kind against every field the field is
nested in, as DEFINE FIELD does, and refuses a mismatch with the same error: `v.*` under
`array<float>` cannot become `int`, by a written or an evaluated name, nor can `g.*.*` under
`array<array<string>>`. The rule holds under OPTION IMPORT, where DEFINE FIELD keeps it too.
A kind the holding fields admit is accepted, and every refusal leaves the stored definitions as
they were.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot set field `v.*` with type `int` as it mismatched with field `v` with type `array<float>`"

[[test.results]]
error = "Cannot set field `v.*` with type `int` as it mismatched with field `v` with type `array<float>`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot set field `v.*` with type `int` as it mismatched with field `v` with type `array<float>`"

[[test.results]]
error = "Cannot set field `g.*.*` with type `int` as it mismatched with field `g` with type `array<array<string>>`"

[[test.results]]
error = "Cannot set field `g.*.*` with type `int` as it mismatched with field `g` with type `array<array<string>>`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot set field `v.*` with type `int` as it mismatched with field `v` with type `array<float>`"

[[test.results]]
error = "Cannot set field `v.*` with type `int` as it mismatched with field `v` with type `array<float>`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ g: 'DEFINE FIELD g ON c TYPE array<array<string>> PERMISSIONS FULL', "g.*": 'DEFINE FIELD g.* ON c TYPE array<string> PERMISSIONS FULL', "g.*.*": 'DEFINE FIELD g.*.* ON c TYPE string PERMISSIONS FULL', v: 'DEFINE FIELD v ON c TYPE array<float> PERMISSIONS FULL', "v.*": 'DEFINE FIELD v.* ON c TYPE float PERMISSIONS FULL' }"""

*/

DEFINE TABLE c SCHEMAFULL;
DEFINE FIELD v ON c TYPE array<float>;
DEFINE FIELD g ON c TYPE array<array<string>>;
-- The element definition `array<float>` implies is held to `float`.
ALTER FIELD v.* ON c TYPE int;
DEFINE FIELD OVERWRITE v.* ON c TYPE int;
LET $fd = "v.*";
ALTER FIELD $fd ON c TYPE int;
-- A field two levels deep is judged against the outermost field holding it.
ALTER FIELD g.*.* ON c TYPE int;
DEFINE FIELD OVERWRITE g.*.* ON c TYPE int;
OPTION IMPORT;
ALTER FIELD v.* ON c TYPE int;
DEFINE FIELD OVERWRITE v.* ON c TYPE int;
OPTION IMPORT = false;
-- A kind every holding field admits is accepted.
ALTER FIELD v.* ON c TYPE float;
(INFO FOR TABLE c).fields;
