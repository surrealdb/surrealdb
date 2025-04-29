/**
[test]

[[test.results]]
value = "NONE"
[[test.results]]
error = "Cannot set field `fd[*]` with type `number` as it mismatched with field `fd` with type `array<string>`"

[[test.results]]
value = "NONE"
[[test.results]]
error = "Cannot set field `fd[*]` with type `number` as it mismatched with field `fd` with type `[string, number]`"
[[test.results]]
error = "Cannot set field `fd[0]` with type `number` as it mismatched with field `fd` with type `[string, number]`"
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
[[test.results]]
error = "Cannot set field `fd[*]` with type `number` as it mismatched with field `fd` with type `{ a: string, b: number }`"
[[test.results]]
error = "Cannot set field `fd.a` with type `number` as it mismatched with field `fd` with type `{ a: string, b: number }`"
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
[[test.results]]
error = "Cannot set field `fd[*]` with type `number` as it mismatched with field `fd` with type `{ a: string, b: number } | { a: string, b: bool }`"
[[test.results]]
error = "Cannot set field `fd.a` with type `number` as it mismatched with field `fd` with type `{ a: string, b: number } | { a: string, b: bool }`"
[[test.results]]
error = "Cannot set field `fd.b` with type `number` as it mismatched with field `fd` with type `{ a: string, b: number } | { a: string, b: bool }`"
[[test.results]]
error = "Cannot set field `fd[*]` with type `string` as it mismatched with field `fd` with type `{ a: string, b: number } | { a: string, b: bool }`"
[[test.results]]
value = "NONE"
[[test.results]]
error = "Cannot set field `fd.b` with type `string` as it mismatched with field `fd` with type `{ a: string, b: number } | { a: string, b: bool }`"

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
error = "Cannot set field `fd[0]` with type `number` as it mismatched with field `fd` with type `object`"

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
error = "Cannot set field `fd.a` with type `number` as it mismatched with field `fd` with type `object | array<number>`"
[[test.results]]
error = "Cannot set field `fd[0]` with type `number` as it mismatched with field `fd` with type `object | array<number>`"

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
error = "Cannot set field `fd[*]` with type `'a'` as it mismatched with field `fd` with type `array<'a' | 'b'>`"

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
error = "Cannot set field `fd[*]` with type `'a' | 'b'` as it mismatched with field `fd` with type `array<'a'>`"

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
error = "Cannot set field `fd[2]` with type `string` as it mismatched with field `fd` with type `array<string, 2>`"

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
*/

DEFINE FIELD OVERWRITE fd ON a TYPE array<string>;
DEFINE FIELD OVERWRITE fd.* ON a TYPE number;
--
DEFINE FIELD OVERWRITE fd ON b TYPE [string, number];
DEFINE FIELD OVERWRITE fd.* ON b TYPE number;
DEFINE FIELD OVERWRITE fd[0] ON b TYPE number;
DEFINE FIELD OVERWRITE fd[1] ON b TYPE number;
--
DEFINE FIELD OVERWRITE fd ON c TYPE { a: string, b: number };
DEFINE FIELD OVERWRITE fd.* ON c TYPE number;
DEFINE FIELD OVERWRITE fd.a ON c TYPE number;
DEFINE FIELD OVERWRITE fd.b ON c TYPE number;
--
DEFINE FIELD OVERWRITE fd ON d TYPE { a: string, b: number } | { a: string, b: bool };
DEFINE FIELD OVERWRITE fd.* ON d TYPE number;
DEFINE FIELD OVERWRITE fd.a ON d TYPE number;
DEFINE FIELD OVERWRITE fd.b ON d TYPE number;
DEFINE FIELD OVERWRITE fd.* ON d TYPE string;
DEFINE FIELD OVERWRITE fd.a ON d TYPE string;
DEFINE FIELD OVERWRITE fd.b ON d TYPE string;
--
DEFINE FIELD OVERWRITE fd ON e TYPE object;
DEFINE FIELD OVERWRITE fd.* ON e TYPE number;
DEFINE FIELD OVERWRITE fd.a ON e TYPE number;
DEFINE FIELD OVERWRITE fd[0] ON e TYPE number;
--
DEFINE FIELD OVERWRITE fd ON f TYPE object | array<number>;
DEFINE FIELD OVERWRITE fd.* ON f TYPE number;
DEFINE FIELD OVERWRITE fd.a ON f TYPE number;
DEFINE FIELD OVERWRITE fd[0] ON f TYPE number;
--
DEFINE FIELD OVERWRITE fd ON g TYPE option<array<number>>;
DEFINE FIELD OVERWRITE fd.* ON g TYPE number;
DEFINE FIELD OVERWRITE fd[0] ON g TYPE number;
--
DEFINE FIELD OVERWRITE fd ON h TYPE [option<number>, number];
DEFINE FIELD OVERWRITE fd.* ON h TYPE number;
DEFINE FIELD OVERWRITE fd[0] ON h TYPE number;
DEFINE FIELD OVERWRITE fd[1] ON h TYPE number;
--
DEFINE FIELD OVERWRITE fd ON i TYPE array<'a' | 'b'>;
DEFINE FIELD OVERWRITE fd.* ON i TYPE 'a' | 'b';
DEFINE FIELD OVERWRITE fd.* ON i TYPE 'a';
--
DEFINE FIELD OVERWRITE fd ON j TYPE array<'a'>;
DEFINE FIELD OVERWRITE fd.* ON j TYPE 'a';
DEFINE FIELD OVERWRITE fd.* ON j TYPE 'a' | 'b';
--
DEFINE FIELD OVERWRITE fd ON k TYPE array<string, 2>;
DEFINE FIELD OVERWRITE fd.* ON k TYPE string;
DEFINE FIELD OVERWRITE fd[0] ON k TYPE string;
DEFINE FIELD OVERWRITE fd[1] ON k TYPE string;
DEFINE FIELD OVERWRITE fd[2] ON k TYPE string;
--
DEFINE FIELD OVERWRITE fd ON l TYPE array<string>;
DEFINE FIELD OVERWRITE fd.* ON l TYPE any;
--
DEFINE FIELD OVERWRITE fd ON m TYPE array<any>;
DEFINE FIELD OVERWRITE fd.* ON m TYPE string;