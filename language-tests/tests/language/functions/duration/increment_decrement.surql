/**
[test]

[[test.results]]
value = "{ id: some:aggregate, time_elapsed: 1h }"

[[test.results]]
value = "{ id: some:aggregate, time_elapsed: 2h }"

[[test.results]]
value = "{ id: some:aggregate, time_elapsed: 1h }"

[[test.results]]
error = 'Failed to compute: "1h - 2h", as the operation results in a negative value.'
*/

UPSERT ONLY some:aggregate SET time_elapsed += 1h;
UPSERT ONLY some:aggregate SET time_elapsed += 1h;
UPSERT ONLY some:aggregate SET time_elapsed -= 1h;
UPSERT ONLY some:aggregate SET time_elapsed -= 2h;