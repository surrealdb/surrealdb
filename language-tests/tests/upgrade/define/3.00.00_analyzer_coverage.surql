/**
[env]
imports = ["upgrade/define/3.00.00_analyzer_coverage_import.surql"]

[test]
upgrade = true
reason = """
Covers the CAMEL and PUNCT variants of the stored `Tokenizer` enum
(surrealdb/expr/src/expr/tokenizer.rs) and the ASCII, UPPERCASE and EDGENGRAM
variants of `Filter` (surrealdb/expr/src/expr/filter.rs). MAPPER is left out on
purpose: it names a file the harness cannot provision.

Each definition is asserted twice. The rendered DDL in `INFO FOR DB` proves the
stored enum discriminants still decode to the variants that were written, and
`search::analyze` proves the new binary rebuilds a working tokenizer and filter
chain out of them — a recompute through the stored definition, not a string
round-trip. The token streams are chosen so a wrong variant cannot produce
them: CAMEL keeps a run of capitals together and splits only on a lower-to-upper
boundary, PUNCT emits each punctuation mark as its own token while keeping
whitespace inside one, EDGENGRAM emits prefixes and passes a token shorter than
`min` through unchanged, ASCII folds accents, and the mixed analyzer applies
ASCII before UPPERCASE, so filter order is observable too."""

[[test.results]]
match = '''
    $result.analyzers.up_analyzer_coverage_camel == "DEFINE ANALYZER up_analyzer_coverage_camel TOKENIZERS CAMEL FILTERS LOWERCASE"
        && $result.analyzers.up_analyzer_coverage_punct == "DEFINE ANALYZER up_analyzer_coverage_punct TOKENIZERS PUNCT FILTERS UPPERCASE"
        && $result.analyzers.up_analyzer_coverage_ascii == "DEFINE ANALYZER up_analyzer_coverage_ascii TOKENIZERS BLANK FILTERS ASCII"
        && $result.analyzers.up_analyzer_coverage_edge == "DEFINE ANALYZER up_analyzer_coverage_edge TOKENIZERS CLASS FILTERS LOWERCASE, EDGENGRAM(2,4)"
        && $result.analyzers.up_analyzer_coverage_mixed == "DEFINE ANALYZER up_analyzer_coverage_mixed TOKENIZERS CAMEL,PUNCT FILTERS ASCII, UPPERCASE"
'''

[[test.results]]
value = "['get', 'httpresponse', 'code']"

[[test.results]]
value = "['SUR', '.', 'REAL', ',', 'DB']"

[[test.results]]
value = "['Cafe', 'Naive', 'Uber']"

[[test.results]]
value = "['su', 'sur', 'surr', 'is', 'db']"

[[test.results]]
value = "['MY', 'URL', '.', 'CAFE']"

*/
INFO FOR DB;
-- CAMEL breaks only where a lowercase letter is followed by an uppercase one,
-- so an acronym stays glued to the word that follows it.
search::analyze('up_analyzer_coverage_camel', 'getHTTPResponseCode');
-- PUNCT isolates every punctuation character into a token of its own.
search::analyze('up_analyzer_coverage_punct', 'sur.real,db');
-- ASCII transliterates to ASCII without changing case.
search::analyze('up_analyzer_coverage_ascii', 'Café Naïve Über');
-- EDGENGRAM emits the prefixes of each token between min and max characters,
-- and passes a token no longer than min through unchanged.
search::analyze('up_analyzer_coverage_edge', 'Surreal is DB');
-- Two tokenizers split on the union of their boundaries, and the filters run in
-- the order they were declared.
search::analyze('up_analyzer_coverage_mixed', 'myURL.café');
