/**
# 100000 documents for the ranked full-text benches in
# `bench/fulltext/ranked_*.surql`: `SELECT … search::score(1) … ORDER BY s DESC
# LIMIT 10`, which `records-*-fulltext.surql` cannot drive. Those fixtures draw
# every word of every 24-word document from twelve, so each term is in most
# documents and weighs next to nothing, and most scores tie.
#
# Each document's `body` holds 8 to 48 terms `w<n>` with `n = floor(u³ · 2000)`
# for a uniform `u`, which skews the draws towards small `n`: a few terms are in
# most documents and most terms are rare. The lengths vary so that BM25's length
# normalisation spreads the scores rather than tying most hits. The benches
# query terms across that range (document frequencies from a simulation of the
# same draw):
#
#   term   documents  share
#   w5        20757   20.8%   common
#   w20        9423    9.4%
#   w500       1183    1.2%   rare
#
# The index records offsets (`HIGHLIGHTS`), so a document's postings carry what
# highlighting reads, as an index that highlights does.
#
# Deterministic: the bench harness reseeds the engine RNG before every dataset
# build. The index is defined before the rows, so the harness's post-import
# compaction drain folds every term before the first measured iteration.
# `run = false`.

[test]
run = false
*/

DEFINE ANALYZER ranked TOKENIZERS blank FILTERS lowercase;
DEFINE TABLE doc SCHEMALESS;
DEFINE INDEX doc_body ON doc FIELDS body FULLTEXT ANALYZER ranked BM25 HIGHLIGHTS;

-- One top-level statement per 1000-row chunk, so each chunk commits in its own
-- transaction: the cost of a write grows with the size of the transaction it
-- lands in. Rows are built with `array::map` in a `LET` and written with one
-- `INSERT` per chunk, which is far faster than a closure inside the write.
DEFINE FUNCTION fn::chunk($chunk: int) {
	LET $rows = array::map(array::range($chunk * 1000, $chunk * 1000 + 1000), |$i| {
		id: $i,
		body: array::join(
			array::map(array::range(0, rand::int(8, 48)), |$k| 'w' + <string> <int> math::floor(math::pow(rand::float(), 3) * 2000)),
			' '
		),
	});
	INSERT INTO doc $rows RETURN NONE;
};
fn::chunk(0); fn::chunk(1); fn::chunk(2); fn::chunk(3); fn::chunk(4); fn::chunk(5); fn::chunk(6); fn::chunk(7); fn::chunk(8); fn::chunk(9);
fn::chunk(10); fn::chunk(11); fn::chunk(12); fn::chunk(13); fn::chunk(14); fn::chunk(15); fn::chunk(16); fn::chunk(17); fn::chunk(18); fn::chunk(19);
fn::chunk(20); fn::chunk(21); fn::chunk(22); fn::chunk(23); fn::chunk(24); fn::chunk(25); fn::chunk(26); fn::chunk(27); fn::chunk(28); fn::chunk(29);
fn::chunk(30); fn::chunk(31); fn::chunk(32); fn::chunk(33); fn::chunk(34); fn::chunk(35); fn::chunk(36); fn::chunk(37); fn::chunk(38); fn::chunk(39);
fn::chunk(40); fn::chunk(41); fn::chunk(42); fn::chunk(43); fn::chunk(44); fn::chunk(45); fn::chunk(46); fn::chunk(47); fn::chunk(48); fn::chunk(49);
fn::chunk(50); fn::chunk(51); fn::chunk(52); fn::chunk(53); fn::chunk(54); fn::chunk(55); fn::chunk(56); fn::chunk(57); fn::chunk(58); fn::chunk(59);
fn::chunk(60); fn::chunk(61); fn::chunk(62); fn::chunk(63); fn::chunk(64); fn::chunk(65); fn::chunk(66); fn::chunk(67); fn::chunk(68); fn::chunk(69);
fn::chunk(70); fn::chunk(71); fn::chunk(72); fn::chunk(73); fn::chunk(74); fn::chunk(75); fn::chunk(76); fn::chunk(77); fn::chunk(78); fn::chunk(79);
fn::chunk(80); fn::chunk(81); fn::chunk(82); fn::chunk(83); fn::chunk(84); fn::chunk(85); fn::chunk(86); fn::chunk(87); fn::chunk(88); fn::chunk(89);
fn::chunk(90); fn::chunk(91); fn::chunk(92); fn::chunk(93); fn::chunk(94); fn::chunk(95); fn::chunk(96); fn::chunk(97); fn::chunk(98); fn::chunk(99);
REMOVE FUNCTION fn::chunk;
