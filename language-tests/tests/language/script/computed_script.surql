/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The COMPUTED clause must not have effects: a script, module or silo function runs code whose effects cannot be known where the clause is defined"

[[test.results]]
value = "[{ id: film:test, ratings: [{ rating: 6.3f }, { rating: 8.7f }] }]"

*/

DEFINE TABLE film SCHEMAFULL;
DEFINE FIELD ratings ON film TYPE array<{ rating: number }>;
DEFINE FIELD display ON film COMPUTED {
	function() {
		return this.ratings.filter(r => {
			return r.rating >= 7;
		}).map(r => {
			return { ...r, rating: Math.round(r.rating * 10) };
		});
	}
};

CREATE film:test SET
	ratings = [
		{ rating: 6.3 },
		{ rating: 8.7 },
	];
