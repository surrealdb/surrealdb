/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = """[
	{
		id: person:[1, 0]
	},
	{
		id: person:[1, 6]
	},
	{
		id: person:[1, 7]
	},
	{
		id: person:[1, 8]
	},
	{
		id: person:[1, 9]
	},
	{
		id: person:[2, 0]
	},
	{
		id: person:[2, 6]
	},
	{
		id: person:[2, 7]
	},
	{
		id: person:[2, 8]
	},
	{
		id: person:[2, 9]
	},
	{
		id: person:[3, 0]
	},
	{
		id: person:[3, 6]
	},
	{
		id: person:[3, 7]
	},
	{
		id: person:[3, 8]
	},
	{
		id: person:[3, 9]
	},
	{
		id: person:[4, 0]
	},
	{
		id: person:[4, 6]
	},
	{
		id: person:[4, 7]
	},
	{
		id: person:[4, 8]
	},
	{
		id: person:[4, 9]
	},
	{
		id: person:[5, 0]
	},
	{
		id: person:[5, 6]
	},
	{
		id: person:[5, 7]
	},
	{
		id: person:[5, 8]
	},
	{
		id: person:[5, 9]
	}
]"""
*/

FOR $i IN [1,2,3,4,5] {
	FOR $j IN [6,7,8,9,0] {
		CREATE type::thing('person', [$i, $j]);
	}
};
SELECT * FROM person;
