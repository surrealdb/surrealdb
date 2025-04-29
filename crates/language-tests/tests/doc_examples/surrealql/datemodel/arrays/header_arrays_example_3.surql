/**
[test]

[[test.results]]
value = "[{ id: person:bzodctepdylyk7m0gp0z, results: [{ date: '2017-06-18T08:00:00Z', name: 'Algorithmics', score: 76 }, { date: '2018-03-21T08:00:00Z', name: 'Concurrent Programming', score: 83 }, { date: '2018-09-17T08:00:00Z', name: 'Advanced Computer Science 101', score: 69 }, { date: '2019-04-20T08:00:00Z', name: 'Distributed Databases', score: 73 }] }]"
skip-record-id-key = true

*/
CREATE person SET results = [
	{ score: 76, date: "2017-06-18T08:00:00Z", name: "Algorithmics" },
	{ score: 83, date: "2018-03-21T08:00:00Z", name: "Concurrent Programming" },
	{ score: 69, date: "2018-09-17T08:00:00Z", name: "Advanced Computer Science 101" },
	{ score: 73, date: "2019-04-20T08:00:00Z", name: "Distributed Databases" },
];
