/**
[test]

[[test.results]]
value = "[{ id: tag:rs, name: 'Rust' }]"

[[test.results]]
value = "[{ id: tag:go, name: 'Golang' }]"

[[test.results]]
value = "[{ id: tag:js, name: 'JavaScript' }]"

[[test.results]]
value = "[{ id: person:tobie, tags: [tag:rs, tag:go, tag:js] }]"

[[test.results]]
value = "[{ id: person:jaime, tags: [tag:js] }]"

[[test.results]]
value = "[{ id: person:jaime, tags: [{ id: tag:js, name: 'JavaScript' }] }]"

[[test.results]]
value = """[
    { 
        id: person:tobie, 
        tags: [{ id: tag:rs, name: "Rust" }, { id: tag:go, name: 'Golang' }, { id: tag:js, name: 'JavaScript' }] 

    }
]"""
*/

CREATE tag:rs SET name = 'Rust';
CREATE tag:go SET name = 'Golang';
CREATE tag:js SET name = 'JavaScript';
CREATE person:tobie SET tags = [tag:rs, tag:go, tag:js];
CREATE person:jaime SET tags = [tag:js];
SELECT * FROM person LIMIT 1 FETCH tags;
SELECT * FROM person START 1 LIMIT 1 FETCH tags;
