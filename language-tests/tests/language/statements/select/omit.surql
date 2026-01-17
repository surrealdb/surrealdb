/**

[test]

# 0: Create two people with a relationship between them
[[test.results]]
value = "[{ id: person:jaime, name: 'Jaime', opts: { age: 21, nested: { data: 'test' }, security: 'secure' }, password: '123456' }, { id: person:tobie, name: 'Tobie', opts: { age: 21, nested: { data: 'test' }, security: 'secure' }, password: '123456' }]"

# 1: Select all fields except password
[[test.results]]
value = "[{ id: person:jaime, name: 'Jaime', opts: { age: 21, nested: { data: 'test' }, security: 'secure' } }, { id: person:tobie, name: 'Tobie', opts: { age: 21, nested: { data: 'test' }, security: 'secure' } }]"

# 2: Select all fields except opts
[[test.results]]
value = "[{ id: person:jaime, name: 'Jaime', password: '123456' }, { id: person:tobie, name: 'Tobie', password: '123456' }]"

# 3: Select all fields except opts.age and password
[[test.results]]
value = "[{ id: person:jaime, name: 'Jaime', opts: { nested: { data: 'test' }, security: 'secure' } }, { id: person:tobie, name: 'Tobie', opts: { nested: { data: 'test' }, security: 'secure' } }]"

# 4: Select all fields except opts.nested and password
[[test.results]]
value = "[{ id: person:jaime, name: 'Jaime', opts: { nested: { data: 'test' }, security: 'secure' } }, { id: person:tobie, name: 'Tobie', opts: { nested: { data: 'test' }, security: 'secure' } }]"

# 5: Select all fields except opts.age and password
[[test.results]]
value = "[{ id: person:jaime, name: 'Jaime', opts: { age: 21, security: 'secure' } }, { id: person:tobie, name: 'Tobie', opts: { age: 21, security: 'secure' } }]"

# 6: Select all fields except opts.nested.data and password
[[test.results]]
value = "[{ id: person:jaime, name: 'Jaime', opts: { age: 21, nested: {  }, security: 'secure' } }, { id: person:tobie, name: 'Tobie', opts: { age: 21, nested: {  }, security: 'secure' } }]"

# 7, 8: Test asterisks in OMIT
[[test.results]]
value = "[{ id: person:jaime, name: 'Jaime', opts: { age: 21, nested: {  }, security: 'secure' } }, { id: person:tobie, name: 'Tobie', opts: { age: 21, nested: {  }, security: 'secure' } }]"
[[test.results]]
value = "[{ id: person:jaime, name: 'Jaime', opts: {  } }, { id: person:tobie, name: 'Tobie', opts: {  } }]"

# 9, 10: Test OMIT when selecting a single field
[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Tobie' }]"
[[test.results]]
value = "[{  }, {  }]"

# 11: Test OMIT applies only to the current record, not relationships.
[[test.results]]
value = "[{ id: person:jaime, knows: [], name: 'Jaime', password: '123456' }, { id: person:tobie, knows: [{ id: person:jaime, name: 'Jaime', opts: { age: 21, nested: { data: 'test' }, security: 'secure' }, password: '123456' }], name: 'Tobie', password: '123456' }]"


*/
// 0: Create two people with a relationship between them
{
    CREATE person:tobie CONTENT {
        name: 'Tobie',
        password: '123456',
        opts: {
            security: 'secure',
            age: 21,
            nested: {
                data: "test"
            }
        }
    };
    CREATE person:jaime CONTENT {
        name: 'Jaime',
        password: '123456',
        opts: {
            security: 'secure',
            age: 21,
            nested: {
                data: "test"
            }
        }
    };
    RELATE person:tobie->knows->person:jaime;
    SELECT * FROM person;
};

// 1: Select all fields except password
SELECT * OMIT password FROM person;

// 2: Select all fields except opts
SELECT * OMIT opts FROM person;

// 3: Select all fields except opts.age and password
SELECT * OMIT opts.age, password FROM person;

// 4: Select all fields except opts.age and password
SELECT * OMIT opts.age, password FROM person;

// 5: Select all fields except opts.nested and password
SELECT * OMIT opts.nested, password FROM person;

// 6: Select all fields except opts.nested.data and password
SELECT * OMIT opts.nested.data, password FROM person;

// 7, 8: Test asterisks in OMIT
SELECT * OMIT opts.nested.*, password FROM person;
SELECT * OMIT opts.*, password FROM person;

// 9, 10: Test OMIT when selecting a single field
SELECT name OMIT password FROM person;
SELECT name OMIT name FROM person;

// 11: Test OMIT applies only to the current record, not relationships.
SELECT *, ->knows->person.* as knows OMIT opts FROM person;
