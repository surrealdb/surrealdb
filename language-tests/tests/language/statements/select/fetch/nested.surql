/**
[test]
reason = "Test FETCH clause with nested fields, wildcards, and deep paths"

# 0: Setup
[[test.results]]
value = "'OK'"

# 1: Basic FETCH
[[test.results]]
value = """[\
    { author: { age: 30, company: company:acme, id: person:john, name: 'John Doe' }, id: article:art1, title: 'Article One' },\
    { author: { age: 28, company: company:techco, id: person:jane, name: 'Jane Smith' }, id: article:art2, title: 'Article Two' }\
]"""

# 2: Nested FETCH with both author and author.company
[[test.results]]
value = """[\
    { author: { age: 30, company: { address: address:addr1, id: company:acme, industry: 'Technology', name: 'Acme Corp' }, id: person:john, name: 'John Doe' }, id: article:art1, title: 'Article One' },\
    { author: { age: 28, company: { address: address:addr2, id: company:techco, industry: 'Software', name: 'TechCo' }, id: person:jane, name: 'Jane Smith' }, id: article:art2, title: 'Article Two' }\
]"""

# 3: Nested FETCH with both author.company and author in different orders -- should be same as 2
[[test.results]]
value = """[\
    { author: { age: 30, company: { address: address:addr1, id: company:acme, industry: 'Technology', name: 'Acme Corp' }, id: person:john, name: 'John Doe' }, id: article:art1, title: 'Article One' },\
    { author: { age: 28, company: { address: address:addr2, id: company:techco, industry: 'Software', name: 'TechCo' }, id: person:jane, name: 'Jane Smith' }, id: article:art2, title: 'Article Two' }\
]"""


# 4: Deeply nested FETCH -- different orders should be identical
[[test.results]]
value = """[\
    {\
      author: {\
        age: 30,\
        company: {\
            address: { city: 'San Francisco', id: address:addr1, state: 'CA', street: '123 Main St', zip: '94102' },\
            id: company:acme,\
            industry: 'Technology', name: 'Acme Corp'\
        },\
        id: person:john,\
        name: 'John Doe'\
      },\
      id: article:art1,\
      title: 'Article One'\
    },\
    {\
      author: {\
        age: 28,\
        company: {\
            address: { city: 'New York', id: address:addr2, state: 'NY', street: '456 Oak Ave', zip: '10001' },\
            id: company:techco,\
            industry: 'Software', name: 'TechCo'\
        },\
        id: person:jane,\
        name: 'Jane Smith'\
      },\
      id: article:art2,\
      title: 'Article Two'\
    }\
]"""

# 5: Deeply nested FETCH -- should be same as 4
[[test.results]]
value = """[\
    {\
      author: {\
        age: 30,\
        company: {\
            address: { city: 'San Francisco', id: address:addr1, state: 'CA', street: '123 Main St', zip: '94102' },\
            id: company:acme,\
            industry: 'Technology', name: 'Acme Corp'\
        },\
        id: person:john,\
        name: 'John Doe'\
      },\
      id: article:art1,\
      title: 'Article One'\
    },\
    {\
      author: {\
        age: 28,\
        company: {\
            address: { city: 'New York', id: address:addr2, state: 'NY', street: '456 Oak Ave', zip: '10001' },\
            id: company:techco,\
            industry: 'Software', name: 'TechCo'\
        },\
        id: person:jane,\
        name: 'Jane Smith'\
      },\
      id: article:art2,\
      title: 'Article Two'\
    }\
]"""

# 7: FETCH with non-recordid fields - this just fetches author.
[[test.results]]
value = """[\
    { author: { age: 30, company: company:acme, id: person:john, name: 'John Doe' }, id: article:art1, title: 'Article One' },\
    { author: { age: 28, company: company:techco, id: person:jane, name: 'Jane Smith' }, id: article:art2, title: 'Article Two' }\
]"""

*/

{
    -- Setup nested data structures
    CREATE address:addr1 SET street = '123 Main St', city = 'San Francisco', state = 'CA', zip = '94102';
    CREATE address:addr2 SET street = '456 Oak Ave', city = 'New York', state = 'NY', zip = '10001';
    CREATE address:addr3 SET street = '789 Pine Rd', city = 'Austin', state = 'TX', zip = '73301';

    CREATE company:acme SET name = 'Acme Corp', industry = 'Technology', address = address:addr1;
    CREATE company:techco SET name = 'TechCo', industry = 'Software', address = address:addr2;

    CREATE person:john SET name = 'John Doe', age = 30, company = company:acme;
    CREATE person:jane SET name = 'Jane Smith', age = 28, company = company:techco;
    CREATE person:bob SET name = 'Bob Wilson', age = 35, company = company:acme;

    CREATE article:art1 SET title = 'Article One', author = person:john;
    CREATE article:art2 SET title = 'Article Two', author = person:jane;

    RETURN "OK";
};

-- 1: Basic FETCH
SELECT * FROM article ORDER BY id FETCH author;

-- 2: Nested FETCH with both author and author.company
SELECT * FROM article ORDER BY id FETCH author, author.company;
-- 3: Nested FETCH with both author.company and author in different orders -- should be same as 2
SELECT * FROM article ORDER BY id FETCH author.company, author;

-- 4, 5: Deeply nested FETCH -- different orders should be identical
SELECT * FROM article ORDER BY id FETCH author, author.company, author.company.address;
SELECT * FROM article ORDER BY id FETCH author.company.address, author, author.company;

-- 6: FETCH with non-recordid fields - this just fetches author.
SELECT * FROM article ORDER BY id FETCH author.name;
