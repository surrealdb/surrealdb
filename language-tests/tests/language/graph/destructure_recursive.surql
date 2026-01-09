/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test recursive destructuring with .@ for building nested structures"

[[test.results]]
value = "{ name: 'Alice Zhang', reports_to: [{ name: 'Priya Patel', reports_to: [{ name: 'Sarah Kim', reports_to: [{ name: 'Marcus Chen', reports_to: [{ name: 'Elena Rodriguez', reports_to: [] }] }] }] }] }"

[[test.results]]
value = "{ name: 'Alice Zhang', reports_to: [{ name: 'Priya Patel', reports_to: [person:dir_platform] }] }"

[[test.results]]
value = "{ id: person:ceo, name: 'Elena Rodriguez', reports: [{ id: person:vp_eng, name: 'Marcus Chen', reports: [{ id: person:dir_platform, name: 'Sarah Kim', reports: [{ id: person:lead_infra, name: 'Priya Patel', reports: [{ id: person:alice, name: 'Alice Zhang', reports: [], title: 'Senior Infrastructure Engineer' }, { id: person:bob, name: 'Bob Martinez', reports: [], title: 'Infrastructure Engineer' }], title: 'Infrastructure Lead' }], title: 'Director of Platform' }, { id: person:dir_product, name: 'James Wilson', reports: [{ id: person:lead_frontend, name: 'Alex Thompson', reports: [{ id: person:charlie, name: 'Charlie Brown', reports: [], title: 'Senior Frontend Engineer' }, { id: person:dana, name: 'Dana Lee', reports: [], title: 'Frontend Engineer' }], title: 'Frontend Lead' }], title: 'Director of Product Engineering' }], title: 'VP Engineering' }], title: 'CEO' }"

[[test.results]]
value = "{ mentees: [{ mentees: [{ mentees: [], name: 'Bob Martinez' }], name: 'Alice Zhang' }], name: 'Priya Patel' }"

[[test.results]]
value = "{ name: 'Acme Corp', sub_orgs: [{ name: 'Engineering', sub_orgs: [{ name: 'Platform', sub_orgs: [{ name: 'Infrastructure', sub_orgs: [] }] }, { name: 'Product Engineering', sub_orgs: [] }] }] }"

[[test.results]]
value = "{ name: 'Technology', subcategories: [{ name: 'Backend', subcategories: [{ name: 'Databases', subcategories: [{ name: 'Relational', subcategories: [{ name: 'SQL', subcategories: [{ name: 'Optimization', subcategories: [{ name: 'Indexing', subcategories: [{ name: 'B-Tree', subcategories: [] }] }] }] }] }] }] }, { name: 'Frontend', subcategories: [] }] }"
*/

-- Build nested connection trees - reports_to chain
person:alice.{..}.{
    name,
    reports_to: ->reports_to->person.@
};

-- Limited depth recursive destructuring
person:alice.{..2}.{
    name,
    reports_to: ->reports_to->person.@
};

-- Build management tree from CEO downward
person:ceo.{..5}.{
    id,
    name,
    title,
    reports: <-reports_to<-person.@
};

-- Build mentorship tree
person:lead_infra.{..3}.{
    name,
    mentees: ->mentors->person.@
};

-- Organization tree from company root
org:company.{..}.{
    name,
    sub_orgs: children.@
};

-- Category tree
category:tech.{..}.{
    name,
    subcategories: children.@
};

