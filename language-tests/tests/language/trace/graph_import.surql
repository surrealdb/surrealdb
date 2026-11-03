/**
[test]
reason = "Setup for the trace tests: people, places and posts joined by several edge tables, with one dead end."
run = false
*/

CREATE person:a SET name = 'Ann';
CREATE person:b SET name = 'Bob';
CREATE person:c SET name = 'Cat';
CREATE person:d SET name = 'Dan';
CREATE place:lib SET name = 'Library';
CREATE place:cafe SET name = 'Cafe';
CREATE post:p1, post:p2;
RELATE person:a->customer_of:a_lib->place:lib SET since = 2019;
RELATE person:a->customer_of:a_cafe->place:cafe SET since = 2023;
RELATE person:b->customer_of:b_lib->place:lib SET since = 2024;
RELATE person:a->works_at:a_lib->place:lib;
RELATE person:a->knows:ab->person:b;
RELATE person:b->knows:bc->person:c;
RELATE person:c->knows:cd->person:d;
RELATE person:a->knows:ac->person:c;
RELATE person:b->likes:b_p1->post:p1;
RELATE person:c->likes:c_p2->post:p2;
