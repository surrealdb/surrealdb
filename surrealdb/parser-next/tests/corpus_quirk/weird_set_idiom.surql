// I dont think any of these make sense.
CREATE foo SET ->a = 1; // set field "->a" to 1
CREATE foo SET a... = [1,2]; // no field set
CREATE foo SET a[WHERE false] = 1; // no field set
CREATE foo SET a[? TRUE] = 1; // no field set
CREATE foo SET a.{b} = 1; // no field set
CREATE foo SET a.{b: 1} = 1; // no field set
CREATE foo SET a->b = 1; // sets field "a" to `{ "->b" : 1 }`
CREATE foo SET a->b->c = 1; // sets field "a" to `{ "->b" : { "->c": 1 } }`
CREATE foo SET a<~b = 1; // sets field "a" to `{ "<~b" : 1 }`
CREATE foo SET a<~? = 1; // sets field "a" to `{ "<~?" : 1 }`
CREATE foo SET a<~(select foo from bar as baz) = 1; // sets field "a" to `{ "<~(select foo from bar as baz)" : 1 }`
CREATE foo SET a.@ = 1; // no field set
CREATE foo SET a.* = 1; // no field set
CREATE foo SET a.? = 1; // no field set
