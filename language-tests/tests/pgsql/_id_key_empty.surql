/**
[test]
run = false
*/

DEFINE TABLE owner SCHEMALESS;
CREATE owner:1 SET name = 'one' RETURN NONE;
CREATE owner:2 SET name = 'two' RETURN NONE;
CREATE owner:⟨1.5⟩ SET name = 'text key that looks like a float' RETURN NONE;

DEFINE TABLE pet SCHEMALESS;
CREATE pet:1 SET label = 'plain key', owner = 1 RETURN NONE;
CREATE pet:2 SET label = 'empty key', owner = '' RETURN NONE;
CREATE pet:3 SET label = 'record link', owner = owner:2 RETURN NONE;
CREATE pet:4 SET label = 'null key', owner = NULL RETURN NONE;
CREATE pet:5 SET label = 'no key' RETURN NONE;
CREATE pet:6 SET label = 'float key', owner = 1.5 RETURN NONE;
CREATE pet:7 SET label = 'bool key', owner = true RETURN NONE;
