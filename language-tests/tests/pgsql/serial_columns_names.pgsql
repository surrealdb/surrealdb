/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL SERIAL columns of different tables count apart even when the table and column names run together alike (serial_x_y with z, serial_x with y_z)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ z: 1i32 }, { z: 2i32 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ y_z: 1i32 }]"

[[test.results]]
value = "[{ z: 3i32 }]"
*/

CREATE TABLE serial_x_y (z SERIAL, name TEXT);

INSERT INTO serial_x_y (name) VALUES ('a'), ('b') RETURNING z;

CREATE TABLE serial_x (y_z SERIAL, name TEXT);

INSERT INTO serial_x (name) VALUES ('c') RETURNING y_z;

INSERT INTO serial_x_y (name) VALUES ('d') RETURNING z;
