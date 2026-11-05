CREATE a;
CREATE a, b;
CREATE ONLY a, b;
CREATE ONLY a, b SET a = 1, b.c += 2, d[1] -= 3, e +?= 4;
// TODO: Remove in CREATE?
CREATE ONLY a, b UNSET a, b.c, d[1];
CREATE ONLY a, b CONTENT { a: 1 };
CREATE ONLY a, b PATCH { a: 1 };
CREATE ONLY a, b MERGE { a: 1 };
CREATE ONLY a, b REPLACE { a: 1 };
CREATE ONLY a, b REPLACE { a: 1 } VERSION 1;
CREATE ONLY a, b REPLACE { a: 1 } VERSION 1 TIMEOUT 1;
