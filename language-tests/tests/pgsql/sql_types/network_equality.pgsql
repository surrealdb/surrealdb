/**
[env]
namespace = true
database = true
imports = ["pgsql/sql_types/_hosts.surql"]

[test]
reason = "an inet and a cidr spelling the same network compare equal, as PostgreSQL compares their network values"

[[test.results]]
value = "[{ addr: '10.0.0.2', id: 1, net: '10.0.0.2/32' }, { addr: '10.0.0.0/16', id: 2, net: '10.0.0.0/16' }, { addr: '10.0.0.3', id: 3, net: '10.0.0.0/8' }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[{ id: 3 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[{ id: 3 }]"
*/

INSERT INTO host (id, addr, net) VALUES (1, '10.0.0.2', '10.0.0.2/32'), (2, '10.0.0.0/16', '10.0.0.0/16'), (3, '10.0.0.3', '10.0.0.0/8');

SELECT id FROM host WHERE addr = net ORDER BY id;

SELECT id FROM host WHERE addr <> net ORDER BY id;

SELECT id FROM host WHERE addr IN ('10.0.0.2/32', '10.0.0.0/16') ORDER BY id;

SELECT id FROM host WHERE addr IN (net, '192.168.0.1') ORDER BY id;

SELECT id FROM host WHERE addr NOT IN ('10.0.0.2/32', '10.0.0.0/16') ORDER BY id;
