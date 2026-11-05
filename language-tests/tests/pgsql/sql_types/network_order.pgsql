/**
[env]
namespace = true
database = true
imports = ["pgsql/sql_types/_hosts.surql"]

[test]
reason = "inet and cidr columns order and compare by address as PostgreSQL does: family, then the bits both prefixes cover, then prefix length, then the whole address; a literal compared for equality is read as the network it spells"

[[test.results]]
value = "[{ addr: '10.0.0.2', id: 1, net: '10.0.0.0/8' }, { addr: '9.255.255.255', id: 2, net: '192.168.0.0/16' }, { addr: '10.0.0.0/16', id: 3, net: '10.1.0.0/16' }, { addr: '::1', id: 4, net: '2001:db8::/32' }, { addr: '100.0.0.1', id: 5, net: '9.0.0.0/8' }, { addr: '10.0.0.10', id: 6, net: '10.0.0.0/16' }]"

[[test.results]]
value = "[{ addr: '9.255.255.255', id: 2 }, { addr: '10.0.0.0/16', id: 3 }, { addr: '10.0.0.2', id: 1 }, { addr: '10.0.0.10', id: 6 }, { addr: '100.0.0.1', id: 5 }, { addr: '::1', id: 4 }]"

[[test.results]]
value = "[{ id: 4, net: '2001:db8::/32' }, { id: 2, net: '192.168.0.0/16' }, { id: 3, net: '10.1.0.0/16' }, { id: 6, net: '10.0.0.0/16' }, { id: 1, net: '10.0.0.0/8' }, { id: 5, net: '9.0.0.0/8' }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }, { id: 3 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 3 }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 6 }]"

[[test.results]]
value = "[{ id: 4 }]"

[[test.results]]
value = "[{ addr: '192.168.1.5/32', net: '10.0.0.0/8' }]"
*/

INSERT INTO host (id, addr, net) VALUES
	(1, '10.0.0.2', '10.0.0.0/8'),
	(2, '9.255.255.255', '192.168.0.0/16'),
	(3, '10.0.0.0/16', '10.1.0.0/16'),
	(4, '::1', '2001:db8::/32'),
	(5, '100.0.0.1', '9.0.0.0/8'),
	(6, '10.0.0.10', '10.0.0.0/16');

SELECT id, addr FROM host ORDER BY addr;

SELECT id, net FROM host ORDER BY net DESC;

SELECT id FROM host WHERE addr < '10.0.0.3' ORDER BY id;

SELECT id FROM host WHERE addr BETWEEN '10.0.0.0/16' AND '10.0.0.9' ORDER BY id;

SELECT id FROM host WHERE addr = '10.0.0.2/32';

SELECT id FROM host WHERE net = '10.0.0.0/16'::cidr;

SELECT id FROM host WHERE addr >= '::' ORDER BY id;

SELECT '192.168.1.5/32'::inet::text AS addr, '10.0.0.0/8'::cidr::text AS net;
