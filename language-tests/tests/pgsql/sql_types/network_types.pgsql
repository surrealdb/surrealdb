/**
[env]
namespace = true
database = true

[test]
reason = "inet, cidr, macaddr and macaddr8 columns store canonical text, a cast from text reads by the type's own reader, and a cast back to text prints the canonical form"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ addr: '192.168.1.5', id: 1, mac: '08:00:2b:01:02:03', mac8: '08:00:2b:01:02:03:04:05', net: '10.0.0.0/8' }, { addr: '10.0.0.1', id: 2, mac: '08:00:2b:01:02:04', mac8: '08:00:2b:01:02:03:04:06', net: '192.168.0.0/16' }]"

[[test.results]]
value = "[{ addr: '192.168.1.5', id: 1, mac: '08:00:2b:01:02:03', mac8: '08:00:2b:01:02:03:04:05', net: '10.0.0.0/8' }, { addr: '10.0.0.1', id: 2, mac: '08:00:2b:01:02:04', mac8: '08:00:2b:01:02:03:04:06', net: '192.168.0.0/16' }]"

[[test.results]]
value = "[{ addr: '192.168.1.5', mac: '08:00:2b:01:02:03' }]"

[[test.results]]
value = "[{ addr: '192.168.1.5/32' }]"

[[test.results]]
value = "[{ id: 1 }]"

*/

CREATE TABLE host (id int PRIMARY KEY, addr inet, net cidr, mac macaddr, mac8 macaddr8);

INSERT INTO host (id, addr, net, mac, mac8) VALUES
	(1, '192.168.1.5/32', '10.0.0.0/8', '08-00-2B-01-02-03', '08002b0102030405'),
	(2, '10.0.0.1', '192.168.0.0/16', '0800.2b01.0204', '08:00:2b:01:02:03:04:06');

SELECT id, addr, net, mac, mac8 FROM host ORDER BY id;

SELECT '08-00-2B-01-02-03'::macaddr AS mac, '192.168.1.5/32'::inet AS addr;

SELECT '192.168.1.5/32'::inet::text AS addr;

SELECT id FROM host WHERE mac = '08-00-2b-01-02-03'::macaddr;
