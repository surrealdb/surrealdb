/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A filter on an array of objects followed by a field and a second filter on the
array of objects that field holds.
"""
issue = 6702

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ recipients: { devices: {{{ delivered: false, delivered_at: NONE, id: '1', read: false, read_at: NONE },},} } }]"

[[test.results]]
value = "[{{{ delivered: false, delivered_at: NONE, id: '1', read: false, read_at: NONE },},}]"
*/
DEFINE TABLE IF NOT EXISTS mr SCHEMAFULL TYPE NORMAL;
DEFINE FIELD IF NOT EXISTS recipients ON TABLE mr TYPE set<object>;
DEFINE FIELD IF NOT EXISTS recipients.*.member ON TABLE mr TYPE bool;
DEFINE FIELD IF NOT EXISTS recipients.*.devices ON TABLE mr TYPE set<object>;
DEFINE FIELD IF NOT EXISTS recipients.*.devices.* ON TABLE mr TYPE object;
DEFINE FIELD IF NOT EXISTS recipients.*.devices.*.id ON TABLE mr TYPE string;
DEFINE FIELD IF NOT EXISTS recipients.*.devices.*.delivered ON TABLE mr TYPE option<bool> DEFAULT false;
DEFINE FIELD IF NOT EXISTS recipients.*.devices.*.delivered_at ON TABLE mr TYPE option<int>;
DEFINE FIELD IF NOT EXISTS recipients.*.devices.*.read ON TABLE mr TYPE option<bool> DEFAULT false;
DEFINE FIELD IF NOT EXISTS recipients.*.devices.*.read_at ON TABLE mr TYPE option<int>;
DEFINE FIELD IF NOT EXISTS recipients.*.delivered ON TABLE mr TYPE option<bool> DEFAULT false;
DEFINE FIELD IF NOT EXISTS recipients.*.read ON TABLE mr TYPE option<bool> DEFAULT false;
CREATE mr:1 SET recipients = [{member: true, devices: [{id: "1"}, {id: "2"}]}] RETURN NONE;
SELECT recipients[WHERE member = true].devices[WHERE id = "1"] FROM mr:1;
SELECT VALUE recipients[WHERE member = true].devices[WHERE id = "1"] FROM mr:1;
