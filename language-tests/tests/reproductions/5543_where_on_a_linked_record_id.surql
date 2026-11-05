/**
[test]
reason = "Comparing a relation endpoint's record-link field with a record id gives the same answer as comparing the linked record's own id, for a negative integer key and for a string key. The report concerned data imported from 1.x into 2.x, which 4.0 does not read; data written by 4.0 behaves as the report expected."
issue = 5543

[[test.results]]
value = "[{ chat: chat:-4212782653, id: foo:1 }]"

[[test.results]]
value = "[{ id: chat:-4212782653 }]"

[[test.results]]
value = "[{ id: split:1, in: foo:1, out: foo:1 }]"

[[test.results]]
value = "[{ in: foo:1 }]"

[[test.results]]
value = "[{ in: foo:1 }]"

[[test.results]]
value = "[foo:1]"

[[test.results]]
value = "[{ chat: chat:⟨-42⟩, id: foo:2 }]"

[[test.results]]
value = "[{ id: chat:⟨-42⟩ }]"

[[test.results]]
value = "[{ id: split:2, in: foo:2, out: foo:2 }]"

[[test.results]]
value = "[{ in: foo:2 }]"

[[test.results]]
value = "[{ in: foo:2 }]"

[[test.results]]
value = "[]"

*/

CREATE foo:1 SET chat = chat:-4212782653;
CREATE chat:-4212782653;
RELATE foo:1->split:1->foo:1;
SELECT in FROM split WHERE in.chat = chat:-4212782653;
SELECT in FROM split WHERE in.chat.id = chat:-4212782653;
(DELETE split WHERE in.chat = chat:-4212782653 RETURN BEFORE).in;
CREATE foo:2 SET chat = chat:⟨-42⟩;
CREATE chat:⟨-42⟩;
RELATE foo:2->split:2->foo:2;
SELECT in FROM split WHERE in.chat = chat:⟨-42⟩;
SELECT in FROM split WHERE in.chat.id = chat:⟨-42⟩;
-- A string key is not the integer key of the same digits.
SELECT in FROM split WHERE in.chat = chat:-42;
