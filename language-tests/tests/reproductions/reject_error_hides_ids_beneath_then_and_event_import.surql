/**
[test]
reason = """
	Setup for reject_error_hides_ids_beneath_then_and_event. Two definer-authored
	frames that suppress enforcement and run a nested DELETE that an ON DELETE
	REJECT refuses, both reached by a record user who can SELECT neither the
	deleted `item` nor the referencing `lock`:

	THEN: `item` references an `asset` ON DELETE THEN { DELETE $this }, so
	deleting the asset deletes the hidden item, which the hidden lock rejects.

	Event: deleting a `widget` fires an event that deletes the hidden
	`item:evt`, which the hidden `lock:evtlock` rejects.
"""
run = false
*/

DEFINE TABLE asset SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE item SCHEMALESS PERMISSIONS FOR select, create, update NONE, FOR delete FULL;
DEFINE FIELD owner ON item TYPE record<asset> REFERENCE ON DELETE THEN { DELETE $this };
DEFINE TABLE lock SCHEMALESS PERMISSIONS FOR select, create, update, delete NONE;
DEFINE FIELD target ON lock TYPE record<item> REFERENCE ON DELETE REJECT;

DEFINE TABLE widget SCHEMALESS PERMISSIONS FULL;
DEFINE EVENT cleanup ON widget WHEN $event = "DELETE" THEN { DELETE item:evt };

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

CREATE user:alice;

CREATE asset:a;
CREATE item:hiddenitem SET owner = asset:a;
CREATE lock:hiddenlock SET target = item:hiddenitem;

CREATE asset:b;
CREATE item:evt SET owner = asset:b;
CREATE lock:evtlock SET target = item:evt;
CREATE widget:w;
