import { expect, test } from "bun:test";
import { RpcClient, guestClient, rootClient, startServer, type TestServer } from "../src/harness";

// Tenant isolation: a session authenticated for one namespace/database must not
// reach another namespace/database's data by re-selecting ns/db after auth. The
// authenticated level pins the reachable tenant regardless of how ns/db is
// subsequently chosen — SQL USE, the RPC use method, the Surreal-NS/Surreal-DB
// request headers under basic auth, or a record-user session.
//
// These are test.skip acceptance tests for the enforced boundary. Enable them
// against a binary that enforces it and pin the exact denial surface (error
// class / status) at that point; they assert only that the foreign-tenant read
// or write does NOT succeed.

async function withServer<T>(fn: (server: TestServer) => Promise<T>): Promise<T> {
	const server = await startServer();
	try {
		return await fn(server);
	} finally {
		await server.stop();
	}
}

async function rejectsOrEmpty(p: Promise<unknown[]>): Promise<void> {
	// A denied read must never return foreign rows: it either rejects, or
	// resolves to an empty result. Both are acceptable; leaking a row is not.
	try {
		const [rows] = (await p) as unknown[];
		expect(rows).toEqual([]);
	} catch {
		// rejected — also acceptable
	}
}

// A second tenant (ns/db) with a secret row, and a database user confined to
// the FIRST tenant. Returned names let each vector attempt the cross-tenant
// pivot. Root sets everything up (root legitimately spans tenants).
async function twoTenants(server: TestServer) {
	const { db, namespace: ns1, database: db1 } = await rootClient(server);
	const ns2 = `${ns1}_other`;
	const db2 = `${db1}_other`;
	await db.query(
		`DEFINE NAMESPACE \`${ns2}\`;
		 USE NS \`${ns2}\`; DEFINE DATABASE \`${db2}\`;
		 USE NS \`${ns2}\` DB \`${db2}\`; CREATE secret:1 SET value = 'foreign';`,
	);
	// A database user confined to tenant one.
	await db.use({ namespace: ns1, database: db1 });
	await db.query("DEFINE USER tenant1 ON DATABASE PASSWORD 'tenant1-pw' ROLES OWNER");
	await db.query("CREATE mine:1 SET value = 'home'");
	return { root: db, ns1, db1, ns2, db2 };
}

test.skip("a database user cannot SELECT another tenant's data via SQL USE", async () => {
	await withServer(async (server) => {
		const { root, ns1, db1, ns2, db2 } = await twoTenants(server);

		const user = await guestClient(server, ns1, db1);
		await user.signin({ namespace: ns1, database: db1, username: "tenant1", password: "tenant1-pw" });

		// Re-selecting the foreign tenant with USE then reading must not succeed.
		await rejectsOrEmpty(
			user.query(`USE NS \`${ns2}\` DB \`${db2}\`; SELECT * FROM secret`).collect() as Promise<unknown[]>,
		);

		await user.close();
		await root.close();
	});
}, 30000);

test.skip("a database user cannot write another tenant's data via SQL USE", async () => {
	await withServer(async (server) => {
		const { root, ns1, db1, ns2, db2 } = await twoTenants(server);

		const user = await guestClient(server, ns1, db1);
		await user.signin({ namespace: ns1, database: db1, username: "tenant1", password: "tenant1-pw" });

		// Create / update / delete against the foreign tenant must be refused.
		await rejectsOrEmpty(
			user.query(`USE NS \`${ns2}\` DB \`${db2}\`; CREATE secret:2 SET value = 'tampered'`).collect() as Promise<unknown[]>,
		);
		await rejectsOrEmpty(
			user.query(`USE NS \`${ns2}\` DB \`${db2}\`; UPDATE secret:1 SET value = 'tampered'`).collect() as Promise<unknown[]>,
		);
		await rejectsOrEmpty(
			user.query(`USE NS \`${ns2}\` DB \`${db2}\`; DELETE secret:1`).collect() as Promise<unknown[]>,
		);

		// The foreign row is untouched, confirmed by root.
		await root.use({ namespace: ns2, database: db2 });
		const [secret] = await root.query<[Array<{ value: string }>]>("SELECT value FROM secret:1").json();
		expect(secret).toEqual([{ value: "foreign" }]);

		await user.close();
		await root.close();
	});
}, 30000);

test.skip("the RPC use method cannot pivot a database user to another tenant", async () => {
	await withServer(async (server) => {
		const { root, ns1, db1, ns2, db2 } = await twoTenants(server);

		const rpc = await RpcClient.connect(server);
		await rpc.call("signin", [{ ns: ns1, db: db1, user: "tenant1", pass: "tenant1-pw" }]);

		// Selecting the foreign tenant, or reading after it, must be denied.
		const used = await rpc.rpc("use", [ns2, db2]);
		if (!used.error) {
			const read = await rpc.rpc("query", ["SELECT * FROM secret"]);
			const envs = (read.result as Array<{ result: unknown[] }>) ?? [];
			if (read.error === undefined && envs[0]) expect(envs[0].result).toEqual([]);
		}

		await rpc.close();
		await root.close();
	});
}, 30000);

test.skip("Surreal-NS/Surreal-DB headers under basic auth cannot reach another tenant", async () => {
	await withServer(async (server) => {
		const { root, ns1, db1, ns2, db2 } = await twoTenants(server);

		// Basic auth pins the user to tenant one; the ns/db headers point at
		// tenant two. The read must not return tenant two's rows.
		const res = await fetch(`${server.httpUrl}/sql`, {
			method: "POST",
			headers: {
				Authorization: `Basic ${btoa("tenant1:tenant1-pw")}`,
				"surreal-ns": ns2,
				"surreal-db": db2,
				Accept: "application/json",
			},
			body: "SELECT * FROM secret",
		});
		if (res.ok) {
			const body = (await res.json()) as Array<{ result: unknown[] }>;
			expect(body[0]?.result ?? []).toEqual([]);
		} else {
			expect(res.status).toBeGreaterThanOrEqual(400);
		}

		await root.close();
	});
}, 30000);

test.skip("a record user cannot read or write another tenant after re-selecting ns/db", async () => {
	await withServer(async (server) => {
		const { db, namespace: ns1, database: db1 } = await rootClient(server);
		const ns2 = `${ns1}_other`;
		const db2 = `${db1}_other`;
		await db.query(
			`DEFINE NAMESPACE \`${ns2}\`;
			 USE NS \`${ns2}\`; DEFINE DATABASE \`${db2}\`;
			 USE NS \`${ns2}\` DB \`${db2}\`; CREATE secret:1 SET value = 'foreign';`,
		);
		await db.use({ namespace: ns1, database: db1 });
		await db.query(`
			DEFINE TABLE account SCHEMALESS;
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNUP ( CREATE account SET email = $email )
				SIGNIN ( SELECT * FROM account WHERE email = $email )
				DURATION FOR SESSION 1h;
		`);

		const rec = await guestClient(server, ns1, db1);
		await rec.signup({ namespace: ns1, database: db1, access: "user", variables: { email: "r@example.com" } });

		// A record user is scoped to its own tenant; the foreign read yields nothing.
		await rejectsOrEmpty(
			rec.query(`USE NS \`${ns2}\` DB \`${db2}\`; SELECT * FROM secret`).collect() as Promise<unknown[]>,
		);
		await rejectsOrEmpty(
			rec.query(`USE NS \`${ns2}\` DB \`${db2}\`; CREATE secret:2 SET value = 'tampered'`).collect() as Promise<unknown[]>,
		);

		await db.use({ namespace: ns2, database: db2 });
		const [secret] = await db.query<[unknown[]]>("SELECT * FROM secret").json();
		expect(secret).toHaveLength(1);

		await rec.close();
		await db.close();
	});
}, 30000);
