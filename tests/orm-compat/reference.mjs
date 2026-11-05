import EmbeddedPostgres from "embedded-postgres";
import fs from "node:fs";
import { fileURLToPath } from "node:url";
const databaseDir = fileURLToPath(
	new URL("./.postgres-reference", import.meta.url),
);
const server = new EmbeddedPostgres({
	databaseDir,
	user: "root",
	password: "root",
	port: 15433,
	persistent: true,
});
if (!fs.existsSync(databaseDir + "/PG_VERSION")) await server.initialise();
await server.start();
process.on("SIGINT", async () => {
	await server.stop();
	process.exit(0);
});
process.on("SIGTERM", async () => {
	await server.stop();
	process.exit(0);
});
console.log("Reference PostgreSQL ready on 15433");
await new Promise(() => {});
