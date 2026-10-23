import "reflect-metadata";
import assert from "node:assert/strict";
import fs from "node:fs";
import pg from "pg";
import { PrismaClient } from "@prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { drizzle } from "drizzle-orm/node-postgres";
import {
	pgTable,
	integer,
	text,
	boolean,
	jsonb,
	numeric,
	timestamp,
	serial,
} from "drizzle-orm/pg-core";
import { eq, and, isNull, asc, sql } from "drizzle-orm";
import { DataSource, EntitySchema } from "typeorm";

const host = process.env.PGHOST || "127.0.0.1",
	port = Number(process.env.PGPORT || 15432);
const reference = process.env.REFERENCE === "1";
const results: any[] = [];
const queries: string[] = [];
const original = pg.Client.prototype.query;
pg.Client.prototype.query = function (...args: any[]) {
	const q = args[0];
	queries.push(typeof q === "string" ? q : q?.text || String(q));
	return original.apply(this, args as any);
} as any;
function config(name: string) {
	return {
		host,
		port,
		user: "root",
		password: reference ? "root" : undefined,
		database: reference ? "postgres" : `orm/${name}`,
		options: reference ? undefined : "-c dialect=pgsql",
		connectionTimeoutMillis: 4000,
		statement_timeout: 4000,
		max: 1,
	};
}
async function scenario(
	orm: string,
	name: string,
	fn: () => Promise<any>,
	setup?: () => Promise<void>,
) {
	const start = Date.now(),
		q = queries.length;
	try {
		if (setup) await setup();
		const setupEnd = queries.length;
		const detail = await fn();
		results.push({
			orm,
			name,
			status: "pass",
			ms: Date.now() - start,
			detail,
			sql: queries.slice(setupEnd),
		});
		console.log(`PASS ${orm}: ${name}`);
	} catch (e: any) {
		results.push({
			orm,
			name,
			status: "fail",
			ms: Date.now() - start,
			error: {
				name: e.name,
				code: e.code,
				message: e.message,
				cause: e.cause?.message,
				driver: e.driverError?.message,
			},
			sql: queries.slice(q),
		});
		console.log(
			`FAIL ${orm}: ${name}: ${e.code || ""} ${e.message.slice(0, 280)}`,
		);
	}
}
const users = pgTable("CompatUser", {
	id: integer("id").primaryKey(),
	name: text("name").notNull(),
	age: integer("age").notNull(),
	active: boolean("active").notNull(),
	nickname: text("nickname"),
});
const posts = pgTable("CompatPost", {
	id: integer("id").primaryKey(),
	title: text("title").notNull(),
	authorId: integer("authorId").notNull(),
});
const User = new EntitySchema({
	name: "CompatUser",
	tableName: "CompatUser",
	columns: {
		id: { type: Number, primary: true },
		name: { type: "text" },
		age: { type: Number },
		active: { type: Boolean },
		nickname: { type: "text", nullable: true },
	},
	relations: {
		posts: {
			type: "one-to-many",
			target: "CompatPost",
			inverseSide: "author",
		},
	},
});
const Post = new EntitySchema({
	name: "CompatPost",
	tableName: "CompatPost",
	columns: {
		id: { type: Number, primary: true },
		title: { type: "text" },
		authorId: { type: Number },
	},
	relations: {
		author: {
			type: "many-to-one",
			target: "CompatUser",
			joinColumn: { name: "authorId" },
		},
	},
});
const valuesTable = pgTable("CompatValue", {
	id: integer("id").primaryKey(),
	payload: jsonb("payload").notNull(),
	tags: text("tags").array().notNull(),
	amount: numeric("amount", { precision: 10, scale: 2 }).notNull(),
	createdAt: timestamp("createdAt", {
		withTimezone: true,
		precision: 3,
		mode: "date",
	}).notNull(),
});
const generatedTable = pgTable("CompatGenerated", {
	id: serial("id").primaryKey(),
	name: text("name").notNull(),
});
const Value = new EntitySchema({
	name: "CompatValue",
	tableName: "CompatValue",
	columns: {
		id: { type: Number, primary: true },
		payload: { type: "jsonb" },
		tags: { type: "text", array: true },
		amount: { type: "numeric", precision: 10, scale: 2 },
		createdAt: { type: "timestamptz", precision: 3 },
	},
});
const Generated = new EntitySchema({
	name: "CompatGenerated",
	tableName: "CompatGenerated",
	columns: {
		id: { type: Number, primary: true, generated: "increment" },
		name: { type: "text" },
	},
});
class Probe1700000000000 {
	name = "Probe1700000000000";
	async up(queryRunner: any) {
		await queryRunner.query(
			'CREATE TABLE "OrmMigration" (id integer PRIMARY KEY)',
		);
	}
	async down(queryRunner: any) {
		await queryRunner.query('DROP TABLE "OrmMigration"');
	}
}

for (const orm of ["prisma", "drizzle", "typeorm"]) {
	const pool = new pg.Pool(config(orm));
	let api: any;
	let connected = false;
	try {
		await scenario(orm, "connect and driver round-trip", async () => {
			const r = await pool.query("SELECT 1 AS n");
			assert.equal(Number(r.rows[0].n), 1);
			connected = true;
		});
		if (!connected) continue;
		await pool.query(
			'CREATE TABLE IF NOT EXISTS "CompatUser" (id integer PRIMARY KEY, name text NOT NULL, age integer NOT NULL, active boolean NOT NULL, nickname text)',
		);
		await pool.query(
			'CREATE TABLE IF NOT EXISTS "CompatPost" (id integer PRIMARY KEY, title text NOT NULL, "authorId" integer NOT NULL)',
		);
		const reset = async () => {
			await pool.query('DELETE FROM "CompatPost"');
			await pool.query('DELETE FROM "CompatUser"');
			await pool.query(
				`INSERT INTO "CompatUser" (id,name,age,active,nickname) VALUES (1,'Ada',42,true,NULL),(2,'Bob',30,false,'B')`,
			);
			await pool.query(
				`INSERT INTO "CompatPost" (id,title,"authorId") VALUES (1,'First',1),(2,'Second',1)`,
			);
		};
		let initialized = false;
		await scenario(orm, "ORM initialization", async () => {
			if (orm === "prisma") {
				api = new PrismaClient({ adapter: new PrismaPg(config(orm)) });
				await api.$connect();
			}
			if (orm === "drizzle") {
				api = drizzle(pool, { schema: { users, posts } });
				await api.execute(sql`select 1`);
			}
			if (orm === "typeorm") {
				api = new DataSource({
					type: "postgres",
					...config(orm),
					username: "root",
					entities: [User, Post, Value, Generated],
					migrations: [Probe1700000000000],
					synchronize: false,
					logging: false,
					extra: {
						options: reference ? undefined : "-c dialect=pgsql",
						statement_timeout: 4000,
						max: 1,
					},
				} as any);
				await api.initialize();
			}
			initialized = true;
		});
		if (!initialized) {
			for (const name of [
				"insert returning",
				"select scalar values",
				"filter/order/pagination",
				"update returning",
				"delete affected rows",
				"transaction commit",
				"transaction rollback",
				"relations",
				"aggregate/group",
				"upsert",
				"schema synchronization",
				"migration lifecycle",
				"schema introspection",
				"complex values",
				"generated ids",
				"duplicate primary key",
			])
				results.push({
					orm,
					name,
					status: "blocked",
					reason: "ORM initialization failed",
				});
			continue;
		}
		await scenario(
			orm,
			"insert returning",
			async () => {
				let r: any;
				const values = {
					id: 3,
					name: "Cy",
					age: 25,
					active: true,
					nickname: null,
				};
				if (orm === "prisma")
					r = await api.compatUser.create({ data: values });
				if (orm === "drizzle")
					r = (await api.insert(users).values(values).returning())[0];
				if (orm === "typeorm")
					r = (
						await api
							.getRepository("CompatUser")
							.createQueryBuilder()
							.insert()
							.values(values)
							.returning(["name", "id", "active", "age"])
							.execute()
					).raw[0];
				assert.equal(r.name, "Cy");
				assert.equal(Number(r.id), 3);
				assert.equal(r.active, true);
				assert.equal(Number(r.age), 25);
			},
			reset,
		);
		await scenario(
			orm,
			"select scalar values",
			async () => {
				let r: any;
				if (orm === "prisma")
					r = await api.compatUser.findUnique({ where: { id: 1 } });
				if (orm === "drizzle")
					r = (
						await api.select().from(users).where(eq(users.id, 1))
					)[0];
				if (orm === "typeorm")
					r = await api
						.getRepository("CompatUser")
						.findOneBy({ id: 1 });
				assert.equal(r.name, "Ada");
				assert.equal(r.active, true);
				assert.equal(r.nickname, null);
				assert.equal(r.age, 42);
			},
			reset,
		);
		await scenario(
			orm,
			"filter/order/pagination",
			async () => {
				let r: any;
				if (orm === "prisma")
					r = await api.compatUser.findMany({
						where: { age: { gte: 30 } },
						orderBy: { id: "desc" },
						skip: 1,
						take: 1,
					});
				if (orm === "drizzle")
					r = await api
						.select()
						.from(users)
						.where(sql`${users.age} >= ${30}`)
						.orderBy(sql`${users.id} desc`)
						.offset(1)
						.limit(1);
				if (orm === "typeorm")
					r = await api
						.getRepository("CompatUser")
						.createQueryBuilder("u")
						.where("u.age >= :age", { age: 30 })
						.orderBy("u.id", "DESC")
						.offset(1)
						.limit(1)
						.getMany();
				assert.equal(r.length, 1);
				assert.equal(Number(r[0].id), 1);
			},
			reset,
		);
		await scenario(
			orm,
			"update returning",
			async () => {
				let r: any;
				if (orm === "prisma")
					r = await api.compatUser.update({
						where: { id: 1 },
						data: { age: 43, nickname: "A" },
					});
				if (orm === "drizzle")
					r = (
						await api
							.update(users)
							.set({ age: 43, nickname: "A" })
							.where(eq(users.id, 1))
							.returning()
					)[0];
				if (orm === "typeorm")
					r = (
						await api
							.getRepository("CompatUser")
							.createQueryBuilder()
							.update()
							.set({ age: 43, nickname: "A" })
							.where("id = :id", { id: 1 })
							.returning(["nickname", "id", "age"])
							.execute()
					).raw[0];
				assert.equal(Number(r.age), 43);
				assert.equal(r.nickname, "A");
				assert.equal(Number(r.id), 1);
			},
			reset,
		);
		await scenario(
			orm,
			"delete affected rows",
			async () => {
				let n: any;
				if (orm === "prisma")
					n = (await api.compatUser.deleteMany({ where: { id: 2 } }))
						.count;
				if (orm === "drizzle")
					n = (await api.delete(users).where(eq(users.id, 2)))
						.rowCount;
				if (orm === "typeorm")
					n = (
						await api.getRepository("CompatUser").delete({ id: 2 })
					).affected;
				assert.equal(n, 1);
				const r = await pool.query(
					'SELECT id FROM "CompatUser" WHERE id = 2',
				);
				assert.equal(r.rows.length, 0);
			},
			reset,
		);
		await scenario(
			orm,
			"transaction commit",
			async () => {
				if (orm === "prisma")
					await api.$transaction(async (tx: any) => {
						await tx.compatUser.update({
							where: { id: 1 },
							data: { age: 50 },
						});
					});
				if (orm === "drizzle")
					await api.transaction(async (tx: any) => {
						await tx
							.update(users)
							.set({ age: 50 })
							.where(eq(users.id, 1));
					});
				if (orm === "typeorm")
					await api.transaction(async (tx: any) => {
						await tx
							.getRepository("CompatUser")
							.update({ id: 1 }, { age: 50 });
					});
				const r = await pool.query(
					'SELECT age FROM "CompatUser" WHERE id = 1',
				);
				assert.equal(Number(r.rows[0].age), 50);
			},
			reset,
		);
		await scenario(
			orm,
			"transaction rollback",
			async () => {
				let thrown = false;
				try {
					if (orm === "prisma")
						await api.$transaction(async (tx: any) => {
							await tx.compatUser.update({
								where: { id: 1 },
								data: { age: 99 },
							});
							throw new Error("expected rollback");
						});
					if (orm === "drizzle")
						await api.transaction(async (tx: any) => {
							await tx
								.update(users)
								.set({ age: 99 })
								.where(eq(users.id, 1));
							throw new Error("expected rollback");
						});
					if (orm === "typeorm")
						await api.transaction(async (tx: any) => {
							await tx
								.getRepository("CompatUser")
								.update({ id: 1 }, { age: 99 });
							throw new Error("expected rollback");
						});
				} catch (e: any) {
					assert.match(e.message, /expected rollback/);
					thrown = true;
				}
				assert.equal(thrown, true);
				const r = await pool.query(
					'SELECT age FROM "CompatUser" WHERE id = 1',
				);
				assert.equal(Number(r.rows[0].age), 42);
			},
			reset,
		);
		await scenario(
			orm,
			"relations",
			async () => {
				if (orm === "prisma") {
					const r = await api.compatUser.findUnique({
						where: { id: 1 },
						include: { posts: true },
					});
					assert.equal(r.posts.length, 2);
					assert.equal(r.posts[0].authorId, 1);
				}
				if (orm === "drizzle") {
					const r = await api
						.select({ name: users.name, title: posts.title })
						.from(users)
						.innerJoin(posts, eq(users.id, posts.authorId))
						.where(eq(users.id, 1))
						.orderBy(posts.id);
					assert.deepEqual(r, [
						{ name: "Ada", title: "First" },
						{ name: "Ada", title: "Second" },
					]);
				}
				if (orm === "typeorm") {
					const r = await api.getRepository("CompatUser").findOne({
						where: { id: 1 },
						relations: { posts: true },
					});
					assert.equal(r.posts.length, 2);
				}
			},
			reset,
		);
		await scenario(
			orm,
			"aggregate/group",
			async () => {
				let r: any;
				if (orm === "prisma") {
					r = await api.compatUser.groupBy({
						by: ["active"],
						_count: { _all: true },
						orderBy: { active: "asc" },
					});
					assert.equal(r.length, 2);
					assert.equal(r[0]._count._all, 1);
				}
				if (orm === "drizzle") {
					r = await api
						.select({ active: users.active, n: sql`count(*)` })
						.from(users)
						.groupBy(users.active)
						.orderBy(users.active);
					assert.equal(r.length, 2);
					assert.equal(Number(r[0].n), 1);
				}
				if (orm === "typeorm") {
					r = await api
						.getRepository("CompatUser")
						.createQueryBuilder("u")
						.select("u.active", "active")
						.addSelect("COUNT(*)", "n")
						.groupBy("u.active")
						.orderBy("u.active")
						.getRawMany();
					assert.equal(r.length, 2);
					assert.equal(Number(r[0].n), 1);
				}
			},
			reset,
		);
		await scenario(
			orm,
			"upsert",
			async () => {
				if (orm === "prisma")
					await api.compatUser.upsert({
						where: { id: 1 },
						create: { id: 1, name: "Ada", age: 55, active: true },
						update: { age: 55 },
					});
				if (orm === "drizzle")
					await api
						.insert(users)
						.values({ id: 1, name: "Ada", age: 55, active: true })
						.onConflictDoUpdate({
							target: users.id,
							set: { age: 55 },
						});
				if (orm === "typeorm")
					await api
						.getRepository("CompatUser")
						.upsert({ id: 1, name: "Ada", age: 55, active: true }, [
							"id",
						]);
				const r = await pool.query(
					'SELECT age FROM "CompatUser" WHERE id = 1',
				);
				assert.equal(Number(r.rows[0].age), 55);
			},
			reset,
		);
		await scenario(orm, "complex values", async () => {
			await pool.query(
				'CREATE TABLE IF NOT EXISTS "CompatValue" (id integer PRIMARY KEY,payload jsonb NOT NULL,tags text[] NOT NULL,amount numeric(10,2) NOT NULL,"createdAt" timestamptz(3) NOT NULL)',
			);
			await pool.query('DELETE FROM "CompatValue"');
			const data = {
				id: 1,
				payload: { nested: { ok: true }, n: 3 },
				tags: ["a", "b"],
				amount: "12.50",
				createdAt: new Date("2024-01-02T03:04:05.000Z"),
			};
			let r: any;
			if (orm === "prisma") r = await api.compatValue.create({ data });
			if (orm === "drizzle")
				r = (await api.insert(valuesTable).values(data).returning())[0];
			if (orm === "typeorm")
				r = (
					await api
						.getRepository("CompatValue")
						.createQueryBuilder()
						.insert()
						.values(data)
						.returning("*")
						.execute()
				).raw[0];
			assert.deepEqual(r.payload, data.payload);
			assert.deepEqual(r.tags, data.tags);
			assert.equal(Number(r.amount), 12.5);
			assert.equal(
				new Date(r.createdAt).toISOString(),
				data.createdAt.toISOString(),
			);
		});
		await scenario(orm, "generated ids", async () => {
			await pool.query(
				'CREATE TABLE IF NOT EXISTS "CompatGenerated" (id serial PRIMARY KEY,name text NOT NULL)',
			);
			let r: any;
			if (orm === "prisma")
				r = await api.compatGenerated.create({
					data: { name: "Generated" },
				});
			if (orm === "drizzle")
				r = (
					await api
						.insert(generatedTable)
						.values({ name: "Generated" })
						.returning()
				)[0];
			if (orm === "typeorm")
				r = (
					await api
						.getRepository("CompatGenerated")
						.createQueryBuilder()
						.insert()
						.values({ name: "Generated" })
						.returning("*")
						.execute()
				).raw[0];
			assert.equal(typeof r.id, "number");
			assert.ok(Number.isInteger(r.id) && r.id > 0);
		});
		await scenario(
			orm,
			"duplicate primary key",
			async () => {
				const data = {
					id: 1,
					name: "Duplicate",
					age: 10,
					active: true,
				};
				let rejected = false;
				try {
					if (orm === "prisma") await api.compatUser.create({ data });
					if (orm === "drizzle") await api.insert(users).values(data);
					if (orm === "typeorm")
						await api.getRepository("CompatUser").insert(data);
				} catch (e: any) {
					rejected = true;
					assert.ok(
						queries[queries.length - 1]
							.toLowerCase()
							.includes("insert"),
						"must reach database",
					);
				}
				assert.equal(rejected, true);
				const r = await pool.query(
					'SELECT name FROM "CompatUser" WHERE id=1',
				);
				assert.equal(r.rows[0].name, "Ada");
			},
			reset,
		);
		if (orm === "typeorm") {
			await scenario(orm, "schema introspection", async () => {
				const runner = api.createQueryRunner();
				try {
					const tables = await runner.getTables([
						"CompatUser",
						"CompatPost",
					]);
					assert.equal(tables.length, 2);
					assert.equal(
						tables[0].columns.some((c: any) => c.name === "id"),
						true,
					);
				} finally {
					await runner.release();
				}
			});
			await scenario(orm, "schema synchronization", async () => {
				await api.synchronize();
			});
			await scenario(orm, "migration lifecycle", async () => {
				const migrations = await api.runMigrations();
				assert.equal(migrations.length, 1);
				await api.undoLastMigration();
			});
		}
	} catch (e: any) {
		results.push({
			orm,
			name: "fixture/bootstrap",
			status: "fail",
			error: { message: e.message, code: e.code },
		});
		console.log(`BOOTSTRAP ${orm}: ${e.message}`);
	} finally {
		if (orm === "prisma" && api) await api.$disconnect().catch(() => {});
		if (orm === "typeorm" && api?.isInitialized)
			await api.destroy().catch(() => {});
		await pool.end().catch(() => {});
	}
}
fs.writeFileSync(
	process.env.RESULT_PATH || "results.json",
	JSON.stringify(
		{
			target: reference ? "postgres-reference" : "surrealdb",
			versions: JSON.parse(fs.readFileSync("package.json", "utf8"))
				.dependencies,
			results,
		},
		null,
		2,
	),
);
console.log(
	JSON.stringify(
		results.reduce((a: any, r: any) => {
			const x = (a[r.orm] ??= { pass: 0, fail: 0, blocked: 0 });
			x[r.status]++;
			return a;
		}, {}),
		null,
		2,
	),
);

process.stdout.write("", () =>
	process.exit(results.some((r) => r.status !== "pass") ? 1 : 0),
);
