import { pgTable, integer, text, boolean } from "drizzle-orm/pg-core";
export const users = pgTable("CompatUser", {
	id: integer("id").primaryKey(),
	name: text("name").notNull(),
	age: integer("age").notNull(),
	active: boolean("active").notNull(),
	nickname: text("nickname"),
});
export const posts = pgTable("CompatPost", {
	id: integer("id").primaryKey(),
	title: text("title").notNull(),
	authorId: integer("authorId")
		.notNull()
		.references(() => users.id),
});
