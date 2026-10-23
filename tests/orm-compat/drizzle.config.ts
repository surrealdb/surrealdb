import { defineConfig } from "drizzle-kit";
export default defineConfig({
	dialect: "postgresql",
	schema: "./drizzle-schema.ts",
	out: process.env.DRIZZLE_OUT || "./drizzle-output",
	dbCredentials: { url: process.env.DATABASE_URL! },
});
