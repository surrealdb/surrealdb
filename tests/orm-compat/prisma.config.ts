import { defineConfig } from "prisma/config";
export default defineConfig({
	schema: "schema.prisma",
	datasource: { url: process.env.DATABASE_URL! },
	migrations: { path: "prisma-migrations" },
});
