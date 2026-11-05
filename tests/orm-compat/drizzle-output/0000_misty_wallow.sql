CREATE TABLE "CompatPost" (
	"id" integer PRIMARY KEY NOT NULL,
	"title" text NOT NULL,
	"authorId" integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE "CompatUser" (
	"id" integer PRIMARY KEY NOT NULL,
	"name" text NOT NULL,
	"age" integer NOT NULL,
	"active" boolean NOT NULL,
	"nickname" text
);
--> statement-breakpoint
ALTER TABLE "CompatPost" ADD CONSTRAINT "CompatPost_authorId_CompatUser_id_fk" FOREIGN KEY ("authorId") REFERENCES "public"."CompatUser"("id") ON DELETE no action ON UPDATE no action;