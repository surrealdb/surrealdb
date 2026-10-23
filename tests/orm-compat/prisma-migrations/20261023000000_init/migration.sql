-- CreateSchema
CREATE SCHEMA IF NOT EXISTS "public";

-- CreateTable
CREATE TABLE "CompatUser" (
    "id" INTEGER NOT NULL,
    "name" TEXT NOT NULL,
    "age" INTEGER NOT NULL,
    "active" BOOLEAN NOT NULL,
    "nickname" TEXT,

    CONSTRAINT "CompatUser_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "CompatPost" (
    "id" INTEGER NOT NULL,
    "title" TEXT NOT NULL,
    "authorId" INTEGER NOT NULL,

    CONSTRAINT "CompatPost_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "CompatValue" (
    "id" INTEGER NOT NULL,
    "payload" JSONB NOT NULL,
    "tags" TEXT[],
    "amount" DECIMAL(10,2) NOT NULL,
    "createdAt" TIMESTAMPTZ(3) NOT NULL,

    CONSTRAINT "CompatValue_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "CompatGenerated" (
    "id" SERIAL NOT NULL,
    "name" TEXT NOT NULL,

    CONSTRAINT "CompatGenerated_pkey" PRIMARY KEY ("id")
);

-- AddForeignKey
ALTER TABLE "CompatPost" ADD CONSTRAINT "CompatPost_authorId_fkey" FOREIGN KEY ("authorId") REFERENCES "CompatUser"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
