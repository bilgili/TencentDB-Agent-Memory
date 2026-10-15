import { defineConfig } from "drizzle-kit";

export default defineConfig({
  dialect: "sqlite",
  schema: "./src/db/schema.sqlite.ts",
  out: "./drizzle",
  dbCredentials: {
    url: process.env.KNOWLEDGE_DB_PATH || "./data/knowledge.db",
  },
});
