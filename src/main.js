import "dotenv/config";
import MongoPackage from "mongodb";
import { MODULES } from "./modules/index.js";

const MongoClient = MongoPackage.MongoClient;

// Connection URL
const url = process.env.DB_URI;
const client = new MongoClient(url, { useUnifiedTopology: true });

async function main() {
  // Connect to the server
  await client.connect();
  console.log("Connected successfully to server");
  const dbName = process.env.DB_NAME;
  const db = client.db(dbName);

  // Run all scripts sequentially
  MODULES.forEach((Module) => new Module({ db }).init());

  return "\n ## Done. \n";
}

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => client.close());
