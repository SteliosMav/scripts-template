import "dotenv/config";
import { runFunctions } from "./scripts/index.js";
import { connectToDb, closeConnection } from "./lib/mongodb.js";
import { DruidHelper } from "./lib/druid.js";

async function main() {
  // Connect to the server
  const db = await connectToDb();

  const druidHelper = new DruidHelper();

  // Initialize all scripts
  await runFunctions({ db, druidHelper });

  return "## Done. \n";
}

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => closeConnection());
