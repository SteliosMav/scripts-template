import "dotenv/config";
import { runFunctions } from "./scripts/scripts.js";
import { connectToDb, closeConnection } from "./lib/mongodb.js";
import { DruidHelper } from "./lib/druid.js";
import { initializeParse } from "./lib/parse.js";

async function main() {
  // Connect to the MongoDB server
  const db = await connectToDb();

  // Initialize Parse
  const Parse = await initializeParse();

  // Initialize the Druid helper
  const druidHelper = new DruidHelper();

  // Initialize all scripts
  await runFunctions({ db, druidHelper, Parse });

  return "## Done. \n";
}

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => closeConnection());
