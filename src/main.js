import "dotenv/config";
import { runFunctions } from "./functions/index.js";
import { connectToDb, closeConnection } from "./lib/mongodb.js";

async function main() {
  // Connect to the server
  const db = await connectToDb();

  // Initialize all functions
  await runFunctions({ db });

  return "## Done. \n";
}

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => closeConnection());
