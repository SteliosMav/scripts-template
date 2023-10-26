import "dotenv/config";
import { initModules } from "./modules/index.js";
import { connectToDb, closeConnection } from "./lib/mongodb.js";

async function main() {
  // Connect to the server
  const db = await connectToDb();

  // Initialize all modules
  await initModules({ db });

  return "## Done. \n";
}

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => closeConnection());
