import Parse from "parse/node.js";

export async function initializeParse() {
  Parse.initialize(
    process.env.APP_ID,
    process.env.JAVASCRIPT_KEY,
    process.env.MASTER_KEY,
  );
  Parse.serverURL = process.env.SERVER_URL;

  return Parse;
}
