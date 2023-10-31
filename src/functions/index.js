import { getUsers } from "./getUsers.js";

// Here we import all functions that we want to run
const scripts = [getUsers, getUsers];

export async function runFunctions({ db }) {
  for (let i = 0; i < scripts.length; i++) {
    const fn = scripts[i];
    const fnName = fn.name || "Unknown";
    const isLastFn = i === scripts.length - 1;
    console.log(`
----------------------------------------
    
## Running ${fnName}...`);
    const fnResponse = await fn({ db });
    if (fnResponse) console.log(`${fnName} response: `, fnResponse);
    console.log(`${fnName} done.`);
    if (isLastFn)
      console.log(`
----------------------------------------
`);
  }
}
