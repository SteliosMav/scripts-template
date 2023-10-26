import { getUsers } from "./getUsers.js";

// Here we import all functions that we want to run
const functions = [getUsers, getUsers];

export async function runFunctions({ db }) {
  for (let i = 0; i < functions.length; i++) {
    const fn = functions[i];
    const fnName = fn.name || "Unknown";
    const isLastFn = i === functions.length - 1;
    console.log(`
----------------------------------------
    
## Running ${fnName}...`);
    const fnResponse = await fn({ db });
    console.log(`${fnName} response: `, fnResponse);
    console.log(`${fnName} done.`);
    if (isLastFn)
      console.log(`
----------------------------------------
`);
  }
}
