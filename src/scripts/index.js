import { getUsersByProductCategory } from "./users/by-product-category/getUsersByProductCategory.js";
import { getOrdersByDate } from "./orders/by-date/getOrdersByDate.js";

// Here we import all scripts that we want to run
const scripts = [getOrdersByDate];

export async function runFunctions({ db }) {
  for (let i = 0; i < scripts.length; i++) {
    const fn = scripts[i];
    const fnName = fn.name || "Unknown";
    const isLastFn = i === scripts.length - 1;
    console.log(`
----------------------------------------
    
## Running ${fnName}...
`);
    const fnResponse = await fn({ db });
    if (fnResponse) console.log(`${fnName} response: `, fnResponse);
    console.log(`
${fnName} done.`);
    if (isLastFn)
      console.log(`
----------------------------------------
`);
  }
}
