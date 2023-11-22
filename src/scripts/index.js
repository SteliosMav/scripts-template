import { getCancelledOrdersByDateByClient } from "./orders/by-date/by-client/getCancelledOrdersByDateByClient.js";
import { getProductsByDateByAreas } from "./products/by-date/by-areas/getProductsByAreas.js";
import { getOrdersByDateByClient } from "./orders/by-date/by-client/getOrdersByDateByClient.js";
import { getSumOfDailyOrdersByDateByClientAndAvgOrdersPerDay } from "./orders/by-date/by-client/getSumOfDailyOrdersByDateByClientAndAvgOrdersPerDay.js";

// Here we import all scripts that we want to run
const scripts = [
  // getOrdersByDateByClient
  getSumOfDailyOrdersByDateByClientAndAvgOrdersPerDay,
];

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
