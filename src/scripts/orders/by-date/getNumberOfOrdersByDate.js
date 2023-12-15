import { DruidHelper } from "../../../lib/druid.js";
import _ from "lodash";
import { WriteFile } from "../../../lib/write-file.js";

const druidHelper = new DruidHelper();

const druidQuery = {
  query: `
    SELECT __time  FROM "OrdersSales"
    WHERE "__time" >= '2023-4-01'
    AND "__time" < '2023-10-01'
    AND status = 'Completed'
    AND countryId = 'mSkwRgn6gt'
    AND EXTRACT(HOUR FROM "__time") = 18
`,
};

export async function getNumberOfOrdersByDate({ db }) {
  const druidRes = await druidHelper.fetchResultsSQL(druidQuery);

  const results = {
    numberOfOrders: 0,
    ordersPickedByInstaShop: 0,
    ordersPickedByMarketplace: 0,
  };

  for (const { instashopPicker } of druidRes) {
    results.numberOfOrders++;
    const orderWasPickedByInstaShop =
      instashopPicker === true || instashopPicker === "true";
    if (orderWasPickedByInstaShop) {
      results.ordersPickedByInstaShop++;
    } else {
      results.ordersPickedByMarketplace++;
    }
  }

  WriteFile.CSV(results, "2023_11_24_number-of-orders-by-date.csv");
  return results;
}
