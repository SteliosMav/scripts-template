import { DruidHelper } from "../../../lib/druid.js";
import _ from "lodash";

const druidHelper = new DruidHelper();

const druidQuery = {
  query: `
            SELECT orderId, amountPayableInternal FROM "OrdersSales"
            WHERE status ='Completed'
            AND __time >= '2023-10-01' AND __time < '2023-11-01'
            AND countryId = 'ryFmc6ACd1'
            AND testClient !='true'
            LIMIT 1000
`,
};

export async function getOrdersByDate({ db }) {
  console.time("druid");
  const druidRes = await druidHelper.fetchResultsSQL(druidQuery);
  console.timeEnd("druid");

  return druidRes;
}
