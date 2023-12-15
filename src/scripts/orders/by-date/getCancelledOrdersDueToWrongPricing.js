import { DruidHelper } from "../../../lib/druid.js";
import _ from "lodash";
import { WriteFile } from "../../../lib/write-file.js";

const druidHelper = new DruidHelper();

const egyptSId = "vrii93Zwoj";
const pricingIssuesReasonId = "Yrv3dRA83v";
const pharmacyBusinessTypeId = "X40sZxFRi5";

const cancelledOrdersQuery = {
  query: `
    SELECT orderId  
    FROM "OrdersSales"
    WHERE "__time" >= '2023-10-01' AND "__time" < '2023-11-01'
    AND countryId = '${egyptSId}'
    AND status IN ('UserCanceled', 'ClientCanceled')
    AND businessTypeId = '${pharmacyBusinessTypeId}'
    AND reason LIKE '%${pricingIssuesReasonId}%'
`,
};
const allOrdersQuery = {
  query: `
        SELECT orderId  
        FROM "OrdersSales"
        WHERE "__time" >= '2023-10-01' AND "__time" < '2023-11-01'
        AND countryId = '${egyptSId}'
        AND businessTypeId = '${pharmacyBusinessTypeId}'
    `,
};

export async function getCancelledOrdersDueToWrongPricing({ db }) {
  const allOrders = await druidHelper.fetchResultsSQL(allOrdersQuery);
  const cancelledOrders =
    await druidHelper.fetchResultsSQL(cancelledOrdersQuery);

  const numberOfAllOrders = allOrders.length;
  const numberOfCancelledOrders = cancelledOrders.length;
  const cancelledOrdersPercentage = _.round(
    (numberOfCancelledOrders / numberOfAllOrders) * 100,
    2,
  );

  const results = {
    "Number of All Orders": numberOfAllOrders,
    "Number of Cancelled Orders": numberOfCancelledOrders,
    "Cancelled Orders Percentage": `${cancelledOrdersPercentage}%`,
  };

  WriteFile.CSV(
    results,
    "2023_11_30_cancelled_orders_due_to_wrong_pricing.csv",
  );
  return results;
}
