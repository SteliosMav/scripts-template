/**
 * @description The average number of days taken from installing the
 * app until making the 1st order.
 */

import { COUNTRY_NAMES } from "../../../utils/countryIds.js";
import { WriteFile } from "../../../lib/write-file.js";

const _getDruidQuery = (monthRange) => {
  let year = 2023;
  if (monthRange[1] === 13) {
    monthRange[1] = 1;
    year = 2024;
  }
  return {
    query: `
        SELECT countryId, "__time", userId, orderId, installationId
        FROM "OrdersSales"
        WHERE "__time" >= '2023-${monthRange[0]}-01'
        AND "__time" < '${year}-${monthRange[1]}-01'
        AND status = 'Completed'
`,
  };
};

export async function daysPastFromInstallationAfterFirstOrder({
  db,
  druidHelper,
  Parse,
}) {
  // Get orders from druid
  const promises = [];
  for (let i = 1; i <= 12; i++) {
    console.log("Month: " + i);
    const monthRange = [i, i + 1];
    const druidQuery = _getDruidQuery(monthRange);
    promises.push(druidHelper.fetchResultsSQL(druidQuery));
  }
  const druidRes = await Promise.all(promises);
  const allOrders = [].concat(...druidRes);

  // Store the first order date and the installationId for each user
  const usersMap = new Map();
  for (const order of allOrders) {
    const { userId, installationId, __time } = order;
    if (!usersMap.has(userId)) {
      usersMap.set(userId, { installationId, firstOrderDate: __time });
    }
  }

  // Get the installation date for each user in chunks of 1000
  const installationIdsChunks = [];
  let chunk = [];
  for (const [userId, user] of usersMap) {
    chunk.push(user.installationId);
    if (chunk.length === 1000) {
      installationIdsChunks.push(chunk);
      chunk = [];
    }
  }

  // Get installations from MongoDB in chunks of 1000
  const installationsChunks = [];
  for (let i = 0; i < installationIdsChunks.length; i++) {
    console.log(`Chunk: ${i + 1}/${installationIdsChunks.length}`);
    const promise = await db
      .collection("_Installation")
      .find({
        _id: {
          $in: chunk,
        },
        _created_at: {
          $gte: new Date("2023-01-01"),
          $lt: new Date("2024-01-01"),
        },
      })
      .project({
        _id: 0,
        _p_user: 1,
        _created_at: 1,
      })
      .toArray();
    installationsChunks.push(promise);
  }

  // Get the sum of the installation date and the first order date
  let counter = 0;
  let installationDateSum = 0;
  let firstOrderDateSum = 0;
  installationsChunks.forEach((chunk) => {
    chunk.forEach((installation) => {
      const { _p_user, _created_at } = installation;
      const userId = _p_user.substring(6);
      if (usersMap.has(userId) && usersMap.get(userId).firstOrderDate) {
        counter++;
        installationDateSum += new Date(_created_at).getTime();
        firstOrderDateSum += new Date(
          usersMap.get(userId).firstOrderDate,
        ).getTime();
      }
    });
  });

  // Calculate the average number of days taken from installing the app until making the 1st order
  const averageDays = Math.round(
    (firstOrderDateSum - installationDateSum) / (1000 * 60 * 60 * 24 * counter),
  );
  const formattedForCsv = [
    { "Average days between installation and first order": averageDays },
  ];

  console.log("Average days: " + averageDays);

  // Export Data
  WriteFile.CSV(
    formattedForCsv,
    "2024_02_15_days_between_installation_and_first_order.csv",
  );
}
