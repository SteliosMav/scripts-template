import { WriteFile } from "../../../lib/write-file.js";
import _ from "lodash";

const egyptSId = "vrii93Zwoj";

export async function getActiveUsersWithoutOrderLastMonth({ db, druidHelper }) {
  // Get all active user ids
  const activeUserIds = await _getActiveUserIds({ druidHelper });

  // Get all active user ids that did NOT make an order in January 2024
  const activeUserIdsWithoutOrderInJanuary2024 =
    await _getActiveUserIdsWithoutOrderInJanuary2024({
      druidHelper,
      activeUserIds,
    });

  // Get all active users that did NOT make an order in January 2024 by their ids
  const users = await _getUsersByIds({
    db,
    userIds: activeUserIdsWithoutOrderInJanuary2024,
  });

  // Export data to CSV
  WriteFile.CSV(users, "2024_01_19_active-users-without-order-last-month.csv");
}

const _getActiveUserIds = async ({ druidHelper }) => {
  const ordersFromActiveUsersQuery = {
    query: `
            SELECT userId
            FROM "OrdersSales"
            WHERE __time >= '2023-11-01' // AND __time <= '2024-01-01'
            AND countryId = '${egyptSId}'
            AND status = 'Completed'
`,
  };
  const duplicateActiveUsers = await druidHelper.fetchResultsSQL(
    ordersFromActiveUsersQuery,
  );
  const uniqueActiveUsers = _.uniqBy(duplicateActiveUsers, "userId");
  const activeUserIds = uniqueActiveUsers.map((row) => row.userId);
  return activeUserIds;
};

const _getActiveUserIdsWithoutOrderInJanuary2024 = async ({
  druidHelper,
  activeUserIds,
}) => {
  const ordersInJanuary2024Query = {
    query: `
            SELECT userId
            FROM "OrdersSales"
            WHERE __time >= '2024-01-01' AND __time <= '2024-01-31'
            AND countryId = '${egyptSId}'
            AND status = 'Completed'
`,
  };
  const duplicateJanuary2024Users = await druidHelper.fetchResultsSQL(
    ordersInJanuary2024Query,
  );
  const uniqueJanuary2024Users = _.uniqBy(duplicateJanuary2024Users, "userId");
  const january2024UserIds = uniqueJanuary2024Users.map((row) => row.userId);
  const activeUserIdsWithoutOrderInJanuary2024 = _.difference(
    activeUserIds,
    january2024UserIds,
  );
  return activeUserIdsWithoutOrderInJanuary2024;
};

const _getUsersByIds = async ({ db, userIds }) => {
  const users = await db
    .collection("_User")
    .aggregate([
      { $match: { _id: { $in: userIds } } },
      { $project: { _id: 0, email: "$unverifiedEmail", badge: "$orderCount" } },
    ])
    .toArray();
  return users;
};
