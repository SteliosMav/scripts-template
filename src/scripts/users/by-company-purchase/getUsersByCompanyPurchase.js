import { WriteFile } from "../../../lib/write-file.js";
import _ from "lodash";

const abVasilopoulosCompanyId = "NPlnAWLExZ";
const thanopoulosKifisiaEleonCompanyId = "Qps6056QEt";

const druidQuery = {
  query: `
            SELECT userId
            FROM "OrdersSales"
            WHERE __time >= '2023-11-27' AND __time < '2023-12-14'
            AND companyId = '${thanopoulosKifisiaEleonCompanyId}'
            AND status = 'Completed'
`,
};

export async function getUsersByCompanyPurchase({ db, druidHelper }) {
  const druidRes = await druidHelper.fetchResultsSQL(druidQuery);
  const duplicateUserIds = druidRes.map((row) => row.userId);
  const uniqueUserIds = _.uniq(duplicateUserIds);

  console.log("duplicateUserIds", duplicateUserIds.length);
  console.log("uniqueUserIds", uniqueUserIds.length);

  const users = await _getUsersByIds({ db, userIds: uniqueUserIds });

  WriteFile.CSV(
    users,
    "2023_12_15_users-by-thanopoulos-kifisia-eleon-purchase.csv",
  );
  return users.length;
}

const _getUsersByIds = async ({ db, userIds }) => {
  return db
    .collection("_User")
    .aggregate([
      { $match: { _id: { $in: userIds } } },
      { $project: { email: "$unverifiedEmail" } },
    ])
    .toArray();
};
