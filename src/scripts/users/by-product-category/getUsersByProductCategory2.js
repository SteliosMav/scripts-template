import { WriteFile } from "../../../lib/write-file.js";
import _ from "lodash";

const uaeId = "ryFmc6ACd1";
const cosmeticsCategoryId = "SNr1z5qkWu";

const _getDruidQuery = ({ druidHelper }) => {
  return {
    query: `
            SELECT userId
            FROM "ProductsSales"
            WHERE __time >= '2023-09-13' AND __time < '2023-12-14'
            AND substituteFor = ''
            AND orderStatus = 'Completed'
            AND productStatus = 'packaged'
            AND countryId = '${uaeId}'
            AND (
              clientBusinessTypeId = 'fDXWEsMFgP' OR
              categoryId = '${cosmeticsCategoryId}'
            )
`,
  };
};

export async function getUsersByProductCategory2({ db, druidHelper }) {
  const druidQuery = _getDruidQuery({ druidHelper });
  const druidRes = await druidHelper.fetchResultsSQL(druidQuery);
  const duplicateUserIds = druidRes.map((row) => row.userId);
  const uniqueUserIds = _.uniq(duplicateUserIds);

  console.log("duplicateUserIds", duplicateUserIds.length);
  console.log("uniqueUserIds", uniqueUserIds.length);

  const users = await _getUsersByIds({ db, userIds: uniqueUserIds });

  WriteFile.CSV(users, "2023_12_13_users-by-product-category.csv");
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
