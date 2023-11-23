import { WriteFile } from "../../../lib/write-file.js";
import _ from "lodash";

const greeceSId = "mSkwRgn6gt";

const _getDruidQuery = ({ categoryIds, druidHelper }) => {
  const sqlArr = druidHelper.getArrayAsString(categoryIds);
  return {
    query: `
            SELECT userId
            FROM "ProductsSales"
            WHERE __time >= '2023-10-23' AND __time < '2023-11-23'
            AND substituteFor = ''
            AND orderStatus = 'Completed'
            AND productStatus = 'packaged'
            AND countryId = '${greeceSId}'
            AND categoryId IN (${sqlArr})
`,
  };
};

export async function getUsersByProductCategory({ db, druidHelper }) {
  const categories = await _getCategories({ db });
  const categoryIds = categories.map((category) => category.categoryId);
  const druidQuery = _getDruidQuery({ categoryIds, druidHelper });
  const druidRes = await druidHelper.fetchResultsSQL(druidQuery);
  const duplicateUserIds = druidRes.map((row) => row.userId);
  const uniqueUserIds = _.uniq(duplicateUserIds);

  console.log("duplicateUserIds", duplicateUserIds.length);
  console.log("uniqueUserIds", uniqueUserIds.length);

  const users = await _getUsersByIds({ db, userIds: uniqueUserIds });

  WriteFile.CSV(users, "2023_11_23_users-by-product-category.csv");
  return users.length;
}

const _getCategories = async ({ db }) => {
  const categories = await db
    .collection("BusinessType")
    .aggregate(
      [
        {
          $match: {
            title: {
              $in: ["Supermarkets", "Pet Shops"],
            },
          },
        },
        {
          $project: {
            _id: 0,
            businessType: "$title",
            businessTypePointer: {
              $concat: ["BusinessType$", "$_id"],
            },
          },
        },
        {
          $lookup: {
            from: "Categories",
            let: {
              businessTypePointer: "$businessTypePointer",
            },
            pipeline: [
              {
                $match: {
                  title: /.*Pet.*/,
                  $expr: {
                    $eq: ["$_p_businessType", "$$businessTypePointer"],
                  },
                },
              },
              {
                $project: {
                  title: 1,
                },
              },
            ],
            as: "categories",
          },
        },
        {
          $unwind: {
            path: "$categories",
          },
        },
        {
          $project: {
            businessType: 1,
            categoryId: "$categories._id",
            categoryName: "$categories.title",
          },
        },
      ],
      {
        allowDiskUse: false,
      },
    )
    .toArray();

  return categories;
};

const _getUsersByIds = async ({ db, userIds }) => {
  return db
    .collection("_User")
    .aggregate([
      { $match: { _id: { $in: userIds } } },
      { $project: { email: "$unverifiedEmail" } },
    ])
    .toArray();
};
