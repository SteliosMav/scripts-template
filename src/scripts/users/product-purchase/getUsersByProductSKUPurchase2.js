import { WriteFile } from "../../../lib/write-file.js";
import _ from "lodash";

const skuList = ["12000054099"];
const uaeCountryId = "ryFmc6ACd1";

function getDruidQuery({ productIds, druidHelper }) {
  const productIdsString = druidHelper.getArrayAsString(productIds);
  return {
    query: `
            SELECT orderId, userId, productId, deliveryDetailsId
            FROM "ProductsSales"
            WHERE __time >= '2024-02-12' AND __time <= '2024-02-14'
            AND countryId = '${uaeCountryId}'
            AND productId IN (${productIdsString})
            AND orderStatus = 'Completed' 
            AND productStatus = 'packaged'
`,
  };
}

export async function getUsersByProductSKUPurchase2({ db, druidHelper }) {
  // Get productIds from Products Collection
  const productIdsRes = await db
    .collection("Products")
    .aggregate([
      {
        $match: {
          "barcode.value": { $in: skuList },
        },
      },
      { $project: { _id: 1 } },
    ])
    .toArray();
  const productIds = productIdsRes.map((row) => row._id);

  // Get Orders from Druid
  const druidQuery = getDruidQuery({ productIds, druidHelper });
  const productSales = await druidHelper.fetchResultsSQL(druidQuery);

  // Group by orderId
  const groupedByOrderId = _.groupBy(productSales, "orderId");

  // Group by userId
  const groupedByUserId = _.groupBy(productSales, "userId");

  // Get unique delivery details ids
  const deliveryDetailsIds = Object.values(groupedByOrderId).map(
    (order) => order[0].deliveryDetailsId,
  );

  const users = await _getUsersByDeliveryDetailsIds({ db, deliveryDetailsIds });

  WriteFile.CSV(users, "2024_02_14_users-by-product-purchase.csv");
}

const _getUsersByDeliveryDetailsIds = async ({ db, deliveryDetailsIds }) => {
  return db
    .collection("DeliveryDetails")
    .aggregate([
      { $match: { _id: { $in: deliveryDetailsIds } } },
      {
        $project: {
          _id: 0,
          name: 1,
          email: 1,
          phoneNumber: 1,
          address: {
            $concat: [
              {
                $arrayElemAt: ["$locationInfo.addressLines", 0],
              },
              " ",
              {
                $arrayElemAt: ["$locationInfo.addressLines", 1],
              },
            ],
          },
        },
      },
    ])
    .toArray();
};
