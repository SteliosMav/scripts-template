import { WriteFile } from "../../../lib/write-file.js";
import _ from "lodash";

const skuList = [
  "7622202017728",
  "7622201765279",
  "7622201765286",
  "7622210874351",
  "7622201707118",
  "7622201698256",
  "7622201765231",
  "7622201765224",
  "7622201764685",
  "7622201764678",
  "7622201764548",
  "7622201764555",
  "7622201699277",
  "7622210785114",
  "7622210785114",
  "7622210785145",
  "7622210957375",
  "7622201510671",
  "7622201731441",
];
const uaeCountryId = "ryFmc6ACd1";

function getDruidQuery({ productIds, druidHelper }) {
  const productIdsString = druidHelper.getArrayAsString(productIds);
  return {
    query: `
            SELECT orderId, userId, productId, productPriceAED, deliveryDetailsId
            FROM "ProductsSales"
            WHERE __time >= '2023-12-15' AND __time <= '2023-12-21'
            AND countryId = '${uaeCountryId}'
            AND productId IN (${productIdsString})
            AND orderStatus = 'Completed' 
            AND productStatus = 'packaged'
`,
  };
}

export async function getUsersByProductSKUPurchase({ db, druidHelper }) {
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

  // Get productSales from Druid
  const druidQuery = getDruidQuery({ productIds, druidHelper });
  const productSales = await druidHelper.fetchResultsSQL(druidQuery);

  // Group by orderId
  const groupedByOrderId = _.groupBy(productSales, "orderId");

  // Add the sum of the product prices to the order
  const ordersWithSumOfProductPrices = _.map(groupedByOrderId, (order) => {
    const sumOfProductPrices = _.sumBy(order, "productPriceAED");
    return {
      userId: order[0].userId,
      deliveryDetailsId: order[0].deliveryDetailsId,
      orderId: order[0].orderId,
      sumOfProductPrices,
      productOrders: order.map((el) => ({
        productId: el.productId,
        productPriceAED: el.productPriceAED,
      })),
    };
  });

  // Filter out orders that have sumOfProductPrices < 30
  const ordersWithSumOfProductPricesGreaterThan30 = _.filter(
    ordersWithSumOfProductPrices,
    (order) => order.sumOfProductPrices > 30,
  );

  // Get users by deliveryDetailsId
  const deliveryDetailsIds = ordersWithSumOfProductPricesGreaterThan30.map(
    (row) => row.deliveryDetailsId,
  );
  const users = await _getUsersByDeliveryDetailsIds({ db, deliveryDetailsIds });

  // WriteFile.CSV(users, "2024_01_17_users-by-product-sku-purchase.csv");

  return JSON.stringify(users);
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
