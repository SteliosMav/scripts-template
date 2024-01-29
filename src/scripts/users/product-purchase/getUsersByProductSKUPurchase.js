import { WriteFile } from "../../../lib/write-file.js";
import _ from "lodash";

const skuList = [
  "655199031641",
  "655199031665",
  "655199041510",
  "655199041619",
  "655199042579",
  "655199073115",
  "655199075218",
  "655199076000",
  "655199029952",
  "655199029969",
  "655199052066",
  "655199090709",
  "655199090716",
  "655199090754",
  "655199090921",
  "655199090976",
  "655199091713",
  "655199095155",
  "764460448825",
  "764460448726",
  "764460448856",
  "764460448733",
  "764460448832",
  "764460448849",
  "764460448634",
  "764460448672",
  "764460448658",
  "764460448696",
  "764460448627",
  "764460448665",
  "764460448641",
  "764460448689",
  "4743318102900",
  "4743318102917",
  "4743318102924",
  "4743318102825",
  "4743318102801",
  "4743318102757",
  "4743318102726",
  "4022858612217",
  "4022858612224",
  "4022858612231",
  "4022858612248",
  "4022858612255",
  "4022858612262",
  "4022858612279",
  "4022858612286",
  "4022858612293",
  "4022858612309",
  "4022858612316",
  "4022858612323",
  "4022858612378",
  "4022858612385",
  "4022858612392",
  "4022858612408",
  "4022858612415",
  "4022858612422",
  "4022858612439",
  "4022858612446",
  "4022858612453",
  "4022858612460",
  "786306585228",
  "786306585235",
  "786306585242",
  "786306585259",
  "786306585266",
  "786306585273",
  "786306585280",
  "786306585297",
  "786306585303",
  "786306585310",
  "786306585327",
  "786306585334",
  "786306585341",
  "786306585358",
  "743723701204",
  "743723701211",
  "743723702799",
  "743723702829",
  "743723702836",
  "743723703000",
  "743723706377",
  "743723706704",
  "743723707312",
  "786306707354",
  "743723707961",
  "743723707985",
  "743723709729",
  "743723709897",
  "786306735654",
  "786306735661",
  "786306735678",
  "786306735685",
  "786306735708",
  "786306735715",
  "786306735739",
  "786306735753",
  "786306735760",
  "786306735784",
  "8429886024597",
  "8429886024603",
  "8429886022470",
  "8429886025075",
  "8429886022951",
  "8429886024672",
  "8429886024665",
  "8429886024689",
  "8429886022937",
  "8429886022944",
  "8429886022463",
  "8429886022968",
  "8429886008313",
  "8429886020230",
  "8429886020247",
  "8429886022593",
  "8429886022609",
  "8429886022616",
  "8429886023408",
  "8429886023415",
  "8429886021701",
  "8429886001000",
  "8429886020285",
  "8429886020292",
  "No Primary Barcode",
  "8429886024696",
  "045125500226",
  "045125502435",
  "045125522402",
  "045125552287",
  "045125552386",
  "45125600292",
  "045125601114",
  "045125604009",
  "045125604023",
  "045125604061",
  "045125604078",
  "045125604924",
  "045125604955",
  "045125605068",
  "045125605075",
  "045125605112",
  "045125605143",
  "045125605150",
  "045125605167",
  "045125605181",
  "045125605211",
  "045125605235",
  "045125605259",
  "045125606232",
  "726684652006",
  "726684652013",
  "726684652020",
  "726684652037",
  "726684652044",
  "726684652051",
  "726684652068",
  "726684652075",
  "726684652082",
  "726684652099",
  "726684652105",
  "726684652112",
  "726684652129",
  "726684652136",
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

  return Object.keys(groupedByOrderId).length;

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

  // WriteFile.CSV(users, "2024_01_17_users-by-product-purchase.csv");

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
