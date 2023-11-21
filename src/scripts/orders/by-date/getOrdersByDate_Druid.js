import { DruidHelper } from "../../../lib/druid.js";
import _ from "lodash";

const druidHelper = new DruidHelper();

// ProductSales

// const druidQuery = {
//   query: `
//             SELECT orderId, amountPayableInternal FROM "OrdersSales"
//             WHERE status='Completed'
//             AND __time >= '2023-10-01' AND __time < '2023-11-01'
//             AND countryId = 'ryFmc6ACd1'
//             AND testClient !='true'
//             LIMIT 1000
//             `,
// };
//
// const testQuery = {
//   query: `
//     SELECT * FROM "OrdersSales"
//     LIMIT 1
// `,
// };

const druidQuery = {
  query: `
    SELECT clientAreaId, productId  FROM "ProductsSales"
    WHERE "__time" > '2023-10-19' 
    AND "__time" < '2023-11-19' 
    AND orderStatus = 'Completed' 
    AND productStatus = 'packaged'
    AND clientId in ('5doGrX17PD',
    'PO5JfA7DeY',
    'sjrjImserN',
    'QpiAYKhWpP',
    'zWSeMCkrwF',
    'H3zUaeS1aH',
    'h5CXKeP4Xr',
    '2LpDEEw1bA')
    GROUP BY 1,2
`,
};

export async function getOrdersByDate_Druid({ db }) {
  console.time("druid");
  const druidRes = await druidHelper.fetchResultsSQL(druidQuery);
  console.timeEnd("druid");

  const duplicateClientAreaIds = druidRes.map((row) => row.clientAreaId);
  const duplicateProductIds = druidRes.map((row) => row.productId);
  const uniqueClientAreaIds = _.uniq(duplicateClientAreaIds);
  const uniqueProductIds = _.uniq(duplicateProductIds);

  // Warning! Data coming from druid might not exist in mongo due to data sync issues.
  // Deleted documents in mongo will not be deleted in druid.
  const [clientAreasDictionary, productsDictionary] = await Promise.all([
    _getClientAreasDictionary({ db, clientAreaIds: uniqueClientAreaIds }),
    _getProductsDictionary({ db, productIds: uniqueProductIds }),
  ]);

  // dataDictionary or dataMap
  const data = {};

  for (const row of druidRes) {
    const el = data[row.clientAreaId];
    if (el) {
      const product = productsDictionary[row.productId];
      // Data from druid might not exist in mongo due to data sync issues.
      if (!product) {
        continue;
      }
      el.barcodes.push(product.barcode);
    } else {
      data[row.clientAreaId] = {};
      const newEl = data[row.clientAreaId];
      const client = clientAreasDictionary[row.clientAreaId];
      const product = productsDictionary[row.productId];
      // Data from druid might not exist in mongo due to data sync issues.
      if (!client || !product) {
        continue;
      }
      newEl.clientAreaName = client.name;
      newEl.barcodes = [product.barcode];
    }
  }

  const dataArr = Object.values(data);

  console.log("dataArr.length", dataArr.length);

  // return dataArr;
}

const _getClientAreasDictionary = async ({ db, clientAreaIds }) => {
  const clientAreasRes = await db
    .collection("ClientArea")
    .aggregate([
      {
        $match: { _id: { $in: clientAreaIds } },
      },
      {
        $project: {
          name: "$properties.name",
        },
      },
    ])
    .toArray();

  const dictionary = {};

  for (const row of clientAreasRes) {
    dictionary[row._id] = {
      _id: row._id,
      name: row.name,
    };
  }

  return dictionary;
};

const _getProductsDictionary = async ({ db, productIds }) => {
  const productsRes = await db
    .collection("Products")
    .aggregate([
      {
        $match: { _id: { $in: productIds } },
      },
      {
        $project: {
          barcode: "$barcode.value",
        },
      },
    ])
    .toArray();

  const dictionary = {};

  for (const row of productsRes) {
    dictionary[row._id] = {
      _id: row._id,
      barcode: row.barcode,
    };
  }

  return dictionary;
};
