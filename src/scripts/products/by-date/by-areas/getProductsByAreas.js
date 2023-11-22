import { DruidHelper } from "../../../../lib/druid.js";
import _ from "lodash";
import { WriteFile } from "../../../../lib/write-file.js";

const druidHelper = new DruidHelper();

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

export async function getProductsByDateByAreas({ db }) {
  console.time("druid");
  const druidRes = await druidHelper.fetchResultsSQL(druidQuery);
  console.timeEnd("druid");

  return _getProductPerAreaCountMap(druidRes);

  const duplicateClientAreaIds = druidRes.map((row) => row.clientAreaId);
  const duplicateProductIds = druidRes.map((row) => row.productId);
  const uniqueClientAreaIds = _.uniq(duplicateClientAreaIds);
  const uniqueProductIds = _.uniq(duplicateProductIds);

  // Warning! Data coming from druid might not exist in mongo due to data sync issues.
  // Deleted documents in mongo will not be deleted in druid.
  const [clientAreasMap, productsMap] = await Promise.all([
    _getClientAreasMap({ db, clientAreaIds: uniqueClientAreaIds }),
    _getProductsMap({ db, productIds: uniqueProductIds }),
  ]);

  const formattedData = _formatDruidResponse({
    druidRes,
    clientAreasMap,
    productsMap,
  });

  WriteFile.CSV(formattedData, "products-by-areas.csv");
}

const _formatDruidResponse = ({ druidRes, clientAreasMap, productsMap }) => {
  const dataMap = new Map();

  for (const row of druidRes) {
    const el = dataMap.get(row.clientAreaId);

    if (el) {
      const product = productsMap.get(row.productId);

      // Data from druid might not exist in mongo due to data sync issues.
      if (!product) continue;

      el.barcodes.push(product.barcode);
    } else {
      const newEl = {
        clientAreaName: "",
        barcodes: [],
      };
      const client = clientAreasMap.get(row.clientAreaId);
      const product = productsMap.get(row.productId);

      // Data from druid might not exist in mongo due to data sync issues.
      if (!client || !product) continue;

      newEl.clientAreaName = client.name;
      newEl.barcodes.push(product.barcode);
      dataMap.set(row.clientAreaId, newEl);
    }
  }

  return Array.from(dataMap.values());
};

const _getClientAreasMap = async ({ db, clientAreaIds }) => {
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

  const map = new Map();

  for (const row of clientAreasRes) {
    map.set(row._id, { ...row });
  }

  return map;
};

const _getProductsMap = async ({ db, productIds }) => {
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

  const map = new Map();

  for (const row of productsRes) {
    map.set(row._id, { ...row });
  }

  return map;
};
