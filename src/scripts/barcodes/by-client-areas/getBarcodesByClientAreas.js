import { DruidHelper } from "../../../lib/druid.js";
import _ from "lodash";
import { WriteFile } from "../../../lib/write-file.js";

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

export async function getBarcodesByClientAreas({ db }) {
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

  const formattedData = _formatDruidResponse({
    druidRes,
    clientAreasDictionary,
    productsDictionary,
  });

  WriteFile.CSV(formattedData, "barcodes-by-client-areas.csv");
}

const _formatDruidResponse = ({
  druidRes,
  clientAreasDictionary,
  productsDictionary,
}) => {
  const dataDictionary = {};

  for (const row of druidRes) {
    const el = dataDictionary[row.clientAreaId];

    if (el) {
      const product = productsDictionary[row.productId];

      // Data from druid might not exist in mongo due to data sync issues.
      if (!product) continue;

      el.barcodes.push(product.barcode);
    } else {
      dataDictionary[row.clientAreaId] = {};
      const newEl = dataDictionary[row.clientAreaId];
      const client = clientAreasDictionary[row.clientAreaId];
      const product = productsDictionary[row.productId];

      // Data from druid might not exist in mongo due to data sync issues.
      if (!client || !product) continue;

      newEl.clientAreaName = client.name;
      newEl.barcodes = [product.barcode];
    }
  }

  const dataArr = Object.values(dataDictionary);

  return dataArr;
};

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
