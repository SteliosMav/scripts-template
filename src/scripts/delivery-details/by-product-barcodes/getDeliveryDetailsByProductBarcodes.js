import { WriteFile } from "../../../lib/write-file.js";
import { barcodes } from "./barcodes.js";
import _ from "lodash";

const uaeCountryId = "ryFmc6ACd1";

const _getDruidQuery = ({ druidHelper, productIds }) => {
  const sqlArr = druidHelper.getArrayAsString(productIds);
  return {
    query: `
            SELECT orderId, userId
            FROM "ProductsSales" 
            WHERE __time >= '2023-11-09' AND __time < '2023-11-22'
            AND substituteFor = ''
            AND orderStatus = 'Completed'
            AND productStatus = 'packaged'
            AND countryId = '${uaeCountryId}'
            AND productId IN (${sqlArr})
`,
  };
};

export async function getDeliveryDetailsByProductBarcodes({ db, druidHelper }) {
  const barcodesArr = barcodes.replace(/ /g, "").split("\n");
  const productIds = await _getProductIdsByBarcodes({
    db,
    barcodes: barcodesArr,
  });

  const druidQuery = _getDruidQuery({ druidHelper, productIds });
  const productSales = await druidHelper.fetchResultsSQL(druidQuery);
  const orderIds = _getUniqueOrderIdsByProductSales(productSales);

  const deliveryDetailsIds = await _getDeliveryDetailsIdsByOrderIds({
    db,
    orderIds: orderIds,
  });

  const deliveryDetails = await _getDeliveryDetailsByIds({
    db,
    deliveryDetailsIds,
  });

  WriteFile.CSV(deliveryDetails, "2023_11_29_delivery_details_by_barcodes.csv");
  return deliveryDetails;
}

const _getProductIdsByBarcodes = async ({ db, barcodes }) => {
  const res = await db
    .collection("Products")
    .aggregate([
      {
        $match: {
          "barcode.value": {
            $in: barcodes,
          },
        },
      },
      {
        $project: {
          _id: 1,
        },
      },
    ])
    .toArray();
  return res.map((product) => product._id);
};

const _getUniqueOrderIdsByProductSales = (productSales) => {
  const map = new Map();
  productSales.forEach(({ orderId, userId }) => {
    const key = `${orderId}-${userId}`;
    if (!map.has(key)) {
      map.set(key, orderId);
    }
  });
  const orderIdArr = Array.from(map.values());
  return orderIdArr;
};

const _getDeliveryDetailsIdsByOrderIds = async ({ db, orderIds }) => {
  const deliveryDetails = await db
    .collection("Orders")
    .aggregate([
      {
        $match: {
          _id: {
            $in: orderIds,
          },
        },
      },
      {
        $project: {
          _id: 0,
          deliveryDetailsId: {
            $arrayElemAt: [
              { $split: ["$_p_deliveryDetails", "DeliveryDetails$"] },
              1,
            ],
          },
        },
      },
    ])
    .toArray();
  return deliveryDetails.map((order) => order.deliveryDetailsId);
};

const _getDeliveryDetailsByIds = async ({ db, deliveryDetailsIds }) => {
  const deliveryDetails = await db
    .collection("DeliveryDetails")
    .aggregate([
      {
        $match: {
          _id: {
            $in: deliveryDetailsIds,
          },
        },
      },
      {
        $project: {
          _id: 0,
          name: 1,
          email: 1,
          phoneNumber: 1,
          address: {
            $concat: [
              { $arrayElemAt: ["$locationInfo.addressLines", 0] },
              ", ",
              { $arrayElemAt: ["$locationInfo.addressLines", 1] },
            ],
          },
        },
      },
    ])
    .toArray();
  return deliveryDetails;
};
