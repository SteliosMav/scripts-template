/**
 * @description List of supermarkets in UAE with the "Russian" subcategory enabled under
 * the "Around the world" category along with the number of SKUs under this subcategory
 * per store
 *
 * @note All "Russian Products" subcategory SKUs are under the "Around the world" category
 */
import { WriteFile } from "../../lib/write-file.js";

// Category ID for "Around the world"
const categoryId = "XE7kc3ZoS8";
// Subcategory ID for "Russian Products"
// const subcategoryId = "0rXb5Ysdet";
// Subcategory ID for "Russia"
const subcategoryId = "BntXrMEuEn";

export async function clientsByCategoryBySubcategory({
  db,
  druidHelper,
  Parse,
}) {
  // Get clients in category and subcategory
  const clientsInCategory = await db
    .collection("_Join:categories:Clients")
    .aggregate([
      { $match: { relatedId: categoryId } },
      { $project: { _id: 0, clientId: "$owningId" } },
    ])
    .toArray();
  const clientsInSubcategory = await db
    .collection("_Join:clients:Subcategories")
    .aggregate([
      { $match: { owningId: subcategoryId } },
      { $project: { _id: 0, clientId: "$relatedId" } },
    ])
    .toArray();

  // Get clientIds in both category and subcategory
  const clientIdsInCategory = clientsInCategory.map((c) => c.clientId);
  const clientIdsInSubcategory = clientsInSubcategory.map((c) => c.clientId);
  const clientIdsInBoth = clientIdsInCategory.filter((c) =>
    clientIdsInSubcategory.includes(c),
  );

  // Get clients with business type grocery in UAE and in subcategory
  const clients = await db
    .collection("Clients")
    .aggregate([
      {
        $match: {
          _id: { $in: clientIdsInBoth },
          "businessType.grocery": true,
          _p_country: "Countries$ryFmc6ACd1",
        },
      },
      {
        $project: {
          name: 1,
        },
      },
    ])
    .toArray();
  const clientIds = clients.map((c) => c._id);

  console.log("clientIds", clientIds.length);

  // Get number of SKUs in "Russian Products" subcategory per store
  const skus = await db
    .collection("ProductObject")
    .aggregate([
      {
        $match: {
          clientId: { $in: clientIds },
          subcategories: {
            $elemMatch: {
              $eq: subcategoryId, // "h6Zsb0Itbs",
            },
          },
          active: true,
        },
      },
      {
        $group: {
          _id: "$clientId",
          numberOfSKUs: { $sum: 1 },
        },
      },
      {
        $project: {
          _id: 0,
          clientId: "$_id",
          numberOfSKUs: 1,
        },
      },
      {
        $lookup: {
          from: "Clients",
          localField: "clientId",
          foreignField: "_id",
          as: "client",
        },
      },
      {
        $unwind: "$client",
      },
      {
        $project: {
          "Client Name": "$client.name",
          "Number of SKUs": "$numberOfSKUs",
        },
      },
    ])
    .toArray();

  WriteFile.CSV(skus, "2024_02_06_clients-by-subcategory.csv");
}
