import { WriteFile } from "../../../lib/write-file.js";
import json from "../../../../exported_data/2023_11_16_users_by_products.json" assert { type: "json" };

const jsonData = json;

export async function usersByProductName({ db }) {
  WriteFile.CSV(jsonData, "2023_11_16_users_by_products.csv");
  return;

  const campaignIds = ["JMNuawugqh", "3QmetGYGYY"];

  const res = await db
    .collection("Monetization")
    .aggregate([
      { $match: { _id: { $in: campaignIds } } },
      { $project: { _id: 0, targetedProducts: 1 } },
      { $unwind: "$targetedProducts" },
      {
        $project: {
          productPointer: {
            $concat: ["Products$", "$targetedProducts.objectId"],
          },
        },
      },
      { $group: { _id: null, productPointers: { $push: "$productPointer" } } },
      {
        $lookup: {
          from: "OrderItem",
          let: { productPointers: "$productPointers" },
          pipeline: [
            {
              $match: {
                $expr: {
                  $in: ["$_p_product", "$$productPointers"],
                },
                substituteFor: {
                  $exists: false,
                },
                _created_at: {
                  $gte: new Date("2023-10-16T20:00:00.000Z"),
                  $lte: new Date("2023-10-31T19:59:59.999Z"),
                },
                status: "packaged",
              },
            },
            {
              $project: {
                _id: 0,
                orderId: "$order",
              },
            },
            // { $limit: 100 },
          ],
          as: "orderItems",
        },
      },
      { $project: { orderItems: 1 } },
      { $unwind: "$orderItems" },
      { $project: { _id: 0, orderId: "$orderItems.orderId" } },
      {
        $lookup: {
          from: "Orders",
          localField: "orderId",
          foreignField: "_id",
          as: "order",
        },
      },
      { $unwind: "$order" },
      {
        $project: {
          _id: 0,
          userId: {
            $substr: ["$order._p_user", 6, { $strLenCP: "$order._p_user" }],
          },
        },
      },
      {
        $lookup: {
          from: "_User",
          localField: "userId",
          foreignField: "_id",
          as: "user",
        },
      },
      { $unwind: "$user" },
      {
        $project: {
          _id: 0,
          name: "$user.fullName",
          email: "$user.unverifiedEmail",
          phoneNumber: {
            $arrayElemAt: ["$user.phoneNumbers.phoneNumber", 0],
          },
        },
      },
      {
        $group: {
          _id: "$email",
          user: {
            $first: {
              name: "$name",
              email: "$email",
              phoneNumber: "$phoneNumber",
            },
          },
        },
      },
      {
        $replaceRoot: {
          newRoot: "$user",
        },
      },
    ])
    .toArray();

  WriteFile.JSON(res, "2023_11_16_users_by_products.json");

  return res;
}
