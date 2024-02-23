import { WriteFile } from "../../../lib/write-file.js";

export async function getUsersByOrderComment({ db, druidHelper, Parse }) {
  // Get orders
  const orders = await db
    .collection("Orders")
    .aggregate([
      {
        $match: {
          $and: [
            {
              _p_country: "Countries$ryFmc6ACd1",
            },
            {
              _created_at: {
                $gte: new Date("2024-02-15T00:00:00.000+0000"),
              },
            },
            {
              _created_at: {
                $lt: new Date("2024-02-23T00:00:00.000+0000"),
              },
            },
            {
              status: "Completed",
            },
            {
              comments: /.*hart.*/i,
            },
          ],
        },
      },
      {
        $project: {
          _id: 0,
          comments: 1,
          _p_user: 1,
          _p_deliveryDetails: 1,
        },
      },
    ])
    .toArray();

  // Users MAP
  let groupedOrdersByUser = {};
  for (const order of orders) {
    const userId = order._p_user.substring(6);
    if (!groupedOrdersByUser[userId]) {
      groupedOrdersByUser[userId] = {
        userId,
        deliveryDetailsId: order._p_deliveryDetails.substring(16),
      };
    }
  }

  // Get delivery details by ids
  const deliveryDetailsIds = Object.values(groupedOrdersByUser).map(
    (el) => el.deliveryDetailsId,
  );
  const deliveryDetails = await db
    .collection("DeliveryDetails")
    .aggregate([
      {
        $match: { _id: { $in: deliveryDetailsIds } },
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

  const results = deliveryDetails.map((el) => {
    const newEl = {
      "InstaShop Member": "",
      Name: el.name,
      Email: el.email,
      "Phone Number": el.phoneNumber,
      Adress: el.address,
    };
    if (el.email?.includes("@instashop.ae")) {
      newEl["InstaShop Member"] = "InstaShop Member";
    }
    return newEl;
  });

  WriteFile.CSV(results, "2024_02_23_users_by_order_comment.csv");
}
