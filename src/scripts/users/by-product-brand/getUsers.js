import json from "../../../../exported_data/orderItems.json" assert { type: "json" };

const orderItemsFromJson = json;

export async function getUsers({ db }) {
  const orderItemIds = orderItemsFromJson.map(({ orderItemId }) => orderItemId);
  const orderItemsResponseWithOrder = await db
    .collection("OrderItem")
    .aggregate(
      [
        {
          $match: {
            _id: {
              $in: orderItemIds,
            },
          },
        },
        {
          $project: {
            _id: 0,
            order: 1,
          },
        },
      ],
      {
        allowDiskUse: false,
      },
    )
    .toArray();

  const orderIds = orderItemsResponseWithOrder.map(({ order }) => order);

  const ordersRes = await db
    .collection("Orders")
    .aggregate([
      {
        $match: {
          _id: {
            $in: orderIds,
          },
          _p_country: "Countries$ryFmc6ACd1",
        },
      },
      {
        $group: {
          _id: "$_p_user",
        },
      },
    ])
    .toArray();

  const userIds = ordersRes.map(({ _id }) => _id.slice(6));

  return await db
    .collection("_User")
    .aggregate([
      {
        $match: {
          _id: {
            $in: userIds,
          },
        },
      },
      {
        $project: {
          email: "$unverifiedEmail",
        },
      },
    ])
    .toArray();
}
