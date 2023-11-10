export async function _getPetShopOrderIds({ db, dateGte, petShopClientIds }) {
  const orderItemsRes = await db
    .collection("OrderItem")
    .aggregate(
      [
        // { $sort: { _created_at: -1 } },
        // { $limit: 1000 },
        {
          $match: {
            status: "packaged",
            client: {
              $in: petShopClientIds,
            },
            substituteFor: {
              $exists: false,
            },
            _created_at: {
              $gte: dateGte,
            },
          },
        },
        {
          $project: {
            _id: 0,
            orderId: "$order",
          },
        },
      ],
      {
        allowDiskUse: false,
      },
    )
    .toArray();

  return orderItemsRes.map(({ orderId }) => orderId);
}
