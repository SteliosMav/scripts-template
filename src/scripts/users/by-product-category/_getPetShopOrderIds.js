export async function _getPetShopOrderIds({
  db,
  dateGte,
  dateLte,
  petShopClientIds,
}) {
  const orderItemsRes = await db
    .collection("OrderItem")
    .aggregate(
      [
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
              $lte: dateLte,
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
