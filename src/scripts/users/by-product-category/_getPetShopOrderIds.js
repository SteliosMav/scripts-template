// NOTE:
//
// SHOULD HAVE FETCHED ORDERS DIRECTLY INSTEAD OF ORDER ITEMS SINCE ORDERS
// DO HAVE CLIENTS ON THEM

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
        // { $limit: 10 },
      ],
      {
        allowDiskUse: false,
      },
    )
    .toArray();

  return orderItemsRes.map(({ orderId }) => orderId);
}
