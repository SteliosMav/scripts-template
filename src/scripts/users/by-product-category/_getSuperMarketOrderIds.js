export async function _getSuperMarketOrderIds({
  db,
  dateGte,
  dateLte,
  superMarketClientIds,
  productPointers,
}) {
  const orderItemsRes = await db
    .collection("OrderItem")
    .aggregate(
      [
        {
          $match: {
            status: "packaged",
            client: {
              $in: superMarketClientIds,
            },
            _p_product: {
              $in: productPointers,
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
