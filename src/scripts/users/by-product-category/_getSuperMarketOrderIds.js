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
            status: "packaged",
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
