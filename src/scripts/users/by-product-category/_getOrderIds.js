export async function _getOrderIds({
  db,
  petShopClientIds,
  superMarketClientIds,
  productPointers,
}) {
  const currentDate = new Date();
  const sixMonthsAgoDate = new Date(
    currentDate.setMonth(currentDate.getMonth() - 6),
  );

  const orderItemsRes = await db
    .collection("OrderItem")
    .aggregate(
      [
        { $sort: { _created_at: -1 } },
        { $limit: 1000 },
        {
          $match: {
            status: "packaged",
            $or: [
              {
                client: {
                  $in: petShopClientIds,
                },
              },
              {
                client: {
                  $in: superMarketClientIds,
                },
                _p_product: {
                  $in: productPointers,
                },
              },
            ],
            substituteFor: {
              $exists: false,
            },
            _created_at: {
              $gte: sixMonthsAgoDate,
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
