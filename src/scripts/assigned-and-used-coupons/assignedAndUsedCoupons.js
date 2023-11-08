export async function assignedAndUsedCoupons({ db }) {
  return await db
    .collection("Coupons")
    .aggregate(
      [
        {
          $match: {
            _created_at: {
              $gte: new Date("2022-11-14T00:00:00.000"),
              $lte: new Date("2022-11-30T23:59:59.999"),
            },
            // Monetization with "givenName": "Greece InstaFriday Cashback | Spend 40 Get 7 + 7 | November 2022 | Mahek"
            fromMonetizations: "bpfA0g30ow",
          },
        },
        {
          $project: {
            _id: 0,
            couponId: "$_id",
            createdAt: "$_created_at",
            useCount: 1,
          },
        },
        {
          $group: {
            _id: null,
            couponIds: {
              $addToSet: "$couponId",
            },
            usedCount: {
              $sum: "$useCount",
            },
          },
        },
        {
          $project: {
            _id: 0,
            assignedCount: {
              $size: "$couponIds",
            },
            usedCount: 1,
          },
        },
      ],
      {
        allowDiskUse: false,
      },
    )
    .toArray();
}
