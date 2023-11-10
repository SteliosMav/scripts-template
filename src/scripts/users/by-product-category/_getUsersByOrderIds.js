export async function _getUsersByOrderIds({ db, orderIds }) {
  return db
    .collection("Orders")
    .aggregate(
      [
        {
          $match: {
            _id: {
              $in: orderIds,
            },
          },
        },
        {
          $project: {
            _id: 0,
            userId: { $substr: ["$_p_user", 6, -1] },
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
        {
          $unwind: "$user",
        },
        {
          $group: {
            _id: "$user._id",
            email: { $first: "$user.unverifiedEmail" },
          },
        },
      ],
      {
        allowDiskUse: false,
      },
    )
    .toArray();
}
