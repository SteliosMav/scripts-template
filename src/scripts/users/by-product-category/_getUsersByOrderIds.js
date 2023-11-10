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
        // THE GROUPING SHOULD HAVE BEEN DONE BEFORE THE LOOKUP ON THE ORDERS COLLECTION
        // OR NOT AT ALL SINCE I DO THE GROUPING ON IN THE END ON THE USERS CHUNKS FROM
        // ALL THE ORDER RESPONSES
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
