export async function getClients({ db }) {
  const res = await db
    .collection("Clients")
    .aggregate(
      [
        {
          $match: {
            _p_country: "Countries$ryFmc6ACd1",
            "businessType.grocery": true,
            closed: false,
            // launched: true,
            // test : { $ne: true }
          },
        },
        {
          $project: {
            _id: 0,
            clientId: "$_id",
          },
        },
      ],
      {
        allowDiskUse: false,
      },
    )
    .toArray();

  return res.map(({ clientId }) => clientId);
}
