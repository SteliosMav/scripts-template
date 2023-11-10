export async function _getSuperMarketClientIds({ db }) {
  const superMarketsBusinessTypeId = "hJqhTfUKxl";

  const superMarketClientIdsRes = await db
    .collection("Clients")
    .aggregate([
      {
        $match: {
          businessTypeIdsArray: superMarketsBusinessTypeId,
        },
      },
      {
        $project: { _id: 1 },
      },
    ])
    .toArray();

  return superMarketClientIdsRes.map(({ _id }) => _id);
}
