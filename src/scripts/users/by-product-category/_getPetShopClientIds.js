export async function _getPetShopClientIds({ db }) {
  const petShopsBusinessTypeId = "QUrcgojK1S";

  const petShopClientIdsRes = await db
    .collection("Clients")
    .aggregate([
      {
        $match: {
          businessTypeIdsArray: { $elemMatch: { $eq: petShopsBusinessTypeId } },
        },
      },
      {
        $project: { _id: 1 },
      },
    ])
    .toArray();

  return petShopClientIdsRes.map(({ _id }) => _id);
}
