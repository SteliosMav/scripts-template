export async function _getProductPointers({ db }) {
  const petShopRelatedCategoryIds = [
    "BgyfzEbp2Y", // Pet Care category ID
    "18pj3tIAsW", // Pet Essentials category ID
  ];
  const petShopRelatedCategoryPointers = petShopRelatedCategoryIds.map(
    (categoryId) => `Categories$${categoryId}`,
  );

  const productsRes = await db
    .collection("Products")
    .aggregate(
      [
        {
          $match: {
            _p_category: {
              $in: petShopRelatedCategoryPointers,
            },
          },
        },
        {
          $project: {
            productPointer: {
              $concat: ["Products$", "$_id"],
            },
            _id: 0,
          },
        },
      ],
      {
        allowDiskUse: false,
      },
    )
    .toArray();

  return productsRes.map(({ productPointer }) => productPointer);
}
