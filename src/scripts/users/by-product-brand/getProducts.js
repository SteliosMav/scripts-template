export async function getProducts({ db }) {
  // const collection = await db.collection("Products");
  const res = await db
    .collection("Products")
    .aggregate(
      [
        {
          $match: {
            _p_category: "Categories$fJpd1nGn5m",
            _p_brand: {
              $in: [
                "Brands$5atNkCvCLe",
                "Brands$h23AW0Efxv",
                "Brands$jWJFKoySNJ",
                "Brands$kjUmvRJeBg",
              ],
            },
          },
        },
        {
          $project: {
            _id: 0,
            productPointer: {
              $concat: ["Products$", "$_id"],
            },
          },
        },
      ],
      {
        allowDiskUse: false,
      },
    )
    .toArray();

  return res.map(({ productPointer }) => productPointer);
}
