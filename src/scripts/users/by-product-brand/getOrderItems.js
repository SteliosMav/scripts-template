import { getProducts } from "./getProducts.js";
import { getClients } from "./getClients.js";

export async function getOrderItems({ db }) {
  const [productPointers, clientIds] = await Promise.all([
    getProducts({ db }),
    getClients({ db }),
  ]);

  return await db
    .collection("OrderItem")
    .aggregate(
      [
        {
          $match: {
            status: "packaged",
            client: {
              $in: clientIds,
            },
            _p_product: {
              $in: productPointers,
            },
            substituteFor: {
              $exists: false,
            },
            _created_at: {
              $gte: new Date("2023-08-08T00:00:00.000+0000"),
            },
          },
        },
        {
          $project: {
            _id: 0,
            orderItemId: "$_id",
          },
        },
      ],
      {
        allowDiskUse: false,
      },
    )
    .toArray();
}
