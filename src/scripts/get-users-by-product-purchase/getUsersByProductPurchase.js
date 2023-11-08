import { exportCsv } from "../../lib/export-csv.js";
import { barcodes } from "./barcodes.js";

export async function getUsersByProductPurchase({ db }) {
  // const productPointersResponse = await db
  //   .collection("Products")
  //   .aggregate(
  //     [
  //       {
  //         $limit: 10000,
  //       },
  //       {
  //         $match: {
  //           barcodes: {
  //             $in: barcodes,
  //           },
  //         },
  //       },
  //       {
  //         $project: {
  //           productPointer: {
  //             $concat: ["Products$", "$_id"],
  //           },
  //           _id: 0,
  //         },
  //       },
  //     ],
  //     {
  //       allowDiskUse: false,
  //     },
  //   )
  //   .toArray();
  // const productPointers = productPointersResponse.map(
  //   ({ productPointer }) => productPointer,
  // );

  const ordersByProducts = await db
    .collection("OrderItem")
    .aggregate(
      [
        // {
        //   $limit: 10000,
        // },
        {
          $match: {
            _p_product: {
              $in: [
                "Products$41gnNtOdVV",
                "Products$6XN0swXfNw",
                "Products$6fAgC1pSIP",
                "Products$8p8IGlKkb8",
                "Products$AVF0xN2yBe",
                "Products$ChH3luDkD4",
                "Products$E4OgkmUTwF",
                "Products$EytYOVnZ4z",
                "Products$GkXZnvtovA",
                "Products$Gnern2HuVh",
                "Products$Hia8Dd1heh",
              ],
            },
          },
        },
      ],
      {
        allowDiskUse: false,
      },
    )
    .toArray();

  // exportCsv(productPointers, "data_3.csv");

  return ordersByProducts;
}
