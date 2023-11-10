import { getOrderItems } from "./getOrderItems.js";
import { getUsers } from "./getUsers.js";
import { WriteFile } from "../../../lib/write-file.js";

export async function usersByProductBrand({ db }) {
  // const orderItems = await getOrderItems({ db });
  // WriteFile.JSON(orderItems, "orderItems.json");

  const users = await getUsers({ db });

  WriteFile.CSV(users, "users_by_product_brand.csv");

  return users;
}
