import { WriteFile } from "../../../lib/write-file.js";
import { _getUsersByOrderIds } from "./_getUsersByOrderIds.js";
import { _getPetShopClientIds } from "./_getPetShopClientIds.js";
import { _getSuperMarketClientIds } from "./_getSuperMarketsClientIds.js";
import { _getProductPointers } from "./_getProductPointers.js";
import { _getPetShopOrderIds } from "./_getPetShopOrderIds.js";
import { _getSuperMarketOrderIds } from "./_getSuperMarketOrderIds.js";
import _ from "lodash";

export async function getUsersByProductCategory({ db }) {
  const [petShopClientIds, superMarketClientIds, productPointers] =
    await Promise.all([
      _getPetShopClientIds({ db }),
      _getSuperMarketClientIds({ db }),
      _getProductPointers({ db }),
    ]);

  const currentDate = new Date();

  const promises = [];

  for (let i = 0; i < 6; i++) {
    const monthsToSubtract = i + 1;
    const monthsToAdd = 1;

    const startDate = new Date(currentDate);
    startDate.setMonth(currentDate.getMonth() - monthsToSubtract);
    const endDate = new Date(startDate);
    endDate.setMonth(startDate.getMonth() + monthsToAdd);
    // console.log(startDate, endDate);

    promises.push(
      _getPetShopOrderIds({
        db,
        dateGte: startDate,
        dateLte: endDate,
        petShopClientIds,
      }),
    );
    promises.push(
      _getSuperMarketOrderIds({
        db,
        dateGte: startDate,
        dateLte: endDate,
        superMarketClientIds,
        productPointers,
      }),
    );
  }

  console.log("Fetching orderIds...", new Date().toJSON());
  const promisesResult = await Promise.all(promises);
  console.log("PromisesResult: ", new Date().toJSON(), promisesResult);

  // SHOULD HAVE USED LODASH'S FLATTEN INSTEAD OF CONCAT APPLY
  const orderIds = Array.prototype.concat.apply([], promisesResult);
  console.log("orderIds: ", orderIds);

  const uniqueOrderIds = _.uniq(orderIds);
  console.log("orderIds after uniqueBy: ", uniqueOrderIds);

  const orderIdsChunks = _.chunk(uniqueOrderIds, 10000);
  const getUsersByOrderIdsPromises = orderIdsChunks.map((orderIdsChunk) => {
    return _getUsersByOrderIds({ db, orderIds: orderIdsChunk });
  });
  console.log("Start: getUsersByOrderIdsPromises: ", new Date().toJSON());
  const usersChunks = await Promise.all(getUsersByOrderIdsPromises);
  console.log(
    "Finish: getUsersByOrderIdsPromises: ",
    new Date().toJSON(),
    usersChunks,
  );

  const duplicateUsers = Array.prototype.concat.apply([], usersChunks);
  console.log("Users count before unique: ", duplicateUsers.length);
  const uniqueUsers = _.uniqBy(duplicateUsers, (user) => user._id);
  console.log("Users count after unique: ", uniqueUsers.length);
  const users = uniqueUsers;

  WriteFile.JSON(users, "users-by-product-category.json");
  WriteFile.CSV(users, "users-by-product-category.csv");

  return users;
}
