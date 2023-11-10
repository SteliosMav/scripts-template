import { WriteFile } from "../../../lib/write-file.js";
import { _getUsersByOrderIds } from "./_getUsersByOrderIds.js";
import { _getOrderIds } from "./_getOrderIds.js";
import { _getPetShopClientIds } from "./_getPetShopClientIds.js";
import { _getSuperMarketClientIds } from "./_getSuperMarketsClientIds.js";
import { _getProductPointers } from "./_getProductPointers.js";
import { _getPetShopOrderIds } from "./_getPetShopOrderIds.js";
import { _getSuperMarketOrderIds } from "./_getSuperMarketOrderIds.js";

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
    endDate.setMonth(startDate.getMonth() + monthsToAdd),
      console.log(startDate, endDate);

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

  const promisesResult = await Promise.all(promises);

  console.log(promisesResult);

  // const orderIds = [...petShopOrderIds, ...superMarketOrderIds];
  //
  // const users = await _getUsersByOrderIds({ db, orderIds });
  //
  // WriteFile.JSON(users, "index.json");
  // WriteFile.CSV(users, "index.csv");
  //
  // return users;
}
