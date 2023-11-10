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
  const sixMonthsAgoDate = new Date(
    currentDate.setMonth(currentDate.getMonth() - 6),
  );

  const [petShopOrderIds, superMarketOrderIds] = await Promise.all([
    _getPetShopOrderIds({ db, dateGte: sixMonthsAgoDate, petShopClientIds }),
    _getSuperMarketOrderIds({
      db,
      dateGte: sixMonthsAgoDate,
      superMarketClientIds,
      productPointers,
    }),
  ]);
  const orderIds = [...petShopOrderIds, ...superMarketOrderIds];

  const users = await _getUsersByOrderIds({ db, orderIds });

  WriteFile.JSON(users, "index.json");
  WriteFile.CSV(users, "index.csv");

  return users;
}
