import { DruidHelper } from "../../../../lib/druid.js";
import { WriteFile } from "../../../../lib/write-file.js";
import moment from "moment";

const druidHelper = new DruidHelper();

export async function getOrdersByDateByClient({ db }) {
  const clientsMap = await _getClientsMap({ db });

  const clientIds = [...clientsMap.keys()];
  const query = _getDruidQueryByClientIds({ clientIds });

  const druidRes = await druidHelper.fetchResultsSQL(query);

  for (const row of druidRes) {
    const { clientId, ...rawOrder } = row;
    const { createdAtTs, ...order } = rawOrder;

    // Format dates coming from druid.
    const createdAt = moment.unix(createdAtTs);
    const formattedDate = createdAt.format("YYYY-MM-DD");
    order.createdAt = formattedDate;

    const el = clientsMap.get(clientId);

    // Data from druid might not exist in mongo due to data sync issues.
    if (!el) continue;

    el.orders.push(order);
  }

  const data = [...clientsMap.values()];
  const flattenedData = data.flatMap(({ clientName, orders }) =>
    orders.map((order) => ({ clientName, ...order })),
  );
  flattenedData.sort((a, b) => a.createdAt.localeCompare(b.createdAt));

  WriteFile.CSV(flattenedData, "2023_11_22_orders-by-date-by-client.csv");

  return flattenedData.length;
}

function _getDruidQueryByClientIds({ clientIds }) {
  const clientIdsString = druidHelper.getArrayAsString(clientIds);
  return {
    query: `
            SELECT clientId, createdAtTs, orderCode, orderNumericId
            FROM "OrdersSales"
            WHERE status ='Completed'
            AND __time >= '2023-10-12' AND __time <= '2023-11-12'
            AND testClient !='true'
            AND clientId IN (${clientIdsString})
`,
  };
}

async function _getClientsMap({ db }) {
  const clientsRes = await db
    .collection("Clients")
    .aggregate([
      { $match: { displayName: "Organic Foods & Cafe" } },
      { $project: { name: 1 } },
    ])
    .toArray();

  const map = new Map();

  for (const client of clientsRes) {
    map.set(client._id, { clientName: client.name, orders: [] });
  }

  return map;
}
