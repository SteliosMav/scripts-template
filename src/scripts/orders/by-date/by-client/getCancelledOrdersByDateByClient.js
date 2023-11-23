import { WriteFile } from "../../../../lib/write-file.js";

export async function getCancelledOrdersByDateByClient({ db, druidHelper }) {
  const clientsMap = await _getClientsMap({ db });

  const clientIds = [...clientsMap.keys()];
  const query = _getDruidQueryByClientIds({ clientIds });

  const druidRes = await druidHelper.fetchResultsSQL(query);

  for (const row of druidRes) {
    const { clientId, ...rawOrder } = row;
    const { createdAtTs, canceledAtTs, ...order } = rawOrder;

    // Format dates coming from druid.
    order.createdAt = new Date(createdAtTs * 1000);
    order.canceledAt = new Date(canceledAtTs * 1000);

    const el = clientsMap.get(clientId);

    // Data from druid might not exist in mongo due to data sync issues.
    if (!el) continue;

    el.orders.push(order);
  }

  const data = [...clientsMap.values()];
  const flattenedData = data.flatMap(({ clientName, orders }) =>
    orders.map((order) => ({ clientName, ...order })),
  );
  WriteFile.CSV(flattenedData, "cancelled-orders-by-client.csv");
  return flattenedData.length;
}

function _getDruidQueryByClientIds({ clientIds }) {
  const clientIdsString = `'${clientIds.join("', '")}'`;

  return {
    query: `
            SELECT clientId, createdAtTs, orderCode, orderNumericId, canceledAtTs
            FROM "OrdersSales"
            WHERE status IN ('UserCanceled', 'ClientCanceled')
            AND __time >= '2023-10-01' AND __time < '2023-11-01'
            AND testClient !='true'
            AND clientId IN (${clientIdsString})
`,
  };
}

async function _getClientsMap({ db }) {
  const clientsRes = await db
    .collection("Clients")
    .aggregate([
      { $match: { displayName: "SuperCare Pharmacy" } },
      { $project: { name: 1 } },
    ])
    .toArray();

  const map = new Map();

  for (const client of clientsRes) {
    map.set(client._id, { clientName: client.name, orders: [] });
  }

  return map;
}
