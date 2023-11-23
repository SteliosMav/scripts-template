import { WriteFile } from "../../../../lib/write-file.js";
import moment from "moment";
import _ from "lodash";

export async function getSumOfDailyOrdersByDateByClientAndAvgOrdersPerDay({
  db,
  druidHelper,
}) {
  const clientsMap = await _getClientsMap({ db });

  const clientIds = [...clientsMap.keys()];
  const query = _getDruidQueryByClientIds({ clientIds, druidHelper });

  const druidRes = await druidHelper.fetchResultsSQL(query);

  for (const { clientId, createdAtTs } of druidRes) {
    // Format dates coming from druid.
    const createdAt = moment.unix(createdAtTs);
    const formattedDate = createdAt.format("YYYY-MM-DD");

    const el = clientsMap.get(clientId);

    // Data from druid might not exist in mongo due to data sync issues.
    if (!el) continue;

    el.orderDates.push(formattedDate);
  }

  const data = [...clientsMap.values()];
  const averageOrders = _calculateAvgOrdersPerDay(data);
  const formattedDataForCSV = _formatAvgOrdersPerDayForCSV(averageOrders);

  WriteFile.CSV(
    formattedDataForCSV,
    "2023_11_22_orders-by-date-by-client_3.csv",
  );

  return formattedDataForCSV.length;
}

function _getDruidQueryByClientIds({ clientIds, druidHelper }) {
  const clientIdsString = druidHelper.getArrayAsString(clientIds);
  return {
    query: `
            SELECT clientId, createdAtTs
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
      {
        $match: {
          displayName: "Organic Foods & Cafe",
        },
      },
      { $project: { name: 1 } },
    ])
    .toArray();

  const map = new Map();

  for (const client of clientsRes) {
    map.set(client._id, { clientName: client.name, orderDates: [] });
  }

  return map;
}

function _calculateAvgOrdersPerDay(clients) {
  return _.map(clients, (client) => {
    const ordersPerDay = _.countBy(client.orderDates);
    const totalDays = _.size(ordersPerDay);
    const totalOrders = client.orderDates.length;
    const averageOrders = Math.round(totalOrders / totalDays);

    return {
      clientName: client.clientName,
      averageOrdersPerDay: averageOrders,
      ordersPerDay,
    };
  });
}

function _formatAvgOrdersPerDayForCSV(averageOrders) {
  const data = averageOrders.flatMap(
    ({ clientName, ordersPerDay, averageOrdersPerDay }) =>
      _.map(ordersPerDay, (orders, date) => ({
        clientName,
        clientSAverageOrdersPerDay: averageOrdersPerDay,
        orderDate: date,
        numberOfOrders: orders,
      })),
  );

  data.sort((a, b) => a.orderDate.localeCompare(b.orderDate));

  return data;
}
