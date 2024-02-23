/**
 * @description Average number of days a user takes between each of their first 5 orders,
 * in all of 2023 (Split per month, and per country). For example: in January, users
 * took an average of 3 days to make their 1st order, 7 days between their 1st & 2nd
 * orders, 4 days between their 2nd & 3rd orders, and so on until the 5th order.
 */

import { COUNTRY_NAMES } from "../../../utils/countryIds.js";
import { WriteFile } from "../../../lib/write-file.js";

const _getDruidQuery = (monthRange) => {
  let year = 2023;
  if (monthRange[1] === 13) {
    monthRange[1] = 1;
    year = 2024;
  }
  return {
    query: `
        SELECT countryId, "__time", userId, orderId, installationId
        FROM "OrdersSales"
        WHERE "__time" >= '2023-${monthRange[0]}-01'
        AND "__time" < '${year}-${monthRange[1]}-01'
        AND status = 'Completed'
`,
  };
};

export async function orderFrequencyOfUsersFirst5orders({
  db,
  druidHelper,
  Parse,
}) {
  // All orders in 2023
  const ordersFrequency = [];

  for (let i = 1; i <= 12; i++) {
    console.log("Month: " + i);
    const monthRange = [i, i + 1];
    const druidQuery = _getDruidQuery(monthRange);
    const druidRes = await druidHelper.fetchResultsSQL(druidQuery);
    const orderFrequency = _getOrdersFrequency(druidRes);
    ordersFrequency.push(orderFrequency);
    console.log(
      "Pushed order frequency for month: " +
        i +
        " - Array length:" +
        orderFrequency.length,
    );
  }

  console.log("Before concat: " + ordersFrequency.length);
  // Use concat to flatten the array
  const results = [].concat(...ordersFrequency);
  console.log("After concat: " + results.length);

  console.log("Before export");
  // Export Data
  WriteFile.CSV(results, "2024_02_14_order_frequency.csv");
  console.log("After export");
}

function _getOrdersFrequency(orders) {
  // Group the first 5 orders, of each user by country and month
  const orderMap = new Map();
  orders.forEach((order) => {
    const { userId, __time, countryId } = order;
    const date = new Date(__time);
    const month = date.getUTCMonth() + 1;
    const country = COUNTRY_NAMES[countryId];

    // If the country is not in the map, add it
    if (!orderMap.has(country)) {
      orderMap.set(country, new Map());
      orderMap.get(country).set(month, new Map());
      orderMap.get(country).get(month).set(userId, [date]);
    } else {
      // If the month is not in the map, add it
      if (!orderMap.get(country).has(month)) {
        orderMap.get(country).set(month, new Map());
        orderMap.get(country).get(month).set(userId, [date]);
      } else {
        // If the user is not in the map, add it
        if (!orderMap.get(country).get(month).has(userId)) {
          orderMap.get(country).get(month).set(userId, [date]);
        } else {
          // If the user has less than 5 orders, add the order
          if (orderMap.get(country).get(month).get(userId).length < 5) {
            orderMap.get(country).get(month).get(userId).push(date);
          }
        }
      }
    }
  });

  // Get sum of the first 5 order dates, for each user, in each month, in each country
  // and then get their average date. Lastly, push the results to an array
  const results = [];
  orderMap.forEach((countryMap, country) => {
    countryMap.forEach((monthMap, month) => {
      // Get the average date between the first 5 orders
      const sumOfMonthOrderDates = [[], [], [], [], []]; // in milliseconds
      monthMap.forEach((orders, userId) => {
        const userOrders = orders.sort((a, b) => a - b);
        userOrders.forEach((order, i) => {
          const dateInMs = new Date(order).getTime();
          sumOfMonthOrderDates[i].push(dateInMs);
        });
      });
      const averageOrderDates = [];
      sumOfMonthOrderDates.forEach((dates) => {
        const sum = dates.reduce((acc, date) => acc + date, 0);
        const avg = Math.round(sum / dates.length);
        if (isNaN(avg)) {
          console.log("AVG IS NaN: ", avg);
          return;
        }
        averageOrderDates.push(new Date(avg).toISOString().split("T")[0]);
      });

      // Get the average date between the 0 order and the first, the first and the second and so on...
      const dateIntervalsInDays = {
        "0-1": 0,
        "1-2": 0,
        "2-3": 0,
        "3-4": 0,
        "4-5": 0,
      };
      averageOrderDates.forEach((date, i) => {
        if (i === 0) {
          // Get the interval between 1 and date day
          const day = new Date(date).getUTCDate();
          dateIntervalsInDays["0-1"] = day - 1;
        } else {
          const dateInMs = new Date(date).getTime();
          const previousDateInMs = new Date(averageOrderDates[i - 1]).getTime();
          dateIntervalsInDays[`${i}-${i + 1}`] = Math.round(
            (dateInMs - previousDateInMs) / (1000 * 60 * 60 * 24),
          );
        }
      });

      // Create an object of the array for better representation in the CSV
      const averageOrderDatesObj = {};
      averageOrderDates.forEach(
        (date, i) => (averageOrderDatesObj[`order_${i + 1}`] = date),
      );

      // Push the results to an array
      results.push({
        country,
        month,
        ...averageOrderDatesObj,
        ...dateIntervalsInDays,
      });
    });
  });

  return results;
}
