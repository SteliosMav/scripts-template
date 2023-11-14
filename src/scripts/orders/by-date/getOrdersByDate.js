import { WriteFile } from "../../../lib/write-file.js";
import _ from "lodash";
// import aprilJson from "../../../../exported_data/orders_by_date_in_chunks_april.json" assert { type: "json" };
// import mayJson from "../../../../exported_data/orders_by_date_in_chunks_may.json" assert { type: "json" };
// import juneJson from "../../../../exported_data/orders_by_date_in_chunks_june.json" assert { type: "json" };
// import julyJson from "../../../../exported_data/orders_by_date_in_chunks_july.json" assert { type: "json" };
// import augustJson from "../../../../exported_data/orders_by_date_in_chunks_august.json" assert { type: "json" };
// import septemberJson from "../../../../exported_data/orders_by_date_in_chunks_september.json" assert { type: "json" };

import ordersByDateRawJson from "../../../../exported_data/orders_by_date_raw.json" assert { type: "json" };

const ordersByDateRaw = ordersByDateRawJson;

// const [april, may, june, july, august, september] = [
//   aprilJson,
//   mayJson,
//   juneJson,
//   julyJson,
//   augustJson,
//   septemberJson,
// ];
//
// const ordersArray = [
//   ...april[0],
//   ...may[0],
//   ...june[0],
//   ...july[0],
//   ...august[0],
//   ...september[0],
// ];

export async function getOrdersByDate({ db }) {
  // const promises = [];
  //
  // for (let i = 0; i < 6; i++) {
  //   promises.push(
  //     _getOrdersByDateRange({
  //       db,
  //       dateRange: _getMonthDateRange(2023, i + 4),
  //     }),
  //   );
  // }
  //
  // console.log("Requests start:", new Date().toJSON());
  // const res = await Promise.all(promises);
  // console.log("Requests end:", new Date().toJSON());
  // WriteFile.JSON(ordersArray, "orders_by_date_raw.json");

  const groupedData = _.groupBy(
    ordersByDateRaw,
    (item) => `${item.date}!${item.hour}`,
  );
  const countOnly = _.mapValues(groupedData, (group) => group.length);
  const formattedData = Object.entries(countOnly).map(([key, value]) => ({
    Date: key.split("!")[0],
    Hour: key.split("!")[1],
    "Number of Orders": value,
  }));
  formattedData.sort((a, b) => {
    const dateComparison = new Date(a.Date) - new Date(b.Date);
    if (dateComparison === 0) {
      // If dates are the same, compare by "Hour" as numbers
      return a.Hour - b.Hour;
    }
    return dateComparison;
  });

  WriteFile.CSV(formattedData, "orders_by_date_grouped.csv");
}

function _getMonthDateRange(year, month) {
  // Check if month is between 1 and 12
  if (month < 1 || month > 12) {
    throw new Error("Invalid month. Month should be between 1 and 12.");
  }

  // Month is 0-indexed
  const start = new Date(year, month - 1, 1);
  const end = new Date(year, month, 0, 23, 59, 59, 999);

  return { start, end };
}

async function _getOrdersByDateRange({ db, dateRange }) {
  const { start, end } = dateRange;
  const greeceSId = "mSkwRgn6gt";

  return db
    .collection("Orders")
    .aggregate([
      {
        $match: {
          status: "Completed",
          _created_at: {
            $gte: start,
            $lte: end,
          },
          _p_country: `Countries$${greeceSId}`,
        },
      },
      {
        $project: {
          _id: 0,
          _created_at: 1,
          date: {
            $concat: [
              { $toString: { $year: "$_created_at" } },
              "/",
              { $toString: { $month: "$_created_at" } },
              "/",
              { $toString: { $dayOfMonth: "$_created_at" } },
            ],
          },
          hour: { $hour: "$_created_at" },
        },
      },
    ])
    .toArray();
}
