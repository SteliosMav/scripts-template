import { exportCsv } from "../lib/export-csv.js";

export async function getUsers({ db }) {
  const collection = db.collection("DashboardLogin");

  const json = await collection.find({}).limit(1).toArray();

  exportCsv(json, "data_3.csv");

  return json;
}
