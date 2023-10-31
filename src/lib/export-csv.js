import { parse } from "json2csv";
import { writeFileSync } from "fs";

export function exportCsv(json, filename) {
  // Convert JSON data to CSV
  const csv = parse(json);

  // Save the CSV data to a local file
  const csvFileName = "exported_data/" + filename;
  writeFileSync(csvFileName, csv);
  console.log(`CSV data saved to ${csvFileName}`);

  return csv;
}
