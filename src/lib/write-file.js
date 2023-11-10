import { parse } from "json2csv";
import { writeFileSync } from "fs";

export class WriteFile {
  static JSON(data, filename) {
    const json = JSON.stringify(data);
    const exportedJSONFileName = "exported_data/" + filename;

    writeFileSync(exportedJSONFileName, json);

    console.log(`JSON data saved to ${exportedJSONFileName}`);

    return json;
  }

  static CSV(data, filename) {
    // Convert data to CSV
    const csv = parse(data);
    // Save the CSV data to a local file
    const csvFileName = "exported_data/" + filename;

    writeFileSync(csvFileName, csv);

    console.log(`CSV data saved to ${csvFileName}`);

    return csv;
  }
}
