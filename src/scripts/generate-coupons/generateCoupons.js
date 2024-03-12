import { WriteFile } from "../../lib/write-file.js";

const egyptSId = "vrii93Zwoj";

export async function generateCoupons({ db, druidHelper, Parse }) {
  // Set constants
  const numberOfDocuments = 150;
  const expireDate = new Date(2024, 2, 15);
  const couponValue = 150 / 5;
  const coupons = [];

  // Fetch the country
  const egyptCountry = await new Parse.Query("Countries")
    .equalTo("objectId", egyptSId)
    .first();

  // Create and push the coupons
  for (let i = 0; i < numberOfDocuments; i++) {
    const coupon = new Parse.Object("Coupons");
    coupon.set("expires", expireDate);
    coupon.set("issuedCountry", egyptCountry);
    coupon.set("useCount", 0);
    coupon.set("quantity", 1);
    coupon.set("createdByName", "rahetBallyPromotion");
    coupon.set("type", "cashbackCampaign_by_instashop");
    coupon.set("shopsPay", false);
    coupon.set("reason", "Promotion");
    coupon.set("identifier", "promotion");
    coupon.set("value", couponValue); // EGP
    coupon.set("reasonFreeText", "rahetBallyPromotion");
    coupon.set("customCode", _generateCustomCode());
    coupons.push(coupon);
  }

  // Project to the CSV only customCode and value
  const couponsCsv = coupons.map((coupon) => ({
    customCode: coupon.get("customCode"),
    value: coupon.get("value"),
  }));

  // Validate customCode uniqueness
  _validateCustomCodeUniqueness(coupons);

  // Export the coupons to CSV
  WriteFile.CSV(couponsCsv, "2024_02_26_rahet_bally_coupons_150x150EGP.csv");

  // Save the coupons
  await Parse.Object.saveAll(coupons, { useMasterKey: true });
}

function _validateCustomCodeUniqueness(coupons) {
  const customCodes = coupons.map((coupon) => coupon.get("customCode"));
  const uniqueCustomCodes = [...new Set(customCodes)];
  if (uniqueCustomCodes.length !== coupons.length) {
    throw new Error("Custom codes are not unique");
  }
}

function _generateCustomCode() {
  const randomCustomCode = _generateRandomString(6);
  const prefixedCustomCode = `RB_${randomCustomCode}`;
  return prefixedCustomCode;
}

function _generateRandomString(length) {
  const characters =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let result = "";
  for (let i = 0; i < length; i++) {
    const randomIndex = Math.floor(Math.random() * characters.length);
    result += characters.charAt(randomIndex);
  }
  return result.toLowerCase();
}
