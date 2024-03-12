import { WriteFile } from "../../lib/write-file.js";
import { VERTICAL_IDS } from "../../utils/verticalIds.js";

/**
 Could you please create 70 unique coupon codes for an event with Gold’s Gym in Egypt :flag-eg:
 Each coupon code must include the word (FitFam)
 Each code will be unique but please let’s try to make them not too long. As short as possible.
 Valid for all users across Egypt
 Coupon value: EGP 100
 Expiration date Feb 28th, 2024
 Financed by InstaShop
 Needed by Tuesday Feb 13th
 Coupon breakdown:
 30 coupons valid on supermarkets only
 20 coupons valid on F&V only
 20 coupons valid on pet shops only
 */

const egyptSId = "vrii93Zwoj";

export async function generateCoupons2({ db, druidHelper, Parse }) {
  // Set constants
  const numberOfDocuments = 3;
  const expireDate = new Date(2024, 2, 15);
  const couponValue = 800 / 5;
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
    coupon.set("createdByName", "goldSGymPromotion");
    coupon.set("type", "cashbackCampaign_by_instashop");
    coupon.set("shopsPay", false);
    coupon.set("reason", "Promotion");
    coupon.set("identifier", "promotion");
    // coupon.set("businessTypesThatCouponIsValidFor", [VERTICAL_IDS.petShops]);
    coupon.set("value", couponValue); // EGP
    coupon.set("reasonFreeText", "goldSGymPromotion");
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
  WriteFile.CSV(
    couponsCsv,
    "2024_02_12_gold_s_gym_coupons_for_all_verticals.csv",
  );

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
  const prefixedCustomCode = `FitFam_${randomCustomCode}`;
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
  return result;
}
