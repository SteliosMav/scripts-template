import { getDeliveryDetailsByProductBarcodes } from "./delivery-details/by-product-barcodes/getDeliveryDetailsByProductBarcodes.js";
import { getCancelledOrdersDueToWrongPricing } from "./orders/by-date/getCancelledOrdersDueToWrongPricing.js";
import { getUsersByProductCategory2 } from "./users/by-product-category/getUsersByProductCategory2.js";
import { getUsersByCompanyPurchase } from "./users/by-company-purchase/getUsersByCompanyPurchase.js";
import { getInstapointsUsageByQAByCountry } from "./instapoints-usage/getInstapointsUsageByQAByCountry.js";
import { getUsersByProductSKUPurchase } from "./users/product-purchase/getUsersByProductSKUPurchase.js";
import { getActiveUsersWithoutOrderLastMonth } from "./users/product-purchase/getActiveUsersWithoutOrderLastMonth.js";
import { getOrdersByCampaignCoupon } from "./orders/by-campaign-coupon/getOrdersByCampaignCoupon.js";
import { generateCoupons } from "./generate-coupons/generateCoupons.js";
import { test } from "./miscellaneous/test.js";

// Here we import all scripts that we want to run
const scripts = [test];

export async function runFunctions({ db, druidHelper, Parse }) {
  for (let i = 0; i < scripts.length; i++) {
    const fn = scripts[i];
    const fnName = fn.name || "Unknown";
    const isLastFn = i === scripts.length - 1;
    console.log(`
----------------------------------------
    
## Running ${fnName}...
`);
    const fnResponse = await fn({ db, druidHelper, Parse });
    if (fnResponse) console.log(`${fnName} response: `, fnResponse);
    console.log(`
${fnName} done.`);
    if (isLastFn)
      console.log(`
----------------------------------------
`);
  }
}
