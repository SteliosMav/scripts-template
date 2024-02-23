import { getDeliveryDetailsByProductBarcodes } from "./delivery-details/by-product-barcodes/getDeliveryDetailsByProductBarcodes.js";
import { getCancelledOrdersDueToWrongPricing } from "./orders/by-date/getCancelledOrdersDueToWrongPricing.js";
import { getUsersByProductCategory2 } from "./users/by-product-category/getUsersByProductCategory2.js";
import { getUsersByCompanyPurchase } from "./users/by-company-purchase/getUsersByCompanyPurchase.js";
import { getInstapointsUsageByQAByCountry } from "./instapoints-usage/getInstapointsUsageByQAByCountry.js";
import { getUsersByProductSKUPurchase } from "./users/product-purchase/getUsersByProductSKUPurchase.js";
import { getActiveUsersWithoutOrderLastMonth } from "./users/product-purchase/getActiveUsersWithoutOrderLastMonth.js";
import { getOrdersByCampaignCoupon } from "./orders/by-campaign-coupon/getOrdersByCampaignCoupon.js";
import { generateCoupons } from "./generate-coupons/generateCoupons.js";
import { clientsByCategoryBySubcategory } from "./clients/clientsByCategoryBySubcategory.js";
import { test } from "./test.js";
import { generateCoupons2 } from "./generate-coupons/generateCoupons2.js";
import { orderFrequencyOfUsersFirst5orders } from "./orders/by-frequency/orderFrequencyOfUsersFirst5orders.js";
import { getUsersByProductSKUPurchase2 } from "./users/product-purchase/getUsersByProductSKUPurchase2.js";
import { daysPastFromInstallationAfterFirstOrder } from "./orders/by-frequency/daysPastFromInstallationAfterFirstOrder.js";
import { getUsersByOrderComment } from "./users/by-order-comment/getUsersByOrderComment.js";

// Here we import all scripts that we want to run
const scripts = [getUsersByOrderComment];

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
