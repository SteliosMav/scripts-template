import { WriteFile } from "../../../lib/write-file.js";

const cashbackCampaignId = "pYKQ5P2fMN";
const newUserCouponId = "gHd1CuqNXG";

export async function getOrdersByCampaignCoupon({ db }) {
  // Get all coupons ids that are from the campaign
  const cashbackCouponsRes = await db
    .collection("Coupons")
    .aggregate([
      { $match: { fromMonetizations: { $in: [cashbackCampaignId] } } },
      { $project: { _id: 1 } },
    ])
    .toArray();
  const cashbackCouponIds = cashbackCouponsRes.map((c) => c._id);

  // Combine cashbackCouponIds with newUserCouponId
  const couponIds = [...cashbackCouponIds, newUserCouponId];

  // Get all order ids that used the coupon
  const usedCouponsRes = await db
    .collection("UsedCoupons")
    .aggregate([
      { $match: { couponId: { $in: couponIds } } },
      { $project: { _id: 0, orderId: 1 } },
    ])
    .toArray();
  const orderIds = usedCouponsRes.map((c) => c.orderId);

  // Get all orders that used the coupon
  const ordersRes = await db
    .collection("Orders")
    .aggregate([
      { $match: { _id: { $in: orderIds } } },
      {
        $project: {
          _id: 0,
          "Order ID": "$_id",
          "Order creation time": "$_created_at",
          "Bin number": "$onlinePaymentTransactionIds.creditCard.binNumber",
          "Total order value": "$price",
          "Discount applied": "$priceBreakDown.discountApplied",
        },
      },
    ])
    .toArray();

  WriteFile.CSV(ordersRes, "2024_01_22_orders-by-campaign.csv");
  // return results;
}
