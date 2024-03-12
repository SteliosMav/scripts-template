import { WriteFile } from "../../lib/write-file.js";

// Coupons data
const couponsMap = new Map();
const getCouponIds = () => Array.from(couponsMap.keys());

// Coupon value is AED, so we need to multiply by 5 to get the value in EGP and divide by 5 to get the value in AED.
const countrySCurrencyMultiplier = 5;

export async function getCouponRedemptionReport3({ db }) {
  // Get all coupons
  const coupons = await db
    .collection("Coupons")
    .aggregate([
      {
        $match: {
          createdByName: "kidVentruePromotion",
          value: {
            $in: [
              50 / countrySCurrencyMultiplier,
              100 / countrySCurrencyMultiplier,
            ],
          },
        },
      },
      { $project: { value: 1 } },
    ])
    .toArray();
  coupons.forEach((coupon) => {
    couponsMap.set(coupon._id, {
      _id: coupon._id,
      value: coupon.value * countrySCurrencyMultiplier,
      assignedToUser: false,
      usedInOrder: false,
    });
  });
  const couponIds = getCouponIds();

  // Find all coupons that have been assigned to users
  const couponIdsAssignedToUsers = await db
    .collection("_Join:assignedTo:Coupons")
    .aggregate([
      {
        $match: {
          owningId: { $in: couponIds },
        },
      },
      { $project: { couponId: "$owningId" } },
    ])
    .toArray();
  couponIdsAssignedToUsers.forEach(({ couponId }) => {
    const coupon = couponsMap.get(couponId);
    coupon.assignedToUser = true;
    couponsMap.set(couponId, coupon);
  });

  // Find all coupons that have been used in orders
  const couponIdsUsedInOrders = await db
    .collection("_Join:usedBy:Coupons")
    .aggregate([
      {
        $match: {
          owningId: { $in: couponIds },
        },
      },
      { $project: { couponId: "$owningId" } },
    ])
    .toArray();
  couponIdsUsedInOrders.forEach(({ couponId }) => {
    const coupon = couponsMap.get(couponId);
    coupon.usedInOrder = true;
    couponsMap.set(couponId, coupon);
  });

  // Group coupons by value and count how many have been assigned to users and used in orders
  const couponRedemptionReportPerValue = {
    50: { value: 50, assignedToUser: 0, usedInOrder: 0 },
    100: { value: 100, assignedToUser: 0, usedInOrder: 0 },
  };
  couponsMap.forEach((coupon) => {
    const value = coupon.value;
    if (coupon.assignedToUser) {
      couponRedemptionReportPerValue[value].assignedToUser += 1;
    }
    if (coupon.usedInOrder) {
      couponRedemptionReportPerValue[value].usedInOrder += 1;
    }
  });
  const couponRedemptionReportPerValueArray = Object.values(
    couponRedemptionReportPerValue,
  );

  // Export data to CSV
  WriteFile.CSV(
    couponRedemptionReportPerValueArray,
    "2024-03-12-coupon-redemption-report-kid-ventrue.csv",
  );
}
