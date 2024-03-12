import { WriteFile } from "../../lib/write-file.js";

const couponTypesMap = {
  // "Prospect-2": [],
  // "Prospect-3": [],
  // "Prospect-EG1": [],
  // "Prospect-EG2": [],
  // "Prospect-EG": [],
  // "WelcomeD3-AE": [],
  // "WelcomeD3-EG": [],
  // "WelcomeD10-2": [],
  // "WelcomeD10-EG": [],
  "Churn-30": [],
  "Churn-60": [],
  "Churn-90": [],
};
const couponTypes = Object.keys(couponTypesMap);

export async function getCouponRedemptionReport2({ db }) {
  // Populate couponTypesMap with coupon ids
  const countryPointers = ["Countries$ryFmc6ACd1", "Countries$vrii93Zwoj"];
  const coupons = await db
    .collection("Coupons")
    .aggregate([
      {
        $match: {
          _p_issuedCountry: { $in: countryPointers },
          useCount: { $gt: 0 },
          expires: { $gte: new Date("2024-02-01T00:00:00.000Z") },
          reason: { $in: couponTypes },
        },
      },
      { $project: { _id: 1, reason: 1 } },
    ])
    .toArray();
  coupons.forEach((coupon) => {
    const couponType = coupon.reason;
    couponTypesMap[couponType].push(coupon._id);
  });

  // For each coupon type, check _JoinerCouponRedemption if exists.
  // Filter each coupon type by coupons that exist in _JoinerCouponRedemption.
  for (let i = 0; i < couponTypes.length; i++) {
    const couponType = couponTypes[i];
    const couponIds = couponTypesMap[couponType];
    const couponRedemption = await db
      .collection("UsedCoupons")
      .aggregate([
        {
          $match: {
            couponId: {
              $in: couponIds,
            },
          },
        },
        {
          $lookup: {
            from: "Orders",
            localField: "orderId",
            foreignField: "_id",
            as: "order",
          },
        },
        {
          $unwind: "$order",
        },
        {
          $lookup: {
            from: "Clients",
            localField: "clientId",
            foreignField: "_id",
            as: "client",
          },
        },
        {
          $unwind: "$client",
        },
        {
          $lookup: {
            from: "Coupons",
            localField: "couponId",
            foreignField: "_id",
            as: "coupon",
          },
        },
        {
          $unwind: "$coupon",
        },
      ])
      .toArray();
    couponTypesMap[couponType] = couponRedemption;
  }

  const result = [];
  for (let i = 0; i < couponTypes.length; i++) {
    const couponType = couponTypes[i];
    const couponTypeData = couponTypesMap[couponType];
    couponTypeData.forEach((couponData) => {
      result.push({
        couponType,
        couponId: couponData.couponId,
        orderId: couponData.order._id,
        orderValue: couponData.order.price,
        clientName: couponData.client.name,
        couponValue: couponData.coupon.value,
        couponCreatedAt: couponData.coupon._created_at,
        couponRedeemedAt: couponData._created_at,
      });
    });
  }

  WriteFile.CSV(result, "2024_03_07_churn_coupon_redemption_report.csv");
}
