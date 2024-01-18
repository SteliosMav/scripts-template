import _ from "lodash";
import { WriteFile } from "../../lib/write-file.js";

export async function getInstapointsUsageByQAByCountry({ db, druidHelper }) {
  let coupons = await fetchCoupons({ db });
  let couponIds = Object.keys(coupons);
  let orders = await fetchCouponsUsers({ db, couponIds });
  let userIds = Object.values(orders);
  let users = await fetchUsers({ db, userIds });

  console.log(
    `User id,Instapoints amount,Reason,Reason free text,QA Name,Date of issuance,Order id,Numeric id,Order date,Order amount,Vendor id,Vendor name,Rider id,CountryCode`,
  );

  const data = [];

  console.log("Orders", Object.entries(orders).length);

  let counter = 0;

  for (let [key, value] of Object.entries(orders)) {
    counter++;
    // if (counter > 100) {
    //   break;
    // }

    let orderId,
      orderDate,
      orderAmount,
      clientName,
      clientId,
      driverId,
      numericId,
      countryCode;

    if (!(key in coupons) || !users.includes(value)) continue;

    if (coupons[key].orderId) {
      let order = await fetchOrders({ db, orderId: coupons[key].orderId });
      console.log("Counter: ", counter);
      orderId = order?.orderId;
      orderDate = order?.["createdAt"] || "";
      orderAmount = order?.["finalPrice"] || "";
      clientName = order?.["client"]?.["name"] || "";
      clientId = order?.["client"]?.id || "";
      numericId = order?.["numericId"] || "";
      driverId =
        (order?.["assignedDriverAccount"] &&
          order?.["assignedDriverAccount"].id) ||
        "";
      countryCode = order?.["country"]?.["name"] || "";

      data.push({
        userId: value,
        instapointsAmount: coupons[key].value * coupons[key].quantity,
        reason: coupons[key].reason,
        reasonFreeText: coupons[key].reasonFreeText,
        qaName: coupons[key].createdByName,
        dateOfIssuance: coupons[key].createdAt,
        orderId: order?._id || "",
        orderDate: order?._created_at || "",
      });
    }
  }

  WriteFile.CSV(data, "2024_01_12_instapoints-usage-by-egypt.csv");
}

const fetchUsers = async ({ db, userIds }) => {
  const users = [];
  const query = {
    _id: { $in: [...userIds] },
  };
  const projection = {
    _id: 1,
    digitsAuth: 1,
  };

  const cursor = db.collection("_User").find(query).project(projection);
  for await (let user of cursor) {
    if (!user?.digitsAuth?.prefix) continue;

    // Filter by UAE
    // if (user.digitsAuth.prefix === "+971" || user.digitsAuth.prefix === "971") {
    //   users.push(user._id);
    // }

    // Filter by Egypt
    if (user.digitsAuth.prefix === "+20" || user.digitsAuth.prefix === "20") {
      users.push(user._id);
    }
  }

  return users;
};
const fetchCouponsUsers = async ({ db, couponIds }) => {
  const couponUsers = {};
  const query = {
    owningId: { $in: [...couponIds] },
  };
  const projection = {
    owningId: 1,
    relatedId: 1,
  };

  const cursor = db
    .collection("_Join:assignedTo:Coupons")
    .find(query)
    .project(projection);
  for await (let coupon of cursor) {
    couponUsers[coupon.owningId] = coupon.relatedId;
  }

  return couponUsers;
};

const fetchCoupons = async ({ db }) => {
  const coupounObj = {};
  const query = {
    _created_at: {
      $gt: new Date("2023-12-01T00:00:00.001Z"),
      $lt: new Date("2023-12-31T23:59:59.999Z"),
    },
    type: { $in: ["serviceFailure", "loyaltyReward"] },
  };
  const projection = {
    _id: 1,
    type: 1,
    value: 1,
    reason: 1,
    _p_issuedFor: 1,
    _p_serviceFailedOrder: 1,
    _created_at: 1,
    createdByName: 1,
    reasonFreeText: 1,
    quantity: 1,
  };

  const cursor = db
    .collection("Coupons", { readPreference: "secondaryPreferred" })
    .find(query)
    .project(projection);

  for await (let coupon of cursor) {
    let createdAt = coupon._created_at || null;
    let type = coupon.type || null;
    let value = coupon.value || null;
    let reason = coupon.reason || null;
    let createdByName = coupon.createdByName || null;
    let orderId =
      (coupon._p_serviceFailedOrder &&
        coupon._p_serviceFailedOrder.replace(/Orders\$/g, "")) ||
      null;
    let reasonFreeText = coupon.reasonFreeText || "";
    let quantity = coupon.quantity || null;

    if (createdByName === "Riya Berry" || createdByName === "Umar Adi")
      continue;
    if (reasonFreeText.includes("testing")) continue;
    if (reasonFreeText.includes("test")) continue;

    coupounObj[coupon._id] = {
      createdAt,
      type,
      value,
      quantity,
      reason,
      reasonFreeText: reasonFreeText, //.replace(/([^.@\s]+)(\.[^.@\s]+)*@([^.@\s]+\.)+([^.@\s]+)/,"")
      createdByName,
      orderId,
    };
  }

  return coupounObj;
};

const fetchOrders = async ({ db, orderId }) => {
  return db.collection("Orders").findOne({ _id: orderId });

  // return new Parse.Query("Orders")
  //   .equalTo("objectId", orderId)
  //   .select(
  //     "finalPrice",
  //     "externalId",
  //     "client",
  //     "client.name",
  //     "assignedDriverAccount",
  //     "numericId",
  //     "options",
  //     "country",
  //     "country.name",
  //   )
  //   .first({ useMasterKey: true });
};
