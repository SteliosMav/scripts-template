import { WriteFile } from "../../lib/write-file.js";

export async function getClientCampaigns({ db, druidHelper }) {
  // Get campaign ids and names
  const ccCampaignIds = await db
    .collection("CCCampaigns")
    .aggregate([
      {
        $match: {
          _p_company: "Companies$ptwCbHbZxs",
          test: false,
        },
      },
      {
        $project: {
          campaignName: "$title",
        },
      },
    ])
    .toArray();
  const ccCampaignPointers = ccCampaignIds.map(({ _id }) => {
    return `CCCampaigns$${_id}`;
  });
  const ccCampaignsMap = ccCampaignIds.reduce((acc, { _id, campaignName }) => {
    acc[`CCCampaigns$${_id}`] = { campaignName };
    return acc;
  }, {});

  // Get the campaign areas (they have more information)
  const dateRange = {
    start: new Date("2024-01-01T02:00:00.000"),
    end: new Date("2024-02-26T01:59:59.999"),
  };
  const ccCampaignAreas = await db
    .collection("CCCampaignAreas")
    .aggregate([
      {
        $match: {
          _p_campaign: {
            $in: ccCampaignPointers,
          },
          active: false,
          start: {
            $gte: dateRange.start,
          },
          actualEnd: {
            $lte: dateRange.end,
          },
        },
      },
      {
        $addFields: {
          clientId: {
            $substr: ["$_p_client", 8, -1], // Assuming the prefix length is always 8 characters
          },
        },
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
        $project: {
          _id: 0,
          campaignName: "$title",
          clientName: "$client.name",
          budget: 1,
          budgetSpend: 1,
          dateRangeStart: "$start",
          dateRangeEnd: "$actualEnd",
          _p_campaign: 1,
        },
      },
    ])
    .toArray();

  // Group by campaign
  ccCampaignAreas.forEach((area) => {
    const associatedCampaign = ccCampaignsMap[area._p_campaign];
    if (!associatedCampaign) {
      console.log(area);
    }
    associatedCampaign.clientName = area.clientName;
    associatedCampaign.budget = (associatedCampaign.budget || 0) + area.budget;
    associatedCampaign.budgetSpend =
      (associatedCampaign.budgetSpend || 0) + area.budgetSpend;
    if (area.dateRangeStart) {
      associatedCampaign.dateRangeStart = area.dateRangeStart;
    }
    if (area.dateRangeEnd) {
      associatedCampaign.dateRangeEnd = area.dateRangeEnd;
    }
  });

  // Remove campaigns without areas
  const result = Object.values(ccCampaignsMap).filter(
    (campaign) => campaign.budgetSpend > 0,
  );

  const formattedResult = result.map((campaign) => {
    return {
      "Campaign Name": campaign.campaignName,
      "Client Name": campaign.clientName,
      Budget: campaign.budget,
      "Budget Spend": campaign.budgetSpend,
      "Exceeded Budget": campaign.budgetSpend - campaign.budget,
      "Date Range Start": campaign.dateRangeStart,
      "Date Range End": campaign.dateRangeEnd,
    };
  });

  WriteFile.CSV(formattedResult, "2024_03_05_client_campaigns.csv");
}
