const CUSTOM_OPTION = {
  _id: "vIjTfERwiE",
  _p_client: "Clients$IMqpNSEKua",
  _p_product: "Products$UuchlbbNkE",
  choices: [
    {
      title: "test",
      mandatory: true,
      single: false,
      options: [
        {
          productOptionId: "3NcGRJiZDa",
          price: 12,
          active: true,
          status: "3",
        },
      ],
      localizedTitle: {
        ru: "russian",
        ar: "araabic",
        el: "greeek",
      },
      groupId: "1521977897697thwm",
      limit: 5,
      titleId: "e3JenT8xX2",
    },
    {
      title: "asd",
      mandatory: true,
      single: false,
      options: [
        {
          productOptionId: "3NcGRJiZDa",
          price: 23,
          active: true,
          status: 1,
        },
        {
          productOptionId: "XCh19ijnah",
          price: 25,
          active: false,
          status: 2,
        },
      ],
      groupId: "1600693815803oomh",
      titleId: "BRTBZI1vXL",
    },
  ],
  _created_at: "2018-03-22T13:29:05.289+0000",
  _updated_at: "2020-09-22T15:27:22.668+0000",
};

const customOptions = [CUSTOM_OPTION];

// Using Node v12+ with async/await syntax and mongodb
export async function test({ db, druidHelper, Parse }) {
  console.log("Before: ", JSON.stringify(customOptions));
  await updateCustomOptionChoiceTitles({ db, customOptions });
  console.log("After: ", JSON.stringify(customOptions));
}

async function updateCustomOptionChoiceTitles({ db = null, customOptions }) {
  // Check if the database connection exists
  const dbExists = db !== null;

  // Create a unique array of every customOption.choices.titleId
  const uniqueTitleIds = customOptions.reduce((acc, customOption) => {
    const choices = customOption.choices || [];
    choices.forEach((choice) => {
      if (choice.titleId) {
        acc.add(choice.titleId);
      }
    });
    return acc;
  }, new Set());

  // Get all ProductOptionClientsGroups titles from the database by _id
  if (!dbExists) {
    db = await Client.connect();
  }
  const titlesRes = await db
    .collection("ProductOptionClientsGroups")
    .aggregate([
      { $match: { _id: { $in: Array.from(uniqueTitleIds) } } },
      { $project: { _id: 1, groupTitle: 1, localizedValue: 1 } },
    ])
    .toArray();

  // Create a map of titleId to title
  const titlesMap = {};
  await titlesRes.forEach((el) => {
    titlesMap[el._id] = {
      title: el.groupTitle,
      localizedValue: el.localizedValue || undefined,
    };
  });

  // Update the customOption.choices.title with the associated title from the map
  customOptions.forEach((customOption) => {
    const choices = customOption.choices || [];
    choices.forEach((choice) => {
      if (choice.titleId) {
        choice.title = titlesMap[choice.titleId].title;
        if (titlesMap[choice.titleId].localizedValue) {
          Object.entries(titlesMap[choice.titleId].localizedValue).forEach(
            ([key, value]) => {
              if (value) {
                choice.localizedTitle[key] = value;
              }
            },
          );
        }
      }
    });
  });

  // Close the database connection
  if (!dbExists) {
    db.close();
  }
}
