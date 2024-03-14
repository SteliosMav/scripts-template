export async function mergeDuplicateCustomOptionGroups({ db, Parse }) {
  // Get all custom option groups
  const groupsGroupedByTitleMap = {};
  let totalDocumentsCount = 0;

  const cursor = db.collection("ProductOptionClientsGroups").aggregate([]);
  for await (const group of cursor) {
    totalDocumentsCount++;
    const groupId = group._id;
    const groupTitle = group.groupTitle;
    const formattedGroupTitle = groupTitle.trim().toLowerCase();

    if (groupsGroupedByTitleMap[formattedGroupTitle]) {
      groupsGroupedByTitleMap[formattedGroupTitle].groupSDuplicates.push(
        groupId,
      );
    } else {
      groupsGroupedByTitleMap[formattedGroupTitle] = {
        id: groupId,
        originalTitle: groupTitle,
        groupSDuplicates: [],
      };
    }
  }

  const groupedDocumentsCount = Object.keys(groupsGroupedByTitleMap).length;

  // Merge duplicate titles
  for (const key in groupsGroupedByTitleMap) {
    const group = groupsGroupedByTitleMap[key];
    const originalGroupId = group.id;
    const groupSDuplicates = group.groupSDuplicates;

    for (const groupId of groupSDuplicates) {
      // Update all products that have the duplicate titles
      const productOperations = [];
      for await (const product of db
        .collection("ProductOptionClients")
        .find({ "choices.titleId": groupId })) {
        const choices = product.choices.map((choice) => {
          if (choice.titleId === groupId) {
            choice.titleId = originalGroupId;
          }
          return choice;
        });

        // await db.collection("ProductOptionClients").updateOne({ _id: product._id }, { $set: { choices } });
        productOperations.push({
          updateOne: {
            filter: { _id: product._id },
            update: { $set: { choices } },
          },
        });
      }
      if (productOperations.length > 0) {
        await db
          .collection("ProductOptionClients")
          .bulkWrite(productOperations);
      }

      // Delete the duplicate title
      await db
        .collection("ProductOptionClientsGroups")
        .deleteOne({ _id: groupId });
    }
  }
}
