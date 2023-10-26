export async function getUsers({ db }) {
  const collection = db.collection("DashboardLogin");
  return await collection.find({}).limit(1).toArray();
}
