import MongoPackage from "mongodb";

const MongoClient = MongoPackage.MongoClient;

// Connection URL
const url = process.env.DB_URI;
const mongoClient = new MongoClient(url, { useUnifiedTopology: true });

function closeConnection() {
  mongoClient.close();
}

async function connectToDb() {
  await mongoClient.connect();
  console.log("Connected successfully to server");
  const dbName = process.env.DB_NAME;
  return mongoClient.db(dbName);
}

export { connectToDb, closeConnection };
