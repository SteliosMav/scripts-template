import { BaseModule } from "../models/BaseModule.js";

export class GetUsers extends BaseModule {
  constructor({ db }) {
    super({ db });
  }

  async init() {
    const collection = this.db.collection("DashboardLogin");
    const findResult = await collection.find({}).limit(10).toArray();
    console.log("Found documents =>", findResult);
  }
}
