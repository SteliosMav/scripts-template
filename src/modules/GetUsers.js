import { BaseModule } from "../models/BaseModule.js";

export class GetUsers extends BaseModule {
  constructor({ db }) {
    super({ db });
  }

  async init() {
    const collection = this.db.collection("DashboardLogin");
    debugger;
    return await collection.find().limit(1).toArray();
  }
}
