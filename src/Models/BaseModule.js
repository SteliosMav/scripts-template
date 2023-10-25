export class BaseModule {
  constructor({ db }) {
    this.db = db;

    if (this.constructor === BaseModule) {
      throw new Error("Abstract classes can't be instantiated.");
    }
  }

  init() {
    throw new Error("Method 'init()' is missing");
  }
}
