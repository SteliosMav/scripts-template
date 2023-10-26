/**
 * Base class for modules.
 */
export class BaseModule {
  /**
   * Creates a new instance of a module.
   * @param {Object} options - The options for initializing the module.
   * @param {MongoClient} options.db - The MongoDB client instance.
   * @throws {Error} Throws an error if the class is instantiated directly (abstract class).
   */
  constructor({ db }) {
    this.db = db;

    /**
     * Initialize the module. This method should be implemented by derived classes.
     * @throws {Error} Throws an error indicating that the method is missing.
     */
    if (this.constructor === BaseModule) {
      throw new Error("Abstract classes can't be instantiated.");
    }
  }

  /**
   * Initializes the module.
   * @returns {Promise<unknown>} A promise that resolves the results to be logged.
   */
  init() {
    throw new Error("Method 'init()' is missing");
  }
}
