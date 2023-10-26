import { GetUsers } from "./GetUsers.js";
import { BaseModule } from "../models/BaseModule.js";

// Here we import all modules that we want to run
const modules = [GetUsers, GetUsers];

export async function initModules({ db }) {
  // Validate all modules before initializing any one of them
  modules.forEach((Module) => {
    if (!(Module.prototype instanceof BaseModule))
      throw "Every module should extend from the BaseModule.";
  });

  for (let i = 0; i < modules.length; i++) {
    const Module = modules[i];
    const isLastModule = i === modules.length - 1;
    console.log(`
----------------------------------------
    
## Running ${Module.name}...`);
    const moduleResponse = await new Module({ db }).init();
    console.log(`${Module.name} response: `, moduleResponse);
    console.log(`${Module.name} done.`);
    if (isLastModule)
      console.log(`
----------------------------------------
`);
  }
}
