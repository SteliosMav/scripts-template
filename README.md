# Module Initialization

This repository contains code for initializing and running modules using Node.js and MongoDB.

## Getting Started

Before running the code, make sure to set up your environment and install the required dependencies.

### Prerequisites

- Node.js
- MongoDB
- npm (Node Package Manager)

### Installation

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd <repo-name>

2. Install dependencies:

   ```bash
   npm install

3. Set up your environment variables by creating a .env file and providing the required configurations. Ensure you have
   the DB_URI and DB_NAME variables properly set.

   ```bash
   DB_URI=mongodb://localhost:27017
   DB_NAME=your-database-name

### Running the Modules

Before running the modules, you need to import all the modules you want to execute in the src/modules/index.js file. Add
these modules to the "modules" array.

To run the modules and initialize them, use the following command:

   ```bash
   npm start
   ```

### Module Structure

Each module should extend from the base module class and implement the required methods. The base module class is
located in the `src/models/BaseModule.js` directory.
