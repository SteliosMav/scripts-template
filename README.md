# Function Initialization

This repository contains code for running functions using Node.js and MongoDB.

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

### Running the Functions

Before running the functions, you need to import all the functions you want to execute in the src/functions/index.js
file. Add
these functions to the "functions" array.

To run the functions, use the following command:

   ```bash
   npm start
   ```
