// Note: This middleware logs errors to the server and bubbles them up to the
// next middleware error handler. It works parallel to the error handler and
// does not interfere with it.
export function loggerMiddleware(next) {
  try {
    // Call next middleware
    next();
  } catch (error) {
    // Get function name
    const functionName = next.name;

    // Get file path
    let filePath = undefined;
    const errorFilePathLine = error.stack.split("\n")[1].trim();
    const errorFilePathWords = errorFilePathLine.split(" ");
    const errorLineHas2Words = errorFilePathWords.length === 2;
    const errorLineHas3Words = errorFilePathWords.length === 3;
    // Error line does NOT include function name
    if (errorLineHas2Words) {
      filePath = errorFilePathWords[1];
    }
    // Error line includes function name
    if (errorLineHas3Words) {
      // If function name is included, file path will be in parentheses
      filePath = errorFilePathWords[2];
      // Remove parentheses
      const firstParenIndex = filePath.indexOf("(");
      const lastParenIndex = filePath.lastIndexOf(")");
      const bothParensPresent = firstParenIndex !== -1 && lastParenIndex !== -1;
      if (bothParensPresent) {
        filePath = filePath.substring(firstParenIndex + 1, lastParenIndex);
      }
    }

    // Log error
    JsonResponse.logger(error, { service: filePath, func: functionName });

    // Bubble up error
    throw error;

    // Call next middleware
    next();
  }
}
