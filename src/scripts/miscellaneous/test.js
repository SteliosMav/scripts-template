import { loggerMiddleware } from "./logger-middleware.js";

export function test() {
  loggerMiddleware(() => {
    const array = undefined;
    array.length;
  });
  loggerMiddleware(namedFunction);
}

function namedFunction() {
  // Cause intentional error
  const array = undefined;
  array.length;
}
