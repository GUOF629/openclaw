import pino from "pino";

export function createLogger(level: string) {
  return pino({
    level,
    redact: {
      paths: [
        "req.headers.authorization",
        "req.headers.cookie",
        "req.headers['x-api-key']",
        "body.password",
        "body.apiKey",
        "body.token",
        "NEO4J_PASSWORD",
        "QDRANT_API_KEY",
      ],
      remove: true,
    },
  });
}
