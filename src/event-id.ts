/** Generates an event ID: 16 random bytes as 32 lowercase hex chars (same as Go). */
export function generateEventId(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  let hex = "";
  for (const b of bytes) {
    hex += b.toString(16).padStart(2, "0");
  }
  return hex;
}
