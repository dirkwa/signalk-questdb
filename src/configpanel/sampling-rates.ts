// The per-path sampling rates as the panel edits them: one rule per line, a
// path pattern and a rate in ms, e.g. "environment.wind.* 0". The schema
// stores them as an object, pattern -> ms, and the order of the lines is the
// order the patterns are tried in.

export interface ParsedSamplingRates {
  rates: Record<string, number>;
  /** Lines that are not a pattern followed by a rate of 0 or more. */
  invalid: string[];
}

export function formatSamplingRates(
  rates: Record<string, number> | undefined,
): string {
  return Object.entries(rates ?? {})
    .map(([pattern, ms]) => `${pattern} ${ms}`)
    .join("\n");
}

export function parseSamplingRates(text: string): ParsedSamplingRates {
  const rates: Record<string, number> = {};
  const invalid: string[] = [];
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (line === "") continue;
    const parts = line.split(/\s+/);
    const ms = parts.length === 2 ? Number(parts[1]) : NaN;
    if (!Number.isFinite(ms) || ms < 0) {
      invalid.push(line);
      continue;
    }
    rates[parts[0]] = ms;
  }
  return { rates, invalid };
}
