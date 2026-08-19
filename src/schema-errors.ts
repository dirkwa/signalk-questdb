// Recognising the QuestDB errors that mean "this column was never migrated
// in", as opposed to "the query is wrong" or "the database is unwell".
//
// Both history providers read tables whose schema grew over time: `value_kind`
// and `source` were added by later migrations, and an external QuestDB the
// plugin does not own may still be missing either. Reads therefore degrade
// rather than fail — but ONLY for these two errors. A timeout, a 5xx or a
// dropped connection must stay visible, because a caller cannot tell an
// honest "no sources here" apart from a swallowed failure.
//
// The strings are QuestDB's, verified against a live instance:
//   SELECT DISTINCT source FROM legacy      -> "Invalid column: source"
//   SELECT DISTINCT source FROM no_such_tbl -> "table does not exist [table=no_such_tbl]"

function message(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** True for the error QuestDB returns when `value_kind` has not been added yet. */
export function isMissingKindColumn(err: unknown): boolean {
  return /Invalid column:\s*value_kind\b/i.test(message(err));
}

/**
 * Same, for the `source` column (a later migration than `value_kind`, so each
 * can be missing independently of the other).
 */
export function isMissingSourceColumn(err: unknown): boolean {
  return /Invalid column:\s*source\b/i.test(message(err));
}

/**
 * True when the table itself is absent.
 *
 * Distinct from a missing column: a plugin that has not recorded any string
 * values yet simply has no `signalk_str`, which is not a schema fault and not
 * something to warn about.
 */
export function isMissingTable(err: unknown): boolean {
  return /table does not exist/i.test(message(err));
}
