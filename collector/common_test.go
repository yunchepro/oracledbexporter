package collector

import (
	"testing"
)

var (
	testStrings = []string{
		"sorts (memory)",
		"sorts (disk)",
		"table scans (long tables)",
		"table scans (short tables)",
		"transaction rollbacks",
		"user commits",
		"redo synch time",
		"redo synch writes",
		"user calls",
		"SQL*Net roundtrips to/from client",
		"enq: TX - row lock contention",
	}
	testStringsTarget = []string{
		"sorts_memory",
		"sorts_disk",
		"table_scans_long_tables",
		"table_scans_short_tables",
		"transaction_rollbacks",
		"user_commits",
		"redo_synch_time",
		"redo_synch_writes",
		"user_calls",
		"sqlnet_roundtrips_tofrom_client",
		"enq_tx_row_lock_contention",
	}
)

func TestFormatLabel(t *testing.T) {

	for i, s := range testStrings {
		s2 := formatLabel(s)

		if s2 != testStringsTarget[i] {
			t.Fatalf("%s, %s, %s", s, s2, testStringsTarget[i])
		}

	}

}
