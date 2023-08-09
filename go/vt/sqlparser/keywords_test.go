/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/


package sqlparser

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeywordTable(t *testing.T) {
	for _, kw := range keywords {
		lookup, ok := keywordLookupTable.LookupString(kw.name)
		require.Truef(t, ok, "keyword %q failed to match", kw.name)
		require.Equalf(t, lookup, kw.id, "keyword %q matched to %d (expected %d)", kw.name, lookup, kw.id)
	}
}

var vitessReserved = map[string]bool{
	"ESCAPE":        true,
	"NEXT":          true,
	"OFF":           true,
	"SAVEPOINT":     true,
	"SQL_NO_CACHE":  true,
	"TIMESTAMPADD":  true,
	"TIMESTAMPDIFF": true,
	"RELOAD":        true,
	"USERS":         true,
}

var mysql80Reserved = map[string]bool{
	"CHANGED": true,
	"FAST":    true,
	"MEDIUM":  true,
	"QUICK":   true,
}

func TestCompatibility(t *testing.T) {
	file, err := os.Open(path.Join("testdata", "mysql_keywords.txt"))
	require.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	skipStep := 4
	for scanner.Scan() {
		if skipStep != 0 {
			skipStep--
			continue
		}

		afterSplit := strings.SplitN(scanner.Text(), "\t", 2)
		word, reserved := afterSplit[0], afterSplit[1] == "1"
		if reserved || vitessReserved[word] || mysql80Reserved[word] {
			word = "`" + word + "`"
		}
		sql := fmt.Sprintf("create table %s(c1 int)", word)
		_, err := ParseStrictDDL(sql)
		if err != nil {
			t.Errorf("%s is not compatible with mysql", word)
		}
	}
}
