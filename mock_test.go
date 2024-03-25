package athena

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vx416/go-athenav/athenamock"
)

func TestMockQuery(t *testing.T) {
	mockAPI := athenamock.AthenaAPI{}
	EnableMockMode(&mockAPI)
	db, err := sql.Open("athena", "mock")
	require.NoError(t, err, "Open failed")

	MockQuery(&mockAPI, map[string]string{"id": "int", "name": "string"}, [][]string{{"1", "vic"}})
	rows, err := db.Query("SELECT id, name FROM test_table")
	require.NoError(t, err, "Query failed")
	id := 0
	name := ""
	scanOK := false
	for rows.Next() {
		err = rows.Scan(&id, &name)
		require.NoError(t, err, "Scan failed")
		t.Logf("id: %d, name: %s", id, name)
		require.Equal(t, 1, id, "id not match")
		require.Equal(t, "vic", name, "name not match")
		scanOK = true
	}
	require.True(t, scanOK, "No rows")
}
