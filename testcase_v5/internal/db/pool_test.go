package db

import (
	"strings"
	"testing"

	"github.com/percona-cs/cs0055422-tc-idr/internal/config"
)

func TestDSNSocketShape(t *testing.T) {
	dsn := DSN(config.Endpoint{
		User: "root", Password: "", Schema: "byfile_tc",
		Socket: "/tmp/mysql.sock",
	})
	for _, want := range []string{
		"root@unix(/tmp/mysql.sock)/byfile_tc?",
		"parseTime=true",
		"multiStatements=false",
		"interpolateParams=false",
		"loc=UTC",
	} {
		if !strings.Contains(dsn, want) {
			t.Errorf("DSN missing %q: %s", want, dsn)
		}
	}
	if strings.Contains(dsn, ":@") {
		t.Errorf("DSN should omit empty password colon: %s", dsn)
	}
}

func TestDSNTCPShape(t *testing.T) {
	dsn := DSN(config.Endpoint{
		Host: "10.0.0.1", Port: 0, User: "tc_reader",
		Password: "p!2", Schema: "byfile_tc",
	})
	for _, want := range []string{
		"tc_reader:p!2@tcp(10.0.0.1:3306)/byfile_tc",
		"parseTime=true",
	} {
		if !strings.Contains(dsn, want) {
			t.Errorf("DSN missing %q: %s", want, dsn)
		}
	}
}
