package vertica

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

func TestDDL(t *testing.T) {
	suite.Run(t, new(DDLTestSuite))
}

func TestRows(t *testing.T) {
	suite.Run(t, new(RowsTestSuite))
}
