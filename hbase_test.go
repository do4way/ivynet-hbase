package ivyhbase

import (
	"fmt"

	. "gopkg.in/check.v1"
)

type HShellTestSuite struct {
	hs *HShell
}

var (
	_ = Suite(&HShellTestSuite{})
)

func (s *HShellTestSuite) SetUpSuite(c *C) {
	s.hs = NewHShell("test")
}

func (s *HShellTestSuite) TestPut(c *C) {
	err := s.hs.PutCell("k1", "cf", "text", "こんにちは")
	c.Assert(err, Equals, nil)
	rst, err := s.hs.GetCell("k1", "cf", "text")
	c.Assert(err, Equals, nil)
	c.Assert(len(rst), Equals, 1)
	c.Assert(string(rst[0].Value), Equals, "こんにちは")
	fmt.Println(rst[0].Timestamp)
	fmt.Println(rst[0].Timestamp.Unix())
}

func (s *HShellTestSuite) TestPutOnUnknowCell(c *C) {
	err := s.hs.PutCell("k2", "df1", "col1", "Hello world column!")
	c.Assert(err, Not(Equals), nil)
}
