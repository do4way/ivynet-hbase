package ivyhbase

import "os"

//HBConfig ...
type HBConfig struct {
	zkquorum string
}

var defaultZkquorum = "localhost"

//ReadConfig ...
func ReadConfig() *HBConfig {

	zkquorum := os.Getenv("ZK_QUORUM")
	if zkquorum == "" {
		zkquorum = defaultZkquorum
	}
	return &HBConfig{
		zkquorum: zkquorum,
	}
}
