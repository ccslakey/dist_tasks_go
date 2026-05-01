package testredis

import "testing"

type Instance struct {
	Addr string
}

func Start(t *testing.T) Instance {
	t.Helper()
	t.Skip("TODO: start or connect to Docker-backed Redis for integration tests")
	return Instance{}
}
