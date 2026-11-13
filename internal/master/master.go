package master

import "time"

type Master struct {
	nr     *NodeRegistry
	ticker time.Ticker
	grpc   *RPCServer
}
