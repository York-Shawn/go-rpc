package tinyrpc

import (
	"net"
	"net/rpc"

	"github.com/York-Shawn/goRPC/codec"
	"github.com/York-Shawn/goRPC/serializer"
)

type Server struct {
	*rpc.Server
	serializer.Serializer
}

func (s *Server) Serve(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			continue
		}
		go s.Server.ServeCodec(codec.NewServerCodec(conn, s.Serializer))
	}
}
