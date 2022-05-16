package codec

import (
	"bufio"
	"hash/crc32"
	"io"
	"net/rpc"
	"sync"

	"github.com/York-Shawn/goRPC/compressor"
	"github.com/York-Shawn/goRPC/header"
	"github.com/York-Shawn/goRPC/serializer"
)

type serverCodec struct {
	w io.Writer
	r io.Reader
	c io.Closer

	request    header.RequestHeader
	serializer serializer.Serializer
	mutex      sync.Mutex
	seq        uint64
	pending    map[uint64]uint64
}

func NewServerCode(conn io.ReadWriteCloser, serializer serializer.Serializer) rpc.ServerCodec {
	return &serverCodec{
		r: bufio.NewReader(conn),
		w: bufio.NewWriter(conn),
		c: conn,

		serializer: serializer,
		pending:    make(map[uint64]uint64),
	}
}

func (s *serverCodec) ReadRequestHeader(r *rpc.Request) error {
	s.request.ResetHeader()
	data, err := recvFrame(s.r)
	if err != nil {
		return err
	}

	err = s.request.UnMarshal(data)
	if err != nil {
		return err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.seq++
	s.pending[s.seq] = s.request.ID
	r.ServiceMethod = s.request.Method
	r.Seq = s.seq

	return nil
}

func (s *serverCodec) ReadRequestBody(param interface{}) error {
	if param == nil {
		if s.request.RequestLen != 0 {
			if err := read(s.r, make([]byte, s.request.RequestLen)); err != nil {
				return err
			}
		}
		return nil
	}

	reqBody := make([]byte, s.request.RequestLen)
	err := read(s.r, reqBody)
	if err != nil {
		return err
	}

	if s.request.Checksum != 0 {
		if crc32.ChecksumIEEE(reqBody) != s.request.Checksum {
			return UnexpectedChecksumError
		}
	}

	if _, ok := compressor.Compressors[compressor.CompressType(s.request.CompressType)]; !ok {
		return NotFoundCompressorError
	}

	req, err := compressor.Compressors[s.request.GetCompressType()].Unzip(reqBody)
	if err != nil {
		return err
	}

	return s.serializer.Unmarshal(req, param)
}

func (s *serverCodec) WriteResponse(r *rpc.Response, param interface{}) error {
	s.mutex.Lock()
	id, ok := s.pending[r.Seq]
	if !ok {
		s.mutex.Unlock()
		return InvalidSequenceError
	}
	delete(s.pending, r.Seq)
	s.mutex.Unlock()

	if r.Error != "" {
		param = nil
	}
	if _, ok := compressor.
		Compressors[s.request.GetCompressType()]; !ok {
		return NotFoundCompressorError
	}

	var respBody []byte
	var err error
	if param != nil {
		respBody, err = s.serializer.Marshal(param)
		if err != nil {
			return err
		}
	}

	compressedRespBody, err := compressor.
		Compressors[s.request.GetCompressType()].Zip(respBody)
	if err != nil {
		return err
	}
	h := header.ResponsePool.Get().(*header.ResponseHeader)
	defer func() {
		h.ResetHeader()
		header.ResponsePool.Put(h)
	}()
	h.ID = id
	h.Error = r.Error
	h.ResponseLen = uint32(len(compressedRespBody))
	h.Checksum = crc32.ChecksumIEEE(compressedRespBody)
	h.CompressType = s.request.CompressType

	if err = sendFrame(s.w, h.Marshal()); err != nil {
		return err
	}

	if err = write(s.w, compressedRespBody); err != nil {
		return err
	}
	s.w.(*bufio.Writer).Flush()
	return nil
}

func (s *serverCodec) Close() error {
	return s.c.Close()
}
