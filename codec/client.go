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

type clientCodec struct {
	r io.Reader
	w io.Writer
	c io.Closer

	compressor compressor.CompressType
	serializer serializer.Serializer
	response   header.ResponseHeader
	mutex      sync.Mutex
	pending    map[uint64]string
}

func (c *clientCodec) WriteRequest(r *rpc.Request, param interface{}) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.pending[r.Seq] = r.ServiceMethod

	if _, ok := compressor.Compressors[c.compressor]; !ok {
		return NotFoundCompressorError
	}

	reqBody, err := c.serializer.Marshal(param)
	if err != nil {
		return err
	}
	compressReqBody, err := compressor.Compressors[c.compressor].Zip(reqBody)
	if err != nil {
		return nil
	}
	h := header.RequestPool.Get().(*header.RequestHeader)
	defer func() {
		h.ResetHeader()
		header.RequestPool.Put(h)
	}()
	h.ID = r.Seq
	h.Method = r.ServiceMethod
	h.RequestLen = uint32(len(compressReqBody))
	h.CompressType = header.CompressType(c.compressor)
	h.Checksum = crc32.ChecksumIEEE(compressReqBody)

	if err := sendFrame(c.w, h.Marshal()); err != nil {
		return err
	}
	if err := write(c.w, compressReqBody); err != nil {
		return err
	}

	c.w.(*bufio.Writer).Flush()
	return nil
}

func (c *clientCodec) ReadResponseHeader(r *rpc.Response) error {
	c.response.ResetHeader()
	data, err := recvFrame(c.r)
	if err != nil {
		return err
	}
	err = c.response.UnMarshal(data)
	if err != nil {
		return err
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	r.Seq = c.response.ID
	r.Error = c.response.Error
	r.ServiceMethod = c.pending[r.Seq]
	delete(c.pending, r.Seq)

	return nil
}

func (c *clientCodec) ReadResponseBody(param interface{}) error {
	if param == nil {
		if c.response.ResponseLen != 0 {
			if err := read(c.r, make([]byte, c.response.ResponseLen)); err != nil {
				return err
			}
		}
		return nil
	}

	respBody := make([]byte, c.response.ResponseLen)
	err := read(c.r, respBody)
	if err != nil {
		return err
	}

	if c.response.Checksum != 0 {
		if crc32.ChecksumIEEE(respBody) != c.response.Checksum {
			return UnexpectedChecksumError
		}
	}

	if _, ok := compressor.Compressors[c.response.GetCompressType()]; !ok {
		return NotFoundCompressorError
	}

	resp, err := compressor.Compressors[c.response.GetCompressType()].Unzip(respBody)
	if err != nil {
		return err
	}

	return c.serializer.Unmarshal(resp, param)
}

func (c *clientCodec) Close() error {
	return c.c.Close()
}

func NewClientCodec(conn io.ReadWriteCloser,
	compressType compressor.CompressType, serializer serializer.Serializer) rpc.ClientCodec {
	return &clientCodec{
		r:          bufio.NewReader(conn),
		w:          bufio.NewWriter(conn),
		c:          conn,
		compressor: compressType,
		serializer: serializer,
		pending:    make(map[uint64]string),
	}
}
