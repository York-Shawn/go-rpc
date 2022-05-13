package compressor

import (
	"bytes"
	"compress/zlib"
	"io"
	"io/ioutil"
)

type ZlibCompressor struct {
}

func (ZlibCompressor) Zip(data []byte) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	w := zlib.NewWriter(buf)
	defer w.Close()

	_, err := w.Write(data)
	if err != nil {
		return nil, err
	}

	err = w.Flush()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

func (ZlibCompressor) Unzip(data []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewBuffer(data))
	defer r.Close()
	if err != nil {
		return nil, err
	}

	data, err = ioutil.ReadAll(r)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	return data, nil
}
