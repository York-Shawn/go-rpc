package compressor

type CompressType int32

const (
	Raw CompressType = iota
	Gzip
	Zlib
)

var Compressors = map[CompressType]Compressor{
	Raw:  RawCompressor{},
	Gzip: GzipCompressor{},
	Zlib: ZlibCompressor{},
}

type Compressor interface {
	Zip([]byte) ([]byte, error)
	Unzip([]byte) ([]byte, error)
}
