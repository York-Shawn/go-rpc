package codec

import "errors"

var (
	InvalidSequenceError    = errors.New("invalid sequence number in response")
	UnexpectedChecksumError = errors.New("unexpected checksum")
	NotFoundCompressorError = errors.New("not find compressor")
)
