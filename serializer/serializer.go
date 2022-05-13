package serializer

type SerializerType int32

type serializer interface {
	Marshal(message interface{}) ([]byte, error)
	Unmarshal(data []byte, message interface{}) error
}
