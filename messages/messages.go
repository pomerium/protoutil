package messages

import (
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func New[T proto.Message]() T {
	var t T
	return t.ProtoReflect().New().Interface().(T)
}

func FieldByName[T proto.Message](name string) protoreflect.FieldDescriptor {
	var t T
	fields := t.ProtoReflect().Descriptor().Fields()
	for i, l := 0, fields.Len(); i < l; i++ {
		field := fields.Get(i)
		if strings.EqualFold(string(field.Name()), name) {
			return field
		}
	}
	return nil
}

func FieldIndexByName[T proto.Message](name string) int {
	var t T
	fields := t.ProtoReflect().Descriptor().Fields()
	for i, l := 0, fields.Len(); i < l; i++ {
		field := fields.Get(i)
		if strings.EqualFold(string(field.Name()), name) {
			return i
		}
	}
	return -1
}
