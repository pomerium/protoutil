// Copyright 2024 Joe Kralicky
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
