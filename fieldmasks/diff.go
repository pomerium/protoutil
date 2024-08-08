// Copyright 2020-2023 SUSE, LLC
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

package fieldmasks

import (
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// Diff returns a FieldMask representing the fields that have changed between the old and new message.
// For repeated and map fields, the entire field is considered changed if any element has changed.
// Nested messages will be recursively compared to obtain a more fine-grained path if possible.
// The fieldmask returned is *not* normalized.
func Diff(old, new protoreflect.Message) *fieldmaskpb.FieldMask {
	var paths []string
	if old.Type().Descriptor() != new.Type().Descriptor() {
		panic("bug: cannot compare messages of different types")
	}
	diffFields(old, new, "", &paths)
	return &fieldmaskpb.FieldMask{Paths: paths}
}

func diffFields(oldMsg, newMsg protoreflect.Message, prefix string, paths *[]string) {
	fields := oldMsg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		hasOld, hasNew := oldMsg.Has(field), newMsg.Has(field)
		switch {
		case !hasOld && !hasNew:
			continue
		case hasOld != hasNew:
			// Add field to paths if the field has been added or removed
			*paths = append(*paths, prefix+string(field.Name()))

			if field.Kind() == protoreflect.MessageKind && !field.IsMap() && !field.IsList() {
				// Recursively check nested message fields
				if hasOld {
					msg := oldMsg.Get(field).Message()
					diffFields(msg, msg.Type().Zero(), prefix+string(field.Name())+".", paths)
				} else {
					msg := newMsg.Get(field).Message()
					diffFields(msg.Type().Zero(), msg, prefix+string(field.Name())+".", paths)
				}
			}
			continue
		}
		oldValue, newValue := oldMsg.Get(field), newMsg.Get(field)

		if field.Kind() == protoreflect.MessageKind && !field.IsMap() && !field.IsList() {
			// Recursively check nested message fields
			nestedPrefix := prefix + string(field.Name()) + "."
			prevNumFields := len(*paths)
			diffFields(oldValue.Message(), newValue.Message(), nestedPrefix, paths)
			if len(*paths) > prevNumFields {
				// nested fields were changed
				*paths = append(*paths, prefix+string(field.Name()))
			}
		} else if !oldValue.Equal(newValue) {
			// Add field to paths if the value has changed
			*paths = append(*paths, prefix+string(field.Name()))
		}
	}
}
