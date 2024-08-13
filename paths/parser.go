// Copyright 2024 Pomerium
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

package paths

import (
	"errors"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

var ErrFieldNotFound = errors.New("field not found")

var globalParser Parser

// A Parser contains a cache and an optional custom resolver. The methods
// [Parse] and [ParseFrom] operate on a global instance of the parser, but
// separate Parser instances can be used instead for specific use cases.
type Parser struct {
	byDescriptor sync.Map

	// Custom message type resolver. If nil, uses protoregistry.GlobalTypes.
	Resolver protoregistry.MessageTypeResolver
}

// Parses a path string with an implied root given by the message descriptor.
//
// The path string itself should not contain the root step; it should instead
// begin with the '.' character that would usually follow it. To parse a full
// path string containing the root step, use [Parse] instead.
func ParseFrom(root protoreflect.MessageDescriptor, pathStr string) (protopath.Path, error) {
	return globalParser.ParseFrom(root, pathStr)
}

// Parses a protopath string as given by the [protopath.Path.String] method.
//
// Repeated calls to this method with the same string will return the previous
// cached result. If the previous result returned an error, the same error will
// be returned. Thus, care should be taken to avoid directly modifying the
// underlying elements of the returned path.
func Parse(pathStr string) (protopath.Path, error) {
	return globalParser.Parse(pathStr)
}

func (p *Parser) Parse(pathStr string) (protopath.Path, error) {
	if len(pathStr) == 0 {
		return nil, errors.New("empty path")
	}
	if pathStr[0] != '(' {
		return nil, errors.New("path must start with root step")
	}
	root, remainder, err := p.splitRoot(pathStr)
	if err != nil {
		return nil, err
	}
	return p.ParseFrom(root, remainder)
}

func (p *Parser) ParseFrom(root protoreflect.MessageDescriptor, pathStr string) (protopath.Path, error) {
	if len(pathStr) == 0 {
		return nil, errors.New("empty path")
	}
	if pathStr[0] != '.' {
		return nil, errors.New("path must start with '.' (root step is omitted)")
	}
	pathStr = pathStr[1:]

	byPathV, ok := p.byDescriptor.Load(root)
	if !ok {
		byPathV, _ = p.byDescriptor.LoadOrStore(root, &sync.Map{})
	}
	fn, ok := byPathV.(*sync.Map).Load(pathStr)
	if !ok {
		fn, _ = byPathV.(*sync.Map).LoadOrStore(pathStr, sync.OnceValues(func() (protopath.Path, error) {
			return p.parse(root, pathStr)
		}))
	}
	return fn.(func() (protopath.Path, error))()
}

func (p *Parser) parse(root protoreflect.MessageDescriptor, pathStr string) (protopath.Path, error) {
	result := protopath.Path{protopath.Root(root)}
	for part := range Split(pathStr) {
		if err := p.parsePart(part, &result); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (p *Parser) parsePart(part string, result *protopath.Path) error {
	currentStep := result.Index(-1)
	if len(part) == 0 {
		return errors.New("path contains empty step")
	}
	switch {
	case part[0] == '(' && part[len(part)-1] == ')':
		// AnyExpand
		var fd protoreflect.FieldDescriptor
		switch currentStep.Kind() {
		case protopath.FieldAccessStep:
			// someAnyField.(pkg.Type)
			fd = currentStep.FieldDescriptor()
		case protopath.ListIndexStep:
			// someRepeatedAnyField[index].(pkg.Type)
			fd = result.Index(-2).FieldDescriptor()
		case protopath.MapIndexStep:
			// someRepeatedAnyField["key"].(pkg.Type)
			fd = result.Index(-2).FieldDescriptor().MapValue()
		}
		if fd != nil {
			if fd.Kind() != protoreflect.MessageKind ||
				(fd.IsList() && currentStep.Kind() != protopath.ListIndexStep) ||
				(fd.IsMap() && currentStep.Kind() != protopath.MapIndexStep) ||
				fd.Message().FullName() != "google.protobuf.Any" {
				// envoy doesn't have any proto2 extensions, and we don't need to reference options
				return fmt.Errorf("can only expand fields of type google.protobuf.Any, not %s", kindStr(fd))
			}
		} else if currentStep.Kind() != protopath.AnyExpandStep {
			return fmt.Errorf("unexpected type expansion after %s step", currentStep.Kind())
		}

		msgName := protoreflect.FullName(part[1 : len(part)-1])
		if !msgName.IsValid() {
			if part[1] == '.' {
				return fmt.Errorf("invalid message type '%s' (try removing the leading dot)", part[1:len(part)-1])
			}
			return fmt.Errorf("invalid message type '%s'", part[1:len(part)-1])
		}
		if msgt, err := protoregistry.GlobalTypes.FindMessageByName(msgName); err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				return fmt.Errorf("message type %s not found", msgName)
			}
			return fmt.Errorf("error looking up message type %s: %w", msgName, err)
		} else {
			*result = append(*result, protopath.AnyExpand(msgt.Descriptor()))
		}
	case part[0] == '[' && part[len(part)-1] == ']':
		// either ListIndex or MapIndex
		switch currentStep.Kind() {
		case protopath.FieldAccessStep:
			fd := currentStep.FieldDescriptor()
			if fd.IsList() {
				idx, err := strconv.ParseInt(part[1:len(part)-1], 10, 32)
				if err != nil {
					return fmt.Errorf("invalid list index: %w", err)
				}
				*result = append(*result, protopath.ListIndex(int(idx)))
			} else if fd.IsMap() {
				key := part[1 : len(part)-1]
				switch fd.MapKey().Kind() {
				case protoreflect.StringKind:
					if len(key) == 0 {
						return errors.New("empty map key")
					}
					unquoted, err := strconv.Unquote(key)
					if err != nil {
						return fmt.Errorf("invalid string map key '%s': %w", key, err)
					}
					*result = append(*result, protopath.MapIndex(protoreflect.ValueOfString(unquoted).MapKey()))
				case protoreflect.BoolKind:
					switch key {
					case "true":
						*result = append(*result, protopath.MapIndex(protoreflect.ValueOfBool(true).MapKey()))
					case "false":
						*result = append(*result, protopath.MapIndex(protoreflect.ValueOfBool(false).MapKey()))
					default:
						return fmt.Errorf("invalid map key '%s' for bool field '%s'", key, fd.FullName())
					}
				case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
					idx, err := strconv.ParseInt(key, 10, 32)
					if err != nil {
						return fmt.Errorf("invalid map key '%s': %w", key, err)
					}
					*result = append(*result, protopath.MapIndex(protoreflect.ValueOfInt32(int32(idx)).MapKey()))
				case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
					idx, err := strconv.ParseInt(key, 10, 64)
					if err != nil {
						return fmt.Errorf("invalid map key '%s': %w", key, err)
					}
					*result = append(*result, protopath.MapIndex(protoreflect.ValueOfInt64(idx).MapKey()))
				case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
					idx, err := strconv.ParseUint(key, 10, 32)
					if err != nil {
						return fmt.Errorf("invalid map key '%s': %w", key, err)
					}
					*result = append(*result, protopath.MapIndex(protoreflect.ValueOfUint32(uint32(idx)).MapKey()))
				case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
					idx, err := strconv.ParseUint(key, 10, 64)
					if err != nil {
						return fmt.Errorf("invalid map key '%s': %w", key, err)
					}
					*result = append(*result, protopath.MapIndex(protoreflect.ValueOfUint64(idx).MapKey()))
				}
			} else {
				return fmt.Errorf("cannot index %s field '%s'", fd.Kind().String(), fd.FullName())
			}
		default:
			return fmt.Errorf("cannot index '%s'", currentStep.String())
		}
	case part[0] == '?' && len(part) == 1:
		// UnknownAccess
		return fmt.Errorf("unknown field access not supported")
	default:
		// FieldAccess
		var msg protoreflect.MessageDescriptor
		switch currentStep.Kind() {
		case protopath.RootStep, protopath.AnyExpandStep:
			msg = currentStep.MessageDescriptor()
		case protopath.FieldAccessStep:
			fd := currentStep.FieldDescriptor()
			if fd.Kind() == protoreflect.MessageKind {
				msg = fd.Message()
			}
		case protopath.ListIndexStep:
			prev := result.Index(-2)
			switch prev.Kind() {
			case protopath.FieldAccessStep:
				fd := prev.FieldDescriptor()
				if fd.Kind() == protoreflect.MessageKind {
					msg = fd.Message()
				}
			}
		case protopath.MapIndexStep:
			prev := result.Index(-2)
			switch prev.Kind() {
			case protopath.FieldAccessStep:
				fd := prev.FieldDescriptor()
				if fd.MapValue().Kind() == protoreflect.MessageKind {
					msg = fd.MapValue().Message()
				}
			}
		}
		if msg != nil {
			field := msg.Fields().ByName(protoreflect.Name(part))
			if field == nil {
				if msg.FullName() == "google.protobuf.Any" {
					return fmt.Errorf("%w: '%s' in message %s (missing type expansion?)", ErrFieldNotFound, part, msg.FullName())
				}
				return fmt.Errorf("%w: '%s' in message %s", ErrFieldNotFound, part, msg.FullName())
			}
			*result = append(*result, protopath.FieldAccess(field))
		} else {
			return fmt.Errorf("cannot access field '%s' of non-message type", part)
		}
	}
	return nil
}

func kindStr(fd protoreflect.FieldDescriptor) string {
	switch fd.Kind() {
	case protoreflect.MessageKind:
		if fd.IsList() {
			return fmt.Sprintf("repeated %s", fd.Message().FullName())
		} else if fd.IsMap() {
			return fmt.Sprintf("map<%s, %s>", fd.MapKey().Kind().String(), kindStr(fd.MapValue()))
		}
		return string(fd.Message().FullName())
	default:
		return fd.Kind().String()
	}
}

// Split returns an iterator that yields each segment of a protopath string in
// order. It splits the path by '.' or '[' except within parentheses or quotes.
func Split(pathStr string) iter.Seq[string] {
	pathStr = strings.TrimSpace(pathStr)
	return func(yield func(string) bool) {
		start := 0
		var withinParens bool
		var withinString rune
		for i, rn := range pathStr {
			switch rn {
			case '(':
				if withinString == 0 {
					withinParens = true
				}
			case ')':
				if withinString == 0 {
					withinParens = false
				}
			case '"', '\'':
				switch withinString {
				case rn:
					withinString = 0
				case 0:
					withinString = rn
				}
			case '.':
				if withinParens || withinString != 0 {
					continue
				}
				if !yield(pathStr[start:i]) {
					return
				}
				start = i + 1
			case '[':
				if withinParens || withinString != 0 {
					continue
				}
				if i-start > 0 {
					if !yield(pathStr[start:i]) {
						return
					}
					start = i
				}
			}
		}
		if len(pathStr)-start > 1 {
			yield(pathStr[start:])
		}
	}
}

func (p *Parser) splitRoot(pathStr string) (root protoreflect.MessageDescriptor, remainder string, err error) {
	close := strings.IndexRune(pathStr[1:], ')')
	if close == -1 {
		return nil, "", errors.New("malformed path: missing ')'")
	}
	rootName := protoreflect.FullName(pathStr[1 : close+1]) // index is relative to pathStr[1:]
	if !rootName.IsValid() {
		return nil, "", fmt.Errorf("invalid message type '%s'", pathStr[1:close+1])
	}
	resolver := p.Resolver
	if resolver == nil {
		resolver = protoregistry.GlobalTypes
	}

	rootType, err := resolver.FindMessageByName(rootName)
	if err != nil {
		return nil, "", fmt.Errorf("error looking up message type %s: %w", rootName, err)
	}
	return rootType.Descriptor(), pathStr[close+2:], nil
}
