package paths

import (
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// Evaluate returns the value within a message referenced by the given path,
// or an invalid value if the path could not be resolved for any reason.
//
// The path may, but is not required to, contain a root step. If the path does
// contain a root step, and the root step does not match the provided message,
// this function will panic.
//
// A non-nil error can be returned if the wrong message type is given in an
// any-expand expression. Otherwise, the message not containing a value at the
// path is not treated as an error.
func Evaluate(root proto.Message, path protopath.Path) (protoreflect.Value, error) {
	v := protoreflect.ValueOfMessage(root.ProtoReflect())
	for _, step := range path {
		if !v.IsValid() {
			return protoreflect.Value{}, nil
		}
		switch step.Kind() {
		case protopath.FieldAccessStep:
			m := v.Message()
			if !m.IsValid() {
				return protoreflect.Value{}, nil
			}
			// check that the field descriptors match, otherwise this will panic
			if m.Descriptor() != step.FieldDescriptor().ContainingMessage() {
				expecting := m.Descriptor().FullName()
				have := step.FieldDescriptor().ContainingMessage().FullName()
				return protoreflect.Value{}, fmt.Errorf("cannot access field '%s': wrong message type: expecting %v, have %v", step.FieldDescriptor().FullName(), expecting, have)
			}
			v = m.Get(step.FieldDescriptor())
		case protopath.ListIndexStep:
			list := v.List()
			if !list.IsValid() {
				return protoreflect.Value{}, nil
			}
			v = list.Get(step.ListIndex())
		case protopath.MapIndexStep:
			m := v.Map()
			if !m.IsValid() {
				return protoreflect.Value{}, nil
			}
			v = m.Get(step.MapIndex())
		case protopath.AnyExpandStep:
			m := v.Message()
			if !m.IsValid() {
				return protoreflect.Value{}, nil
			}
			msg, err := m.Interface().(*anypb.Any).UnmarshalNew()
			if err != nil {
				panic(fmt.Errorf("bug: %w", err))
			}
			v = protoreflect.ValueOfMessage(msg.ProtoReflect())
		case protopath.RootStep:
			if v.Message().Descriptor() != step.MessageDescriptor() {
				panic(fmt.Sprintf("bug: mismatched root descriptor (%s != %s)",
					v.Message().Descriptor().FullName(), step.MessageDescriptor().FullName()))
			}
		}
	}
	return v, nil
}

// Slice returns a new path with both its path and values resliced by the given
// range. It is assumed that both slices are of the same length.
func Slice(from protopath.Values, start, end int) protopath.Values {
	// if start is > 0, transform the first step into a Root step with the
	// message type of the first value
	if start > 0 {
		rootStep := protopath.Root(from.Values[start].Message().Descriptor())
		return protopath.Values{
			Path:   append(protopath.Path{rootStep}, from.Path[start+1:end]...),
			Values: from.Values[start:end],
		}
	}
	return protopath.Values{
		Path:   from.Path[start:end],
		Values: from.Values[start:end],
	}
}

// Join returns a new path constructed by appending the steps of 'b' to 'a'.
// The first step in 'b' is skipped; it is assumed (but not checked for)
// that 'b' starts with a Root step matching the message type of the last
// step in path 'a'.
func Join[T protopath.Step, S ~[]T](a protopath.Path, b S) protopath.Path {
	p := slices.Clone(a)
	for _, step := range b[1:] {
		p = append(p, protopath.Step(step))
	}
	return p
}
