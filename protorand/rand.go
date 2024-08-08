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

package protorand

// Contains a generic wrapper around protorand.

import (
	"math"
	"math/rand"
	"unsafe"

	art "github.com/kralicky/go-adaptive-radix-tree"
	"github.com/kralicky/protoutil/fieldmasks"
	"github.com/kralicky/protoutil/messages"
	"github.com/sryoya/protorand"
	"github.com/zeebo/xxh3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protorange"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
)

type ProtoRand[T proto.Message] struct {
	*protorand.ProtoRand
	// If true, durations will be limited to the range of values representable
	// by time.Duration (+/- ~292y) instead of the much larger range supported by
	// durationpb.Duration (+/- 10000y). Defaults to false.
	UseGoDurationLimits bool

	// If true, clamps the values of (u)int64 fields to 53 bits to ensure they are
	// representable in JSON. Defaults to false.
	UseJsonCompatibleIntegers bool

	mask *fieldmaskpb.FieldMask
	seed int64
}

func New[T proto.Message]() *ProtoRand[T] {
	return &ProtoRand[T]{
		ProtoRand: protorand.New(),
	}
}

func (p *ProtoRand[T]) Seed(seed int64) {
	p.seed = seed
	p.ProtoRand.Seed(seed)
}

// If non-nil, fields listed in the mask will be unset in generated messages.
// When generating a partial message, listed fields will not be included
// in the list of possible fields to select from, as if the fields did not
// exist in the message at all. Paths in the mask are relative to the root
// of the message and can include nested fields. If nested fields are listed,
// the mask only applies to "leaf" fields, and not to containing messages.
func (p *ProtoRand[T]) ExcludeMask(mask *fieldmaskpb.FieldMask) {
	p.mask = mask
}

func (p *ProtoRand[T]) Gen() (T, error) {
	out, err := p.ProtoRand.Gen(messages.New[T]())
	if err != nil {
		var zero T
		return zero, err
	}
	if p.mask != nil {
		fieldmasks.ExclusiveDiscard(out, p.mask)
	}
	p.sanitize(out)
	return out.(T), nil
}

func (p *ProtoRand[T]) MustGen() T {
	out, err := p.Gen()
	if err != nil {
		panic(err)
	}
	return out
}

// Generate a message with a specific ratio of set/unset fields.
//
// If ratio is 0, the message will be empty. If it is 1, all fields will be set.
// Otherwise, a randomly chosen subset of fields will be set. The ratio of set
// fields to unset fields is given by min(1, round(ratio * size)) where size
// is the number of fields in the current message. This function applies
// recursively to all fields, using the original ratio.
//
// This function reads the same amount of randomness from the underlying
// random number generator as a single call to Gen().
func (p *ProtoRand[T]) GenPartial(ratio float64) (T, error) {
	if ratio <= 0 {
		return messages.New[T](), nil
	} else if ratio >= 1 {
		return p.Gen()
	}

	newGeneratedMsg := p.MustGen()

	var nestedMask fieldmasks.Tree
	if p.mask != nil {
		nestedMask = fieldmasks.AsTree(p.mask)
	}

	var walk func(msg protoreflect.Message, prefix string, mask fieldmasks.Tree)
	walk = func(msg protoreflect.Message, prefix string, mask fieldmasks.Tree) {
		md := msg.Descriptor()
		wire, _ := proto.MarshalOptions{Deterministic: true}.Marshal(msg.Interface())
		msgFields := md.Fields()
		selectedFields := make([]protoreflect.FieldDescriptor, 0, msgFields.Len())
		for i := 0; i < msgFields.Len(); i++ {
			msgField := msgFields.Get(i)
			included := true
			if mask != nil {
				if _, masked := mask.Search(art.Key(prefix + string(msgField.Name()))); masked {
					// only exclude leaf fields from the mask
					included = false
				}
			}
			if included {
				selectedFields = append(selectedFields, msgField)
			} else {
				msg.Clear(msgField)
			}
		}

		partition := newPartition(len(selectedFields), ratio)
		rand.New(rand.NewSource(int64(xxh3.Hash(wire)))).
			Shuffle(len(partition), func(i, j int) {
				partition[i], partition[j] = partition[j], partition[i]
			})
		for i, msgField := range selectedFields {
			if partition[i] == 0 {
				msg.Clear(msgField)
			} else if !msgField.IsList() && !msgField.IsMap() && msgField.Kind() == protoreflect.MessageKind {
				walk(msg.Mutable(msgField).Message(), prefix+string(msgField.Name())+".", mask)
			}
		}
	}
	walk(newGeneratedMsg.ProtoReflect(), "", nestedMask)
	return newGeneratedMsg, nil
}

func (p *ProtoRand[T]) MustGenPartial(ratio float64) T {
	out, err := p.GenPartial(ratio)
	if err != nil {
		panic(err)
	}
	return out
}

// Returns a slice of a given size containing min(1, round(ratio * size)) 1s
// followed by 0s for the remaining elements.
func newPartition(size int, ratio float64) []int {
	if size == 0 {
		return nil
	}
	s := make([]int, size)
	numOnes := max(1, int(math.Round(ratio*float64(size))))
	for i := 0; i < numOnes; i++ {
		s[i] = 1
	}
	return s
}

var (
	durationDesc = (*durationpb.Duration)(nil).ProtoReflect().Descriptor()
	structDesc   = (*structpb.Struct)(nil).ProtoReflect().Descriptor()
)

func (p *ProtoRand[T]) sanitize(msg proto.Message) {
	protorange.Options{Stable: true}.Range(msg.ProtoReflect(), func(vs protopath.Values) error {
		// mask randomly generated (u)int64s to 53 bits, as larger values are not
		// representable in json.
		const mask = 1<<53 - 1
		v := vs.Index(-1)
		switch v.Step.Kind() {
		case protopath.ListIndexStep:
			if p.UseJsonCompatibleIntegers {
				// handle list values
				prevList := vs.Index(-2).Value.List()
				switch lv := v.Value.Interface().(type) {
				case uint64:
					if masked := lv & mask; masked != lv {
						prevList.Set(v.Step.ListIndex(), protoreflect.ValueOfUint64(masked))
					}
				case int64:
					if masked := lv & mask; masked != lv {
						prevList.Set(v.Step.ListIndex(), protoreflect.ValueOfInt64(masked))
					}
				}
			}
		case protopath.MapIndexStep:
			if p.UseJsonCompatibleIntegers {
				// handle map keys/values
				prevMap := vs.Index(-2).Value.Map()
				mapValue := prevMap.Get(v.Step.MapIndex())
				switch mv := mapValue.Interface().(type) {
				case uint64:
					if masked := mv & mask; masked != mv {
						mapValue = protoreflect.ValueOfUint64(masked)
						prevMap.Set(v.Step.MapIndex(), mapValue)
					}
				case int64:
					if masked := mv & mask; masked != mv {
						mapValue = protoreflect.ValueOfInt64(masked)
						prevMap.Set(v.Step.MapIndex(), mapValue)
					}
				}
				rng := (*rand.Rand)(unsafe.Pointer(p.ProtoRand))

				switch idx := v.Step.MapIndex().Interface().(type) {
				case uint64:
					if masked := idx & mask; masked != idx {
						newKey := protoreflect.MapKey(protoreflect.ValueOfUint64(masked))
						for prevMap.Has(newKey) {
							// this is an unfortunate and probably unlikely situation, but we
							// can generate new values without breaking possibly seeded rng
							// because we enabled stable range order.
							newKey = protoreflect.MapKey(protoreflect.ValueOfUint64(rng.Uint64() & mask))
						}
						prevMap.Clear(v.Step.MapIndex())
						prevMap.Set(newKey, mapValue)
					}
				case int64:
					if masked := idx & mask; masked != idx {
						newKey := protoreflect.MapKey(protoreflect.ValueOfInt64(masked))
						for prevMap.Has(newKey) {
							newKey = protoreflect.MapKey(protoreflect.ValueOfInt64(rng.Int63n(mask + 1)))
						}
						prevMap.Clear(v.Step.MapIndex())
						prevMap.Set(newKey, mapValue)
					}
				}
			}
		case protopath.FieldAccessStep:
			fd := v.Step.FieldDescriptor()
			if fd.IsList() || fd.IsMap() {
				return nil
			}
			switch fd.Kind() {
			case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
				if p.UseJsonCompatibleIntegers {
					u := v.Value.Uint()
					if masked := u & mask; masked != u {
						newValue := protoreflect.ValueOfUint64(masked)
						prev := vs.Index(-2)
						switch prev.Step.Kind() {
						case protopath.FieldAccessStep, protopath.RootStep:
							prev.Value.Message().Set(fd, newValue)
						}
					}
				}
			case protoreflect.Int64Kind, protoreflect.Sfixed64Kind, protoreflect.Sint64Kind:
				if p.UseJsonCompatibleIntegers {
					u := v.Value.Int()
					if masked := u & mask; masked != u {
						newValue := protoreflect.ValueOfInt64(masked)
						prev := vs.Index(-2)
						switch prev.Step.Kind() {
						case protopath.FieldAccessStep, protopath.RootStep:
							prev.Value.Message().Set(fd, newValue)
						}
					}
				}
			case protoreflect.MessageKind:
				switch fd.Message() {
				case durationDesc:
					if p.UseGoDurationLimits {
						// restrict durations to the range allowed by time.Duration.
						dpb := v.Value.Message().Interface().(*durationpb.Duration)
						duration := dpb.AsDuration()
						if duration == math.MinInt64 || duration == math.MaxInt64 {
							nanos := duration.Nanoseconds()
							dpb.Seconds = nanos / 1e9
							dpb.Nanos = int32(nanos - dpb.Seconds*1e9)
						}
					}
				case structDesc:
					// ensure we don't end up with invalid structs, which can happen if
					// the recursion limit is hit before the field oneof can be set
					spb := v.Value.Message().Interface().(*structpb.Struct)
					for field, value := range spb.Fields {
						if value.Kind == nil {
							spb.Fields[field] = structpb.NewNullValue()
						}
					}
				}
			}
		}
		return nil
	}, nil)
}
