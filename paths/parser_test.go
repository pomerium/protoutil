package paths_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/kralicky/protoutil/paths"
	"github.com/kralicky/protoutil/protorand"
	"github.com/kralicky/protoutil/test/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protorange"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestParsePath(t *testing.T) {
	rng := protorand.New[*testdata.Message]()
	rng.MaxCollectionElements = 2
	rng.MaxDepth = 1
	rng.ExcludeMask(&fieldmaskpb.FieldMask{
		Paths: []string{
			"any_field",
			"any_list",
			"uint64_to_any",
			"string_to_any",
		},
	})

	newAny := func(msg proto.Message) *anypb.Any {
		any, err := anypb.New(msg)
		require.NoError(t, err)
		return any
	}

	var gen func(depth int, partial float64) *testdata.Message

	gen = func(depth int, partial float64) *testdata.Message {
		if depth == 0 {
			return nil
		}
		entry, err := rng.GenPartial(partial)
		require.NoError(t, err)
		entry.AnyField = newAny(gen(depth-1, 0.25))
		entry.AnyList = []*anypb.Any{
			newAny(gen(depth-1, 0.25)),
		}
		entry.Uint64ToAny = map[uint64]*anypb.Any{
			1: newAny(gen(depth-1, 0.25)),
		}
		entry.StringToAny = map[string]*anypb.Any{
			"foo": newAny(gen(depth-1, 0.25)),
		}
		return entry
	}
	t.Run("Round Trip", func(t *testing.T) {
		entry := gen(4, 1)
		protorange.Range(entry.ProtoReflect(), func(v protopath.Values) error {
			if len(v.Path) == 1 {
				return nil
			}
			expectedValue := v.Index(-1).Value

			{
				pathStr := v.Path.String()
				parsedPath, err := paths.Parse(pathStr)
				// parsedPath, err := paths.ParseFrom(entry.ProtoReflect().Descriptor(), pathStr)
				require.NoError(t, err, "path: %s", pathStr)
				assert.Equal(t, v.Path.String(), parsedPath.String())

				actualValue, err := paths.Evaluate(entry, parsedPath)
				require.NoError(t, err)
				assert.True(t, actualValue.Equal(expectedValue),
					"expected %s to equal %s", actualValue, expectedValue)
			}

			{
				pathStr := v.Path[1:].String()

				parsedPath, err := paths.ParseFrom(entry.ProtoReflect().Descriptor(), pathStr)
				require.NoError(t, err, "path: %s", pathStr)
				assert.Equal(t, v.Path.String(), parsedPath.String())

				actualValue, err := paths.Evaluate(entry, parsedPath)
				require.NoError(t, err)
				assert.True(t, actualValue.Equal(expectedValue),
					"expected %s to equal %s", actualValue, expectedValue)
			}
			return nil
		})
	})

	t.Run("Errors", func(t *testing.T) {
		cases := []struct {
			path string
			err  string
		}{
			{
				path: "",
				err:  "empty path",
			},
			{
				path: "message_field.int32_field",
				err:  "path must start with '.'",
			},
			{
				path: ".message_field..int32_field",
				err:  "path contains empty step",
			},
			{
				path: ".message_field.string_field[1]",
				err:  "cannot index string field 'testdata.Message2.string_field'",
			},
			{
				path: ".message_field[1]",
				err:  "cannot index message field 'testdata.Message.message_field'",
			},
			{
				path: ".[1]",
				err:  "cannot index '(testdata.Message)'",
			},
			{
				path: `.message_field.string_to_any["key"].(testdata.Message2).string_list[0][0]`,
				err:  "cannot index '[0]'",
			},
			{
				path: `.message_field.string_to_any["key"].(testdata.Message2).oneof_nested_message_field.nested_bool_list[0].(testdata.Message2)`,
				err:  "can only expand fields of type google.protobuf.Any, not bool",
			},
			{
				path: `.message_field.string_to_any["key"].(testdata.Message2).any_list.(testdata.Message2)`,
				err:  "can only expand fields of type google.protobuf.Any, not repeated google.protobuf.Any",
			},
			{
				path: ".message_field.string_to_any.(testdata.Message2)",
				err:  "can only expand fields of type google.protobuf.Any, not map<string, google.protobuf.Any>",
			},
			{
				path: ".message_field.string_field.(google.protobuf.StringValue)",
				err:  "can only expand fields of type google.protobuf.Any, not string",
			},
			{
				path: `.message_field.string_list["not-an-integer"]`,
				err:  `invalid list index: strconv.ParseInt: parsing "\"not-an-integer\"": invalid syntax`,
			},
			{
				path: ".message_field.string_to_string[]",
				err:  "empty map key",
			},
			{
				path: ".message_field.string_to_string[1234]",
				err:  "invalid string map key '1234': invalid syntax",
			},
			{
				path: `.message_field.string_to_any["key"].bool_to_any[notABool]`,
				err:  "no such field 'bool_to_any' in message google.protobuf.Any (missing type expansion?)",
			},
			{
				path: `.message_field.string_to_any["[key]"].(testdata.Message2).nonexistent`,
				err:  "no such field 'nonexistent' in message testdata.Message2",
			},
			{
				path: `.message_field.string_to_any["]key["].(testdata.Message2).bool_to_any[notABool]`,
				err:  "invalid map key 'notABool' for bool field 'testdata.Message2.bool_to_any'",
			},
			{
				path: `.message_field.string_to_any["k e y"].(testdata.Message2).int32_to_bool[true]`,
				err:  "invalid map key 'true': strconv.ParseInt: parsing \"true\": invalid syntax",
			},
			{
				path: `.message_field.string_to_any["(key)"].(testdata.Message2).int64_to_string[false]`,
				err:  "invalid map key 'false': strconv.ParseInt: parsing \"false\": invalid syntax",
			},
			{
				path: `.message_field.string_to_any["\"key\""].(testdata.Message2).uint32_to_bytes[-1]`,
				err:  "invalid map key '-1': strconv.ParseUint: parsing \"-1\": invalid syntax",
			},
			{
				path: `.message_field.string_to_any["'key'"].(testdata.Message2).uint64_to_any["foo"]`,
				err:  `invalid map key '"foo"': strconv.ParseUint: parsing "\"foo\"": invalid syntax`,
			},
			{
				path: `.message_field.string_to_any["*"].(testdata.Message2).int64_to_string[-1].field`,
				err:  "cannot access field 'field' of non-message type",
			},
			{
				path: ".message_field.?.string_to_any",
				err:  "unknown field access not supported",
			},
			{
				path: ".(testdata.Message)",
				err:  "unexpected type expansion after Root step",
			},
			{
				path: `.message_field.string_to_any["^$%"].(.testdata.Message2)`,
				err:  "invalid message type '.testdata.Message2' (try removing the leading dot)",
			},
			{
				path: `.message_field.string_to_any[""].(testdata.Message2.)`,
				err:  "invalid message type 'testdata.Message2.'",
			},
			{
				path: `.message_field.string_to_any["key"].(testdata.Message.NestedMessage.NestedEnum)`,
				err:  "error looking up message type testdata.Message.NestedMessage.NestedEnum: proto: found wrong type: got enum, want message",
			},
			{
				path: `.message_field.string_to_any["~"].(foo.bar.Nonexistent)`,
				err:  "message type foo.bar.Nonexistent not found",
			},
		}

		for _, c := range cases {
			t.Run(c.path, func(t *testing.T) {
				_, err := paths.ParseFrom((*testdata.Message)(nil).ProtoReflect().Descriptor(), c.path)
				// note: protobuf randomly swaps out spaces in error messages with
				// non-breaking spaces (U+00A0)
				assert.Contains(t, strings.ReplaceAll(err.Error(), "\u00a0", " "), c.err)
			})
		}
	})

	t.Run("Evaluate Errors", func(t *testing.T) {
		cases := []struct {
			path string
			err  string
		}{
			{
				path: `.message_field.string_to_any["key2"].(testdata.Message).int32_field`,
				err:  "wrong message type: expecting testdata.Message2, have testdata.Message",
			},
			{
				path: `.string_to_any["key2"].(testdata.Message).int32_field`,
			},
		}
		msg := &testdata.Message{
			MessageField: &testdata.Message2{
				StringToAny: map[string]*anypb.Any{
					"key2": newAny(&testdata.Message2{
						BoolToAny: map[bool]*anypb.Any{
							true: {},
						},
					}),
					"key": newAny(&testdata.Message2{}),
				},
			},
		}
		for _, c := range cases {
			t.Run(c.path, func(t *testing.T) {
				path, err := paths.ParseFrom(msg.ProtoReflect().Descriptor(), c.path)
				require.NoError(t, err)
				_, err = paths.Evaluate(msg, path)
				if c.err == "" {
					assert.NoError(t, err)
				} else {
					require.Error(t, err)
					assert.Contains(t, err.Error(), c.err)
				}
			})
		}
	})

	t.Run("Invalid input", func(t *testing.T) {
		longPath := `.message_field.string_to_any["key"].(testdata.Message2).oneof_nested_message_field.nested_bool_list[0]`
		steps := []proto.Message{
			(*testdata.Message)(nil),
			&testdata.Message{},
			&testdata.Message{
				MessageField: &testdata.Message2{},
			},
			&testdata.Message{
				MessageField: &testdata.Message2{
					StringToAny: map[string]*anypb.Any{},
				},
			},
			&testdata.Message{
				MessageField: &testdata.Message2{
					StringToAny: map[string]*anypb.Any{
						"key": nil,
					},
				},
			},
			&testdata.Message{
				MessageField: &testdata.Message2{
					StringToAny: map[string]*anypb.Any{
						"wrongKey": newAny(&testdata.Message2{}),
					},
				},
			},
			&testdata.Message{
				MessageField: &testdata.Message2{
					StringToAny: map[string]*anypb.Any{
						"key": newAny(&testdata.Message2{}),
					},
				},
			},
			&testdata.Message{
				MessageField: &testdata.Message2{
					StringToAny: map[string]*anypb.Any{
						"key": newAny(&testdata.Message2{
							OneofField: &testdata.Message2_OneofInt32Field{},
						}),
					},
				},
			},
			&testdata.Message{
				MessageField: &testdata.Message2{
					StringToAny: map[string]*anypb.Any{
						"key": newAny(&testdata.Message2{
							OneofField: &testdata.Message2_OneofNestedMessageField{},
						}),
					},
				},
			},
			&testdata.Message{
				MessageField: &testdata.Message2{
					StringToAny: map[string]*anypb.Any{
						"key": newAny(&testdata.Message2{
							OneofField: &testdata.Message2_OneofNestedMessageField{
								OneofNestedMessageField: &testdata.Message2_NestedMessage{},
							},
						}),
					},
				},
			},
			&testdata.Message{
				MessageField: &testdata.Message2{
					StringToAny: map[string]*anypb.Any{
						"key": newAny(&testdata.Message2{
							OneofField: &testdata.Message2_OneofNestedMessageField{
								OneofNestedMessageField: &testdata.Message2_NestedMessage{
									NestedBoolList: nil,
								},
							},
						}),
					},
				},
			},
			&testdata.Message{
				MessageField: &testdata.Message2{
					StringToAny: map[string]*anypb.Any{
						"key": newAny(&testdata.Message2{
							OneofField: &testdata.Message2_OneofNestedMessageField{
								OneofNestedMessageField: &testdata.Message2_NestedMessage{
									NestedBoolList: []bool{},
								},
							},
						}),
					},
				},
			},
			&testdata.Message{
				MessageField: &testdata.Message2{
					StringToAny: map[string]*anypb.Any{
						"key": newAny(&testdata.Message2{
							OneofField: &testdata.Message2_OneofNestedMessageField{
								OneofNestedMessageField: &testdata.Message2_NestedMessage{
									NestedBoolList: []bool{true},
								},
							},
						}),
					},
				},
			},
		}

		for i, c := range steps {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				path, err := paths.ParseFrom(c.ProtoReflect().Descriptor(), longPath)
				require.NoError(t, err)

				v, err := paths.Evaluate(c, path)
				require.NoError(t, err)
				if i == len(steps)-1 {
					require.True(t, v.IsValid(), "expected valid value")
					assert.Equal(t, true, v.Bool())
				} else {
					assert.False(t, v.IsValid(), "expected invalid value but got %q", v.String())
				}
			})
		}
	})
}

func TestParser(t *testing.T) {
	httpAccessLogEntryDesc := (*testdata.Message)(nil).ProtoReflect().Descriptor()
	tcpAccessLogEntryDesc := (*testdata.Message)(nil).ProtoReflect().Descriptor()

	parser := paths.Parser{}
	path1, err := parser.ParseFrom(httpAccessLogEntryDesc, ".message_field.string_field")
	require.NoError(t, err)

	path2, err := parser.ParseFrom(tcpAccessLogEntryDesc, ".message_field.string_field")
	require.NoError(t, err)

	require.Equal(t, "(testdata.Message).message_field.string_field", path1.String())
	require.Equal(t, "(testdata.Message).message_field.string_field", path2.String())
}

func BenchmarkParser(b *testing.B) {
	const path = `.message_field.string_to_any["key"].(testdata.Message2).oneof_nested_message_field.nested_bool_list[0].(testdata.Message2)`
	desc := (*testdata.Message)(nil).ProtoReflect().Descriptor()
	b.Run("Without cache", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var parser paths.Parser
			_, _ = parser.ParseFrom(desc, path)
		}
	})
	b.Run("With cache", func(b *testing.B) {
		var parser paths.Parser
		for i := 0; i < b.N; i++ {
			_, _ = parser.ParseFrom(desc, path)
		}
	})
}
