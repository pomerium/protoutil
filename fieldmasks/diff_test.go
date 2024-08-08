package fieldmasks_test

import (
	"github.com/kralicky/protoutil/fieldmasks"
	"github.com/kralicky/protoutil/test/testdata"
	"github.com/kralicky/protoutil/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var _ = Describe("Diff", Label("unit"), func() {
	DescribeTable("identifying changes between protobuf messages",
		func(oldMsg, newMsg proto.Message, expectedPaths []string) {
			mask := fieldmasks.Diff(oldMsg.ProtoReflect(), newMsg.ProtoReflect())
			Expect(mask.Paths).To(ConsistOf(expectedPaths))
		},
		Entry("no changes",
			&testdata.SampleMessage{Field1: &testdata.Sample1FieldMsg{Field1: 1}},
			&testdata.SampleMessage{Field1: &testdata.Sample1FieldMsg{Field1: 1}},
			[]string{},
		),
		Entry("single field change",
			&testdata.SampleMessage{Field1: &testdata.Sample1FieldMsg{Field1: 1}},
			&testdata.SampleMessage{Field1: &testdata.Sample1FieldMsg{Field1: 2}},
			[]string{
				"field1",
				"field1.field1",
			},
		),
		Entry("multiple fields change",
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{Field1: 2, Field2: 3},
			},
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 10},
				Field2: &testdata.Sample2FieldMsg{Field1: 2, Field2: 30},
			},
			[]string{
				"field1",
				"field1.field1",
				"field2",
				"field2.field2",
			},
		),
		Entry("multiple fields change",
			&testdata.Sample6FieldMsg{
				Field1: 1,
				Field2: 2,
				Field3: 3,
				Field4: 4,
				Field5: 5,
				Field6: 6,
			},
			&testdata.Sample6FieldMsg{
				Field1: 10,
				Field2: 20,
				Field3: 30,
				Field4: 40,
				Field5: 5,
				Field6: 6,
			},
			[]string{"field1", "field2", "field3", "field4"},
		),
		Entry("nested message change",
			&testdata.SampleMessage{Msg: &testdata.SampleMessage2{Field1: &testdata.Sample1FieldMsg{Field1: 1}}},
			&testdata.SampleMessage{Msg: &testdata.SampleMessage2{Field1: &testdata.Sample1FieldMsg{Field1: 2}}},
			[]string{
				"msg",
				"msg.field1",
				"msg.field1.field1",
			},
		),
		Entry("nested message add",
			&testdata.SampleMessage{Msg: &testdata.SampleMessage2{Field2: &testdata.Sample2FieldMsg{Field1: 1}}},
			&testdata.SampleMessage{Msg: &testdata.SampleMessage2{Field2: &testdata.Sample2FieldMsg{Field1: 2, Field2: 3}}},
			[]string{
				"msg",
				"msg.field2",
				"msg.field2.field1",
				"msg.field2.field2",
			},
		),
		Entry("field added",
			&testdata.SampleMessage{},
			&testdata.SampleMessage{Field1: &testdata.Sample1FieldMsg{Field1: 1}},
			[]string{
				"field1",
				"field1.field1",
			},
		),
		Entry("field removed",
			&testdata.SampleMessage{Field1: &testdata.Sample1FieldMsg{Field1: 1}},
			&testdata.SampleMessage{},
			[]string{
				"field1",
				"field1.field1",
			},
		),
		Entry("field zeroed",
			&testdata.SampleMessage{Field1: &testdata.Sample1FieldMsg{Field1: 0}},
			&testdata.SampleMessage{},
			[]string{
				"field1",
			},
		),
		Entry("repeated field change",
			&testdata.SampleConfiguration{RepeatedField: []string{"a", "b", "c"}},
			&testdata.SampleConfiguration{RepeatedField: []string{"a", "b", "d"}},
			[]string{"repeatedField"},
		),
		Entry("repeated field change",
			&testdata.SampleConfiguration{RepeatedField: []string{"a", "b", "c"}},
			&testdata.SampleConfiguration{RepeatedField: []string{"a", "b"}},
			[]string{"repeatedField"},
		),
		Entry("repeated field change",
			&testdata.SampleConfiguration{RepeatedField: []string{"a", "b", "c"}},
			&testdata.SampleConfiguration{RepeatedField: []string{"a", "b", "c", "d"}},
			[]string{"repeatedField"},
		),
		Entry("repeated field change",
			&testdata.SampleConfiguration{RepeatedField: []string{"a", "b", "c"}},
			&testdata.SampleConfiguration{RepeatedField: nil},
			[]string{"repeatedField"},
		),
		Entry("the old message is nil",
			(*testdata.SampleMessage)(nil),
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{Field1: 2, Field2: 3},
				Field3: &testdata.Sample3FieldMsg{Field1: 4, Field2: 5, Field3: 6},
			},
			[]string{
				"field1",
				"field1.field1",
				"field2",
				"field2.field1",
				"field2.field2",
				"field3",
				"field3.field1",
				"field3.field2",
				"field3.field3",
			},
		),
		Entry("the new message is nil",
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{Field1: 2, Field2: 3},
				Field3: &testdata.Sample3FieldMsg{Field1: 4, Field2: 5, Field3: 6},
			},
			(*testdata.SampleMessage)(nil),
			[]string{
				"field1",
				"field1.field1",
				"field2",
				"field2.field1",
				"field2.field2",
				"field3",
				"field3.field1",
				"field3.field2",
				"field3.field3",
			},
		),
	)

	It("should handle nil messages", func() {
		Expect(fieldmasks.Diff((*testdata.SampleMessage)(nil).ProtoReflect(), (*testdata.SampleMessage)(nil).ProtoReflect())).To(testutil.ProtoEqual(&fieldmaskpb.FieldMask{}))
	})

	It("should panic on different message types", func() {
		Expect(func() {
			fieldmasks.Diff((&testdata.SampleMessage{}).ProtoReflect(), (&testdata.SampleMessage2{}).ProtoReflect())
		}).To(Panic())
	})
})
