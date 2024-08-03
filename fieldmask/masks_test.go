package fieldmask_test

import (
	"fmt"

	"github.com/kralicky/protoutil/fieldmask"
	"github.com/kralicky/protoutil/protorand"
	"github.com/kralicky/protoutil/test/testdata"
	"github.com/kralicky/protoutil/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var _ = Describe("Masks", Label("unit"), func() {
	DescribeTable("using a field mask to keep only the specified fields",
		func(msg *testdata.SampleMessage, mask *fieldmaskpb.FieldMask, expected *testdata.SampleMessage) {
			fieldmask.ExclusiveKeep(msg, mask)
			Expect(msg).To(testutil.ProtoEqual(expected))
		},
		Entry("empty mask",
			&testdata.SampleMessage{Field1: &testdata.Sample1FieldMsg{Field1: 1}},
			&fieldmaskpb.FieldMask{},
			&testdata.SampleMessage{},
		),
		Entry("mask with partial matching fields (a)",
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{
					Field1: 2,
					Field2: 3,
				},
			},
			&fieldmaskpb.FieldMask{Paths: []string{"field2.field1"}},
			&testdata.SampleMessage{
				Field2: &testdata.Sample2FieldMsg{Field1: 2},
			},
		),
		Entry("mask with partial matching fields (b)",
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{
					Field1: 2,
					Field2: 3,
				},
			},
			&fieldmaskpb.FieldMask{Paths: []string{"field1.field1"}},
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
			},
		),
		Entry("mask with partial matching fields (c)",
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{
					Field1: 2,
					Field2: 3,
				},
			},
			&fieldmaskpb.FieldMask{Paths: []string{"field2.field1", "field2.field2"}},
			&testdata.SampleMessage{
				Field2: &testdata.Sample2FieldMsg{
					Field1: 2,
					Field2: 3,
				},
			},
		),
		Entry("mask with partial matching fields (d)",
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{
					Field1: 2,
					Field2: 3,
				},
			},
			&fieldmaskpb.FieldMask{Paths: []string{"field2"}},
			&testdata.SampleMessage{
				Field2: &testdata.Sample2FieldMsg{
					Field1: 2,
					Field2: 3,
				},
			},
		),
		Entry("mask with no matching fields",
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{
					Field1: 2,
					Field2: 3,
				},
				Msg: &testdata.SampleMessage2{
					Field1: &testdata.Sample1FieldMsg{Field1: 1},
				},
			},
			&fieldmaskpb.FieldMask{Paths: []string{"field3"}},
			&testdata.SampleMessage{},
		),
		Entry("mask with all fields",
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{Field1: 2},
			},
			&fieldmaskpb.FieldMask{Paths: []string{"field1.field1", "field2.field1"}},
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{Field1: 2},
			},
		),
	)
	DescribeTable("using a field mask to discard the specified fields",
		func(msg *testdata.SampleMessage, mask *fieldmaskpb.FieldMask, expected *testdata.SampleMessage) {
			fieldmask.ExclusiveDiscard(msg, mask)
			Expect(msg).To(testutil.ProtoEqual(expected))
		},
		Entry("empty mask",
			&testdata.SampleMessage{Field1: &testdata.Sample1FieldMsg{Field1: 1}},
			&fieldmaskpb.FieldMask{},
			&testdata.SampleMessage{Field1: &testdata.Sample1FieldMsg{Field1: 1}},
		),
		Entry("mask with partial matching fields (a)",
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{
					Field1: 2,
					Field2: 3,
				},
			},
			&fieldmaskpb.FieldMask{Paths: []string{"field2.field1"}},
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{Field2: 3},
			},
		),
		Entry("mask with partial matching fields (b)",
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{
					Field1: 2,
					Field2: 3,
				},
			},
			&fieldmaskpb.FieldMask{Paths: []string{"field1.field1"}},
			&testdata.SampleMessage{
				Field2: &testdata.Sample2FieldMsg{
					Field1: 2,
					Field2: 3,
				},
			},
		),
		Entry("mask with partial matching fields (c)",
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{
					Field1: 2,
					Field2: 3,
				},
			},
			&fieldmaskpb.FieldMask{Paths: []string{"field2.field1", "field2.field2"}},
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
			},
		),
		Entry("mask with partial matching fields (d)",
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{
					Field1: 2,
					Field2: 3,
				},
			},
			&fieldmaskpb.FieldMask{Paths: []string{"field2"}},
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
			},
		),
		Entry("mask with no matching fields",
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{
					Field1: 2,
					Field2: 3,
				},
				Msg: &testdata.SampleMessage2{
					Field1: &testdata.Sample1FieldMsg{Field1: 1},
				},
			},
			&fieldmaskpb.FieldMask{Paths: []string{"field3"}},
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{
					Field1: 2,
					Field2: 3,
				},
				Msg: &testdata.SampleMessage2{
					Field1: &testdata.Sample1FieldMsg{Field1: 1},
				},
			},
		),
		Entry("mask with all fields",
			&testdata.SampleMessage{
				Field1: &testdata.Sample1FieldMsg{Field1: 1},
				Field2: &testdata.Sample2FieldMsg{Field1: 2},
			},
			&fieldmaskpb.FieldMask{Paths: []string{"field1.field1", "field2.field1"}},
			&testdata.SampleMessage{},
		),
	)
	It("should treat a nil mask as a no-op", func() {
		msg := &testdata.SampleMessage{Field1: &testdata.Sample1FieldMsg{Field1: 1}}
		fieldmask.ExclusiveKeep(msg, nil)
		Expect(msg).To(testutil.ProtoEqual(&testdata.SampleMessage{Field1: &testdata.Sample1FieldMsg{Field1: 1}}))
		fieldmask.ExclusiveDiscard(msg, nil)
		Expect(msg).To(testutil.ProtoEqual(&testdata.SampleMessage{Field1: &testdata.Sample1FieldMsg{Field1: 1}}))
	})
	It("should create field masks by presence", func() {
		rand := protorand.New[*testdata.SampleMessage]()
		rand.Seed(0)
		obj, err := rand.GenPartial(0.5)
		Expect(err).NotTo(HaveOccurred())
		presence := fieldmask.ByPresence(obj.ProtoReflect())
		absence := fieldmask.ByAbsence(obj.ProtoReflect())
		expectedPresence := &fieldmaskpb.FieldMask{
			Paths: []string{
				"field3.field1",
				"field3.field3",
				"field4.field1",
				"field4.field2",
				"field5.field1",
				"field5.field3",
				"field5.field5",
				"msg.field3.field2",
				"msg.field3.field3",
				"msg.field5.field1",
				"msg.field5.field2",
				"msg.field5.field5",
				"msg.field6.field2",
				"msg.field6.field3",
				"msg.field6.field4",
			},
		}
		expectedAbsence := &fieldmaskpb.FieldMask{
			Paths: []string{
				"field1",
				"field2",
				"field3.field2",
				"field4.field3",
				"field4.field4",
				"field5.field2",
				"field5.field4",
				"field6",
				"msg.field1",
				"msg.field2",
				"msg.field3.field1",
				"msg.field4",
				"msg.field5.field3",
				"msg.field5.field4",
				"msg.field6.field1",
				"msg.field6.field5",
				"msg.field6.field6",
			},
		}
		expectedPresence.Normalize()
		expectedAbsence.Normalize()
		Expect(presence).To(testutil.ProtoEqual(expectedPresence))
		Expect(absence).To(testutil.ProtoEqual(expectedAbsence))
		By("checking that ExclusiveKeep(obj, absence) results in an empty object", func() {
			obj := proto.Clone(obj)
			fieldmask.ExclusiveKeep(obj, absence)
			Expect(obj).To(testutil.ProtoEqual(&testdata.SampleMessage{}))
		})
		By("checking that ExclusiveDiscard(obj, presence) results in an empty object", func() {
			obj := proto.Clone(obj)
			fieldmask.ExclusiveDiscard(obj, presence)
			Expect(obj).To(testutil.ProtoEqual(&testdata.SampleMessage{}))
		})

		rand2 := protorand.New[*testdata.SampleConfiguration]()
		rand2.Seed(0)
		obj2, err := rand2.GenPartial(0.5)
		Expect(err).NotTo(HaveOccurred())
		presence2 := fieldmask.ByPresence(obj2.ProtoReflect())
		absence2 := fieldmask.ByAbsence(obj2.ProtoReflect())
		expectedPresence2 := &fieldmaskpb.FieldMask{
			Paths: []string{
				"enabled",
				"enumField",
				"messageField.field1.field1",
				"messageField.field2.field2",
				"messageField.field3.field2",
				"messageField.field3.field3",
				"messageField.field6.field2",
				"messageField.field6.field5",
				"messageField.field6.field6",
				"revision.timestamp.seconds",
			},
		}
		expectedAbsence2 := &fieldmaskpb.FieldMask{
			Paths: []string{
				"mapField",
				"messageField.field2.field1",
				"messageField.field3.field1",
				"messageField.field4",
				"messageField.field5",
				"messageField.field6.field1",
				"messageField.field6.field3",
				"messageField.field6.field4",
				"messageField.msg",
				"repeatedField",
				"revision.revision",
				"revision.timestamp.nanos",
				"bytesField",
				"stringField",
			},
		}
		expectedPresence2.Normalize()
		expectedAbsence2.Normalize()
		Expect(presence2).To(testutil.ProtoEqual(expectedPresence2))
		Expect(absence2).To(testutil.ProtoEqual(expectedAbsence2))

		By("checking that ExclusiveKeep(obj2, absence2) results in an empty object", func() {
			obj2 := proto.Clone(obj2)
			fieldmask.ExclusiveKeep(obj2, absence2)
			Expect(obj2).To(testutil.ProtoEqual(&testdata.SampleConfiguration{}))
		})
		By("checking that ExclusiveDiscard(obj2, presence2) results in an empty object", func() {
			obj2 := proto.Clone(obj2)
			fieldmask.ExclusiveDiscard(obj2, presence2)
			Expect(obj2).To(testutil.ProtoEqual(&testdata.SampleConfiguration{}))
		})
	})
	It("should create complete field masks for a type", func() {
		mask := fieldmask.AllFields[*testdata.SampleMessage]()
		expected := &fieldmaskpb.FieldMask{
			Paths: []string{},
		}
		for i := 1; i <= 6; i++ {
			for j := 1; j <= i; j++ {
				expected.Paths = append(expected.Paths, fmt.Sprintf("field%d.field%d", i, j))
			}
		}
		for i, l := 0, len(expected.Paths); i < l; i++ {
			expected.Paths = append(expected.Paths, fmt.Sprintf("msg.%s", expected.Paths[i]))
		}
		expected.Normalize()
		Expect(mask).To(testutil.ProtoEqual(expected))

		mask2 := fieldmask.AllFields[*testdata.SampleConfiguration]()
		expected2 := &fieldmaskpb.FieldMask{
			Paths: []string{
				"enabled",
				"enumField",
				"revision.revision",
				"revision.timestamp.nanos",
				"revision.timestamp.seconds",
				"stringField",
				"bytesField",
				"mapField",
				"repeatedField",
			},
		}
		for _, p := range expected.Paths {
			expected2.Paths = append(expected2.Paths, fmt.Sprintf("messageField.%s", p))
		}
		expected2.Normalize()
		Expect(mask2).To(testutil.ProtoEqual(expected2))
	})
	It("should create masks containing only leaf fields from an existing mask", func() {
		leaves := fieldmask.Leaves(&fieldmaskpb.FieldMask{
			Paths: []string{
				"field1.field1",
				"field2.field1",
				"field3",
				"msg.field2.field1",
				"msg.field3.field1",
				"msg.field3.field2",
				"msg.field3.field3",
				"msg.field4",
				"msg.field5",
			},
		}, (&testdata.SampleMessage{}).ProtoReflect().Descriptor())
		expectedLeaves := &fieldmaskpb.FieldMask{
			Paths: []string{
				"field1.field1",
				"field2.field1",
				"msg.field2.field1",
				"msg.field3.field1",
				"msg.field3.field2",
				"msg.field3.field3",
			},
		}
		Expect(leaves).To(testutil.ProtoEqual(expectedLeaves))
	})
})
