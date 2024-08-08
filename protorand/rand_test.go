package protorand_test

import (
	"fmt"
	"math"

	"github.com/kralicky/protoutil/protorand"
	"github.com/kralicky/protoutil/test/testdata"
	. "github.com/kralicky/protoutil/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var _ = Describe("Protorand", Label("unit"), func() {
	seed0 := struct {
		inputs  []float64
		outputs []*testdata.SampleMessage
	}{
		inputs: []float64{
			1.0,
			0.5,
			0.25,
			0.25,
			0.1,
			math.SmallestNonzeroFloat64,
			0.0,
		},
		outputs: []*testdata.SampleMessage{
			{
				Field1: &testdata.Sample1FieldMsg{
					Field1: 2029793274,
				},
				Field2: &testdata.Sample2FieldMsg{
					Field1: 526058514,
					Field2: 1408655353,
				},
				Field3: &testdata.Sample3FieldMsg{
					Field1: 116702506,
					Field2: 789387515,
					Field3: 621654496,
				},
				Field4: &testdata.Sample4FieldMsg{
					Field1: 413258767,
					Field2: 1407315077,
					Field3: 1926657288,
					Field4: 359390928,
				},
				Field5: &testdata.Sample5FieldMsg{
					Field1: 619732968,
					Field2: 1938329147,
					Field3: 1824889259,
					Field4: 586363548,
					Field5: 1307989752,
				},
				Field6: &testdata.Sample6FieldMsg{
					Field1: 544722126,
					Field2: 1663557311,
					Field3: 37539650,
					Field4: 1690228450,
					Field5: 1716684894,
					Field6: 765381515,
				},
				Msg: &testdata.SampleMessage2{
					Field1: &testdata.Sample1FieldMsg{
						Field1: 915240468,
					},
					Field2: &testdata.Sample2FieldMsg{
						Field1: 1095737066,
						Field2: 516323580,
					},
					Field3: &testdata.Sample3FieldMsg{
						Field1: 449257601,
						Field2: 1488356586,
						Field3: 863069152,
					},
					Field4: &testdata.Sample4FieldMsg{
						Field1: 611655120,
						Field2: 1467582991,
						Field3: 939604012,
						Field4: 223370157,
					},
					Field5: &testdata.Sample5FieldMsg{
						Field1: 678537188,
						Field2: 324900545,
						Field3: 1570544764,
						Field4: 674669078,
						Field5: 791293393,
					},
					Field6: &testdata.Sample6FieldMsg{
						Field1: 1391004200,
						Field2: 772328110,
						Field3: 1164609149,
						Field4: 39014514,
						Field5: 1112625487,
						Field6: 246694044,
					},
				},
			},
			{
				Field2: &testdata.Sample2FieldMsg{
					Field2: 1826733919,
				},
				Field3: &testdata.Sample3FieldMsg{
					Field1: 179662087,
					Field2: 1982518068,
				},
				Field4: &testdata.Sample4FieldMsg{
					Field1: 1011847422,
					Field3: 90440750,
				},
				Field6: &testdata.Sample6FieldMsg{
					Field1: 1827244694,
					Field3: 1107367304,
					Field6: 527198397,
				},
			},
			{
				Field1: &testdata.Sample1FieldMsg{
					Field1: 1660555972,
				},
				Field6: &testdata.Sample6FieldMsg{
					Field1: 176859541,
					Field4: 1303020509,
				},
			},
			{
				Field5: &testdata.Sample5FieldMsg{
					Field4: 474413850,
				},
				Msg: &testdata.SampleMessage2{
					Field3: &testdata.Sample3FieldMsg{
						Field2: 1158390584,
					},
					Field5: &testdata.Sample5FieldMsg{
						Field4: 1849101460,
					},
				},
			},
			{
				Field4: &testdata.Sample4FieldMsg{
					Field1: 1400974796,
				},
			},
			{
				Msg: &testdata.SampleMessage2{
					Field3: &testdata.Sample3FieldMsg{
						Field2: 1765974250,
					},
				},
			},
			{
				// empty message
			},
		},
	}
	Context("GenPartial", func() {
		It("should generate a partially-filled protobuf message with random values", MustPassRepeatedly(2), func() {
			rand := protorand.New[*testdata.SampleMessage]()
			rand.Seed(0)
			for i, input := range seed0.inputs {
				out, err := rand.GenPartial(input)
				Expect(err).NotTo(HaveOccurred())
				Expect(out).To(ProtoEqual(seed0.outputs[i]), fmt.Sprintf("index %d (input %f)", i, input))
			}
		})
		It("should produce different results with a different seed", MustPassRepeatedly(2), func() {
			rand := protorand.New[*testdata.SampleMessage]()
			rand.Seed(1)
			for i, input := range seed0.inputs {
				out, err := rand.GenPartial(input)
				Expect(err).NotTo(HaveOccurred())
				if input == 0 {
					// ratio of 0 should always produce an empty message
					Expect(out).To(ProtoEqual(&testdata.SampleMessage{}), fmt.Sprintf("index %d (input %f)", i, input))
				} else {
					Expect(out).NotTo(ProtoEqual(seed0.outputs[i]), fmt.Sprintf("index %d (input %f)", i, input))
				}
			}
		})
		It("should produce different results based on the state of the underlying random source", MustPassRepeatedly(2), func() {
			for i := 0; i < len(seed0.inputs); i++ {
				// generate a message to between index i and i+1 to disrupt the state
				rand := protorand.New[*testdata.SampleMessage]()
				rand.Seed(0)
				for j := 0; j < len(seed0.inputs); j++ {
					if j == i {
						rand.MustGen()
					}
					out := rand.MustGenPartial(seed0.inputs[j])

					if j < i {
						// should match
						Expect(out).To(ProtoEqual(seed0.outputs[j]), fmt.Sprintf("index %d (input %f)", j, seed0.inputs[j]))
					} else {
						if seed0.inputs[j] == 0 {
							Expect(out).To(ProtoEqual(&testdata.SampleMessage{}), fmt.Sprintf("index %d (input %f)", i, seed0.inputs[j]))
						} else {
							Expect(out).NotTo(ProtoEqual(seed0.outputs[i]), fmt.Sprintf("index %d (input %f)", i, seed0.inputs[j]))
						}
					}
				}
			}
		})
		It("should allow excluding fields from the random selection using a mask", func() {
			exclude := &fieldmaskpb.FieldMask{
				Paths: []string{
					"field1",
					"field2",
					"field3",
					"field4",
					"field5",
					"field6",
				},
			}
			rand := protorand.New[*testdata.SampleMessage]()
			rand.Seed(0)
			rand.ExcludeMask(exclude)
			out := rand.MustGenPartial(0.999) // this will round up to 1, but not trigger the ratio==1 fast path
			Expect(out).To(ProtoEqual(&testdata.SampleMessage{
				Msg: seed0.outputs[0].Msg,
			}))
			rand.ExcludeMask(nil)

			By("ensuring subsequent outputs are not affected")
			for i, input := range seed0.inputs[1:] {
				out := rand.MustGenPartial(input)
				Expect(out).To(ProtoEqual(seed0.outputs[i+1]), fmt.Sprintf("index %d (input %f)", i, input))
			}
		})
	})

	It("should not generate numbers larger than 2^53", func() {
		By("checking uint64", func() {
			rand := protorand.New[*wrapperspb.UInt64Value]()
			rand.UseJsonCompatibleIntegers = true
			rand.Seed(0) // seed 0 happens to generate the first few numbers > 2^53
			out := rand.MustGen()
			Expect(out.Value).To(Equal(uint64(0x78FC2FFAC2FD9401 & ((1 << 53) - 1))))
		})

		By("checking int64", func() {
			rand := protorand.New[*wrapperspb.Int64Value]()
			rand.UseJsonCompatibleIntegers = true
			rand.Seed(0)
			out := rand.MustGen()
			Expect(out.Value).To(Equal(int64(0x78FC2FFAC2FD9401 & ((1 << 53) - 1))))
		})

		By("checking maps with uint64 keys", func() {
			rand := protorand.New[*testdata.Uint64Map]()
			rand.UseJsonCompatibleIntegers = true
			rand.MaxCollectionElements = 1
			rand.Seed(0)
			out := rand.MustGen()
			Expect(out.Value).To(HaveKey(uint64(0x1F5B0412FFD341C0 & ((1 << 53) - 1))))
			Expect(out.Value[0x1F5B0412FFD341C0&((1<<53)-1)]).To(BeNumerically("<=", uint64(1<<53-1)))
			Expect(out.Value).To(HaveLen(1))
		})

		By("checking maps with int64 keys", func() {
			rand := protorand.New[*testdata.Int64Map]()
			rand.UseJsonCompatibleIntegers = true
			rand.MaxCollectionElements = 1
			rand.Seed(0)
			out := rand.MustGen()
			Expect(out.Value).To(HaveKey(int64(0x1F5B0412FFD341C0 & ((1 << 53) - 1))))
			Expect(out.Value[0x1F5B0412FFD341C0&((1<<53)-1)]).To(BeNumerically("<=", int64(1<<53-1)))
			Expect(out.Value).To(HaveLen(1))
		})

		By("checking lists with uint64 keys", func() {
			rand := protorand.New[*testdata.Uint64List]()
			rand.UseJsonCompatibleIntegers = true
			rand.MaxCollectionElements = 1
			rand.Seed(0)
			out := rand.MustGen()
			Expect(out.Value).To(ConsistOf(uint64(0x1F5B0412FFD341C0 & ((1 << 53) - 1))))
		})

		By("checking lists with int64 keys", func() {
			rand := protorand.New[*testdata.Int64List]()
			rand.UseJsonCompatibleIntegers = true
			rand.MaxCollectionElements = 1
			rand.Seed(0)
			out := rand.MustGen()
			Expect(out.Value).To(ConsistOf(int64(0x1F5B0412FFD341C0 & ((1 << 53) - 1))))
		})
	})
})
