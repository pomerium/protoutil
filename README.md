# protoutil

A set of supplementary utility packages for working with protocol buffers in Go.

- `fieldmasks` contains utilities for building and comparing field masks (`fieldmaskpb.FieldMask`).

- `merge` contains a replacement for the upstream merge function containing extra options.

- `messages` contains a few helpful generic functions for working with message types.

- `paths` contains utilities for parsing and evaluating `protopath.Path` objects.

- `protorand` is a wrapper around github.com/sryoya/protorand with some extra features.

- `streams` contains utilities for working with GRPC streams.

- `testutil` contains Gomega matchers for comparing `proto.Message` and `protoreflect.Value` objects.
