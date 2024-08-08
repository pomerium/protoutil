// Copyright 2024 Joe Kralicky
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package paths

import (
	"slices"

	"google.golang.org/protobuf/reflect/protopath"
)

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
