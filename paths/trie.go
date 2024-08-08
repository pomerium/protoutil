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

package paths

import (
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Trie[V any] struct {
	root  *TrieNode[V]
	index map[string]*TrieNode[V]
}

type TrieNode[V any] struct {
	protopath.Path
	parent *TrieNode[V]
	nodes  map[protoreflect.Name]*TrieNode[V]
	value  V
}

func NewTrie[V any](desc protoreflect.MessageDescriptor, newV func() V) *Trie[V] {
	t := &Trie[V]{
		root: &TrieNode[V]{
			Path: protopath.Path{protopath.Root(desc)},
		},
	}
	buildNode(t.root, desc, newV)
	t.index = make(map[string]*TrieNode[V])
	t.Walk(func(node *TrieNode[V]) {
		if len(node.Path) == 1 {
			return
		}
		t.index[node.Path[1:].String()[1:]] = node
	})
	return t
}

func buildNode[V any](node *TrieNode[V], desc protoreflect.MessageDescriptor, newV func() V) {
	node.nodes = make(map[protoreflect.Name]*TrieNode[V])
	node.value = newV()
	for i := 0; i < desc.Fields().Len(); i++ {
		field := desc.Fields().Get(i)
		newNode := &TrieNode[V]{
			parent: node,
			Path:   append(append(protopath.Path{}, node.Path...), protopath.FieldAccess(field)),
			value:  newV(),
		}
		if field.Kind() == protoreflect.MessageKind && !field.IsMap() && !field.IsList() {
			buildNode(newNode, field.Message(), newV)
		}
		node.nodes[field.Name()] = newNode
	}
}

func (t *Trie[V]) Find(path protopath.Path) *TrieNode[V] {
	return t.index[path[1:].String()[1:]]
}

func (t *Trie[V]) FindString(pathStr string) *TrieNode[V] {
	return t.index[pathStr]
}

// Walk performs a depth-first post-order traversal of the trie, calling fn
// for each node. The root node is visited last.
func (t *Trie[V]) Walk(fn func(*TrieNode[V])) {
	var walk func(*TrieNode[V])
	walk = func(node *TrieNode[V]) {
		for _, child := range node.nodes {
			walk(child)
		}
		fn(node)
	}
	walk(t.root)
}

func (t *Trie[V]) WalkValues(fn func(node *TrieNode[V], value protoreflect.Value), rootValue protoreflect.Value) {
	var walk func(*TrieNode[V], protoreflect.Value)
	walk = func(node *TrieNode[V], value protoreflect.Value) {
		for _, child := range node.nodes {
			if !value.IsValid() {
				walk(child, protoreflect.Value{})
			} else {
				walk(child, value.Message().Get(child.Path.Index(-1).FieldDescriptor()))
			}
		}
		fn(node, value)
	}
	walk(t.root, rootValue)
}
