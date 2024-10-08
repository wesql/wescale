/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

// GTIDSet represents the set of transactions received or applied by a server.
// In some flavors, a single GTID is enough to specify the set of all
// transactions that came before it, but in others a more complex structure is
// required.
//
// GTIDSet is wrapped by replication.Position, which is a concrete struct.
// When sending a GTIDSet over RPCs, encode/decode it as a string.
// Most code outside of this package should use replication.Position rather
// than GTIDSet.
type GTIDSet interface {
	// String returns the canonical printed form of the set as expected by a
	// particular flavor of MySQL.
	String() string

	// Flavor returns the key under which the corresponding parser function is
	// registered in the transactionSetParsers map.
	Flavor() string

	// ContainsGTID returns true if the set contains the specified transaction.
	ContainsGTID(GTID) bool

	// Contains returns true if the set is a superset of another set. All implementations should return false if
	// other GTIDSet is not the right concrete type for that flavor.
	Contains(GTIDSet) bool

	// Equal returns true if the set is equal to another set.
	Equal(GTIDSet) bool

	// AddGTID returns a new GTIDSet that is expanded to contain the given GTID.
	AddGTID(GTID) GTIDSet

	// Union returns a union of the receiver GTIDSet and the supplied GTIDSet.
	Union(GTIDSet) GTIDSet

	// Intersect return the intersection of the receiver GTIDSet and the supplied GTIDSet
	Intersect(GTIDSet) GTIDSet

	// Union returns a union of the receiver GTIDSet and the supplied GTIDSet.
	Last() string
}

// gtidSetParsers maps flavor names to parser functions. It is used by
// ParsePosition().
var gtidSetParsers = make(map[string]func(string) (GTIDSet, error))
