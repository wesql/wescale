/*
   Copyright ApeCloud, Inc.
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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

/*
	This file has been copied over from VTOrc package
*/

package inst

import (
	"regexp"
)

// InstanceKey is an instance indicator, identifued by hostname and port
type InstanceKey struct {
	Hostname string
	Port     int
}

var (
	ipv4Regexp = regexp.MustCompile(`^([0-9]+)[.]([0-9]+)[.]([0-9]+)[.]([0-9]+)$`)
)

// IsValid uses simple heuristics to see whether this key represents an actual instance
func (instanceKey *InstanceKey) IsIPv4() bool {
	return ipv4Regexp.MatchString(instanceKey.Hostname)
}
