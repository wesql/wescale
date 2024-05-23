/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"fmt"
	"strings"

	vtgatepb "github.com/wesql/wescale/go/vt/proto/vtgate"
)

func ValidateReadAfterWriteConsistency(readAfterWriteConsistency string) error {
	if _, ok := vtgatepb.ReadAfterWriteConsistency_value[strings.ToUpper(readAfterWriteConsistency)]; !ok {
		return fmt.Errorf("read_after_write_consistency must be one of [EVENTUAL,SESSION,INSTANCE,GLOBAL]")
	}
	return nil
}

func ConvertReadAfterWriteConsistency(readAfterWriteConsistency string) vtgatepb.ReadAfterWriteConsistency {
	v, ok := vtgatepb.ReadAfterWriteConsistency_value[strings.ToUpper(readAfterWriteConsistency)]
	if !ok {
		return vtgatepb.ReadAfterWriteConsistency_EVENTUAL
	}
	return vtgatepb.ReadAfterWriteConsistency(v)
}
