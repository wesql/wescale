/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package failpointutil

import (
	"errors"

	"github.com/pingcap/failpoint"
)

type failpointParam struct {
	packageName string // failpoint packgeName used by failpoint.Enable
	retValue    string // retValue use to inject
}
type failpointTable struct {
	fpTable map[string]failpointParam
}

var FPTable failpointTable

func init() {

}
func (fpt *failpointTable) reEnable() {
	for _, value := range fpt.fpTable {
		err := failpoint.Enable(value.packageName, value.retValue)
		if err != nil {
			return
		}
	}
}

func (fpt *failpointTable) Inject(failpointName string, fpbody interface{}, packegeName string) {
	failpoint.Inject(failpointName, fpbody)
	fpt.fpTable[failpointName] = failpointParam{
		packageName: packegeName,
	}
}
func (fpt *failpointTable) AddFailpoint(failpointName string, retValue string) error {
	if fpParam, exists := fpt.fpTable[failpointName]; exists {
		fpParam.retValue = retValue
		err := failpoint.Enable(fpParam.packageName, fpParam.retValue)
		if err != nil {
			return err
		}
	} else {
		// failpointName not exist
		return errors.New("failpointName not exist")
	}
	return nil
}
func (fpt *failpointTable) RemoveFailpoint(failpointName string) error {
	if fpParam, exists := fpt.fpTable[failpointName]; exists {
		err := failpoint.Disable(fpParam.packageName)
		if err != nil {
			return err
		}
	}
	delete(fpt.fpTable, failpointName)
	return nil
}
