/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"fmt"
	"strconv"

	"github.com/pingcap/errors"
)

// SetDatabasePoolSize The constraints on this parameter are the same as in KB Addons
func SetDatabasePoolSize(value string) error {
	fmt.Printf("SetDatabasePoolSize: %v", value)
	i, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	if i < 1 {
		return errors.New("make sure that databasePoolSize >= 1")
	}
	databasePoolSize = i
	return nil
}

// SetDefaultBatchSize The constraints on this parameter are the same as in KB Addons
func SetDefaultBatchSize(value string) error {
	i, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	if i < 1 {
		return errors.New("make sure that defaultBatchSize >= 1")
	}
	defaultBatchSize = i
	return nil
}

// SetDefaultBatchInterval The constraints on this parameter are the same as in KB Addons
func SetDefaultBatchInterval(value string) error {
	i, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	if i < 1 {
		return errors.New("make sure that defaultBatchInterval >= 1")
	}
	defaultBatchInterval = i
	return nil
}

// SetTableGCInterval The constraints on this parameter are the same as in KB Addons
func SetTableGCInterval(value string) error {
	i, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	if i < 1 {
		return errors.New("make sure that tableGCInterval >= 1")
	}
	tableGCInterval = i
	return nil
}

// SetJobManagerRunningInterval The constraints on this parameter are the same as in KB Addons
func SetJobManagerRunningInterval(value string) error {
	i, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	if i < 1 {
		return errors.New("make sure that jobManagerRunningInterval >= 1")
	}
	jobManagerRunningInterval = i
	return nil
}

// SetThrottleCheckInterval The constraints on this parameter are the same as in KB Addons
func SetThrottleCheckInterval(value string) error {
	i, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	if i < 1 {
		return errors.New("make sure that throttleCheckInterval >= 1")
	}
	throttleCheckInterval = i
	return nil
}

// SetBatchSizeThreshold The constraints on this parameter are the same as in KB Addons
func SetBatchSizeThreshold(value string) error {
	i, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	if i < 1 || i > 1000000 {
		return errors.New("make sure that 1 <= batchSizeThreshold <= 1000000")
	}
	batchSizeThreshold = i
	return nil
}

// SetRatioOfBatchSizeThreshold The constraints on this parameter are the same as in KB Addons
func SetRatioOfBatchSizeThreshold(value string) error {
	i, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return err
	}
	if i < 0 || i > 1 {
		return errors.New("make sure that 0 <= ratioOfBatchSizeThreshold <= 1")
	}
	ratioOfBatchSizeThreshold = i
	return nil
}
