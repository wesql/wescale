#!/bin/bash
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).




failpoints=(
"vitess.io/vitess/go/vt/vtgate/testPanic=return(2)"
)

# Create an empty string
result=""

# Loop through each element in the failpoints array
for item in "${failpoints[@]}"; do
  # Concatenate each element to the result string, separated by ";"
  result+="${item};"
done

# Remove the last ";"
result=${result%;}

# Print the result
echo "GO_FAILPOINTS=\"${result}\""
export GO_FAILPOINTS="${result}"