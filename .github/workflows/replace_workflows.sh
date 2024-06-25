#!/bin/bash

# Define workflows and archive directories
workflows_dir="."
archive_dir="./archive"

# Check if the workflows directory exists
if [ ! -d "$workflows_dir" ]; then
  echo "Directory $workflows_dir does not exist!"
  exit 1
fi

# Check if the archive directory exists
if [ ! -d "$archive_dir" ]; then
  echo "Directory $archive_dir does not exist!"
  exit 1
fi

# Iterate over all yml files in the workflows directory
for file in "$workflows_dir"/*.yml; do
  # Get the filename
  filename=$(basename "$file")

  # Check if a file with the same name exists in the archive directory
  if [ -f "$archive_dir/$filename" ]; then
    # Replace the file in the workflows directory with the one from the archive
    mv "$archive_dir/$filename" "$workflows_dir/$filename"
    echo "Replaced $filename with archive version"
  fi
done