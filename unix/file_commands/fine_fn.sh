#!/bin/bash

# Check if file exists
if [ ! -f ./data.csv ]; then
        echo "File not Found!"
fi

# Count lines in file
wc -l ./data.csv

# Add newline to file
echo "" >> ./data.csv

# Delete line 3 from file
sed -i '3d' ./data.csv

# Search keyword "Paris" in file
if grep -q Paris ./data.csv
then
        echo "Paris found"
else
        echo "Paris not found"
fi

# Print data
cat ./data.csv
