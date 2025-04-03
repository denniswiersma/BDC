#!/bin/bash

DEST="bioinf.nl:/students/2023-2024/Thema12/dwiersma_BDC/BDC"

# Path to the .syncignore file
SYNCIGNORE=".syncignore"

# Check if the .syncignore file exists
if [ ! -f "$SYNCIGNORE" ]; then
    echo "$SYNCIGNORE file not found. Please create one to specify exclusions."
    exit 1
fi

# Construct exclude parameters from the .syncignore file
EXCLUDE_PARAMS=""
while IFS= read -r line; do
    # Skip empty lines
    if [ -z "$line" ]; then
        continue
    fi
    EXCLUDE_PARAMS+=" --exclude $line"
done < "$SYNCIGNORE"

# First copy to the first destination
echo "COPYING TO SERVER..."
rsync -rav --progress $EXCLUDE_PARAMS . "$DEST"

echo "DONE! [$(date +'%Y-%m-%d %H:%M:%S')]"
