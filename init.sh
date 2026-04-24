content = """#!/bin/bash
set -e

echo "=== personalInit: installing udunits2 and cfchecks ==="

# Try conda first (most likely available in this environment)
if command -v conda &> /dev/null; then
    echo "Conda found, installing udunits2 and cfchecks..."
    conda install -c conda-forge udunits2 cfchecks -y
elif command -v mamba &> /dev/null; then
    echo "Mamba found, installing udunits2 and cfchecks..."
    mamba install -c conda-forge udunits2 cfchecks -y
else
    echo "No conda/mamba found, trying apt-get + pip..."
    apt-get update && apt-get install -y libudunits2-dev
    pip install cfchecks
fi

echo "=== Done! cfchecks should now be available ==="
"""

with open("init.sh", "w") as f:
    f.write(content)

print("init.sh written successfully")
