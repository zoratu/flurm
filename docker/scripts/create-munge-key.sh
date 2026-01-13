#!/bin/bash
# Create MUNGE key for shared authentication

KEY_FILE=${1:-/etc/munge/munge.key}

echo "Creating MUNGE key at $KEY_FILE"

# Create directory if needed
mkdir -p "$(dirname "$KEY_FILE")"

# Generate random key
dd if=/dev/urandom bs=1 count=1024 > "$KEY_FILE" 2>/dev/null

# Set permissions
chmod 400 "$KEY_FILE"

echo "MUNGE key created successfully"
