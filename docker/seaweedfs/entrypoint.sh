#!/bin/sh
# Single-node SeaweedFS (master + volume + filer + S3 gateway). The S3 identity is generated from environment
# variables at start-up so no key is stored in the repository or the image.
set -eu
: "${S3_ACCESS_KEY:?S3_ACCESS_KEY is required}"
: "${S3_SECRET_KEY:?S3_SECRET_KEY is required}"
mkdir -p /data /etc/seaweedfs-runtime
cat > /etc/seaweedfs-runtime/s3.json <<JSON
{"identities": [{"name": "signalflow", "credentials": [{"accessKey": "${S3_ACCESS_KEY}", "secretKey": "${S3_SECRET_KEY}"}],
  "actions": ["Admin", "Read", "Write", "List", "Tagging"]}]}
JSON
chmod 600 /etc/seaweedfs-runtime/s3.json
exec weed server -dir=/data -ip=seaweedfs -ip.bind=0.0.0.0 -volume.max=0 -master.volumeSizeLimitMB=256 \
  -s3 -s3.port=8333 -s3.config=/etc/seaweedfs-runtime/s3.json
