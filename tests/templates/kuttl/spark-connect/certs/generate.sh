#!/bin/bash

set -euo pipefail

# Function to display help message
show_help() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS] [COMMON_NAME] [LIFETIME_DAYS]

Generate a self-signed root CA and client certificate for TLS connections.

Arguments:
  COMMON_NAME      Common name for the certificate (default: minio)
  LIFETIME_DAYS    Validity period in days for CA and client cert (default: 36500)

Options:
  -h, --help       Show this help message and exit

Examples:
  $(basename "$0")                    # Use defaults (minio, 36500 days)
  $(basename "$0") myserver           # Custom common name
  $(basename "$0") myserver 365       # Custom common name and 1 year validity

The script generates the following files:
  - ca.crt (Root CA)
  - tls.crt (Client certificate)
  - tls.key (Client private key)
  - minio-tls-ca-secret.yaml (Kubernetes secret manifest)
EOF
    exit 0
}

# Parse command line arguments
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    show_help
fi

# Set defaults
FQDN="${1:-minio}"
LIFETIME="${2:-36500}"

# Validate that LIFETIME is a number
if ! [[ "$LIFETIME" =~ ^[0-9]+$ ]]; then
    echo "Error: LIFETIME_DAYS must be a positive integer" >&2
    exit 1
fi

echo "Generating certificates with:"
echo "  Common Name: ${FQDN}"
echo "  Lifetime: ${LIFETIME} days"
echo ""

echo "Creating Root Certificate Authority"
openssl genrsa \
  -out root-ca.key.pem \
  2048

echo "Self-signing the Root Certificate Authority"
openssl req \
  -x509 \
  -new \
  -nodes \
  -key root-ca.key.pem \
  -days "${LIFETIME}" \
  -out root-ca.crt.pem \
  -subj "/C=DE/ST=Schleswig-Holstein/L=Wedel/O=Stackable Signing Authority Inc/CN=stackable.de"

echo "Creating client private key"
openssl genrsa \
  -out client.key.pem \
  2048

echo "Creating the CSR"
openssl req -new \
  -key client.key.pem \
  -out client.csr.pem \
  -subj "/C=DE/ST=Schleswig-Holstein/L=Wedel/O=Stackable/CN=${FQDN}" \
  -addext "subjectAltName = DNS:${FQDN}, DNS:localhost"

echo "Signing the client cert with the root ca"
openssl x509 \
  -req -in client.csr.pem \
  -CA root-ca.crt.pem \
  -CAkey root-ca.key.pem \
  -CAcreateserial \
  -out client.crt.pem \
  -days "${LIFETIME}" \
  -copy_extensions copy

echo "Copying the files to match the api of the secret-operator"
mv root-ca.crt.pem ca.crt
mv client.key.pem tls.key
mv client.crt.pem tls.crt

echo ""
echo "Generating Kubernetes secret manifest..."

# Calculate dates
GENERATION_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
EXPIRATION_DATE=$(date -u -d "+${LIFETIME} days" +"%Y-%m-%dT%H:%M:%SZ")

# Base64 encode the certificate files
CA_CRT_B64=$(base64 -w 0 < ca.crt)
TLS_CRT_B64=$(base64 -w 0 < tls.crt)
TLS_KEY_B64=$(base64 -w 0 < tls.key)

# Generate the Kubernetes secret manifest
cat > minio-tls-ca-secret.yaml << EOF
# Generated with certs/generate.sh in this test folder.
---
apiVersion: v1
kind: Secret
metadata:
  name: minio-tls-ca
  labels:
    secrets.stackable.tech/class: minio-tls-ca
  annotations:
    cert.common-name: "${FQDN}"
    cert.generated-at: "${GENERATION_DATE}"
    cert.expires-at: "${EXPIRATION_DATE}"
data:
  ca.crt: ${CA_CRT_B64}
  tls.crt: ${TLS_CRT_B64}
  tls.key: ${TLS_KEY_B64}
EOF

echo ""
echo "Certificate generation complete!"
echo "Generated files:"
echo "  - ca.crt, tls.crt, tls.key (certificate files)"
echo "  - minio-tls-ca-secret.yaml (Kubernetes secret manifest)"
echo ""
