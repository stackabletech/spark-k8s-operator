#!/bin/bash

echo "Creating client cert"
FQDN="minio"

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
  -days 36500 \
  -out root-ca.crt.pem \
  -subj "/C=DE/ST=Schleswig-Holstein/L=Wedel/O=Stackable Signing Authority Inc/CN=stackable.de"

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
  -days 36500 \
  -copy_extensions copy

echo "Copying the files to match the api of the secret-operator"
cp root-ca.crt.pem ca.crt
cp client.key.pem tls.key
cp client.crt.pem tls.crt

echo "To create a k8s secret run"
echo "kubectl create secret generic foo --from-file=ca.crt --from-file=tls.crt --from-file=tls.key"
