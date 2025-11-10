#!/bin/bash
# Build and push Docker image with version tags
# This script builds a Linux container image suitable for Kubernetes

set -e

# Get version from Cargo.toml
VERSION=$(grep '^version' Cargo.toml | cut -d'"' -f2)
IMAGE_NAME="dmarshaltu/ehub_debug_consumer"

echo "=========================================="
echo "Building Docker image for version $VERSION"
echo "Image: ${IMAGE_NAME}"
echo "=========================================="

# Build the image with version tag
echo "Building ${IMAGE_NAME}:${VERSION}..."
docker build -t ${IMAGE_NAME}:${VERSION} .

# Tag as latest
echo "Tagging as latest..."
docker tag ${IMAGE_NAME}:${VERSION} ${IMAGE_NAME}:latest

echo ""
echo "=========================================="
echo "Pushing images to Docker Hub..."
echo "=========================================="

# Push version tag
echo "Pushing ${IMAGE_NAME}:${VERSION}..."
docker push ${IMAGE_NAME}:${VERSION}

# Push latest tag
echo "Pushing ${IMAGE_NAME}:latest..."
docker push ${IMAGE_NAME}:latest

echo ""
echo "=========================================="
echo "Successfully pushed:"
echo "  - ${IMAGE_NAME}:${VERSION}"
echo "  - ${IMAGE_NAME}:latest"
echo "=========================================="

