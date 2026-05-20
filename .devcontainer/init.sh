#!/bin/bash

set -e

# Initialize SDKMAN!
export SDKMAN_DIR="/usr/local/sdkman"
[[ -s "${SDKMAN_DIR}/bin/sdkman-init.sh" ]] && source "${SDKMAN_DIR}/bin/sdkman-init.sh"
# Install Gradle
sdk install gradle
# Install Scala
sdk install scala 2.12.21   # Replace with your version
sdk install sbt             # Optional: if using SBT
sdk install scalacli        # Highly recommended for 2026 workflows
# Install Gemini CLI
npm install -g @google/gemini-cli
