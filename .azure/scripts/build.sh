#!/usr/bin/env bash
set -e

echo "Build reason: ${BUILD_REASON}"
echo "Source branch: ${BRANCH}"

# Build with Maven
mvn $MVN_ARGS install
mvn $MVN_ARGS spotbugs:check

# Push to Nexus
if [ "$BUILD_REASON" == "PullRequest" ] ; then
    echo "Building Pull Request - nothing to push"
elif [[ "$BRANCH" != "refs/tags/"* ]] && [ "$BRANCH" != "refs/heads/main" ]; then
    echo "Not in main branch or in release tag - nothing to push"
else
    echo "In main branch or in release tag - pushing to nexus"
    ./.azure/scripts/push-to-nexus.sh
fi
