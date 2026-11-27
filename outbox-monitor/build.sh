#!/bin/bash
# Lambda 번들 → dist/index.js (Node.js 20). AWS SDK v3 는 Lambda 런타임에 들어 있어 번들에서 뺀다.
# terraform(infra 레포)은 이 파일을 infra 쪽 경로로 복사해 커밋한 것을 zip 한다 — qa-metrics-batch 와 같은 방식
set -euo pipefail
cd "$(dirname "$0")"
npx tsc --noEmit
npx esbuild src/index.ts --bundle --platform=node --target=node20 --format=cjs \
  --external:@aws-sdk/* --outfile=dist/index.js
ls -l dist/index.js
