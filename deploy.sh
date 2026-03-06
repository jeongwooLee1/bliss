#!/bin/bash
# Bliss 배포 스크립트 - 파싱 검증 후 배포
set -e

SRC="${1:-/home/claude/index.html}"
VERSION="${2:-}"

echo "📋 배포 준비: $SRC"

# 1. 파일 존재 확인
if [ ! -f "$SRC" ]; then
  echo "❌ 파일 없음: $SRC"
  exit 1
fi

# 2. JSX 파싱 검증
echo "🔍 JSX 파싱 검증 중..."
node - "$SRC" << 'NODEEOF'
const fs = require('fs');
const path = process.argv[1];
const content = fs.readFileSync(path, 'utf8');
const start = content.indexOf('<script type="text/babel">') + '<script type="text/babel">'.length;
const end = content.lastIndexOf('</script>');
if (start < 0 || end < 0) { console.error('❌ script 태그 없음'); process.exit(1); }
const jsx = content.slice(start, end);
try {
  const parser = require('/home/claude/.npm-global/lib/node_modules/@babel/parser');
  parser.parse(jsx, { plugins: ['jsx'], sourceType: 'script' });
  console.log('✅ JSX 파싱 성공');
} catch(e) {
  console.error('❌ JSX 파싱 오류:', e.message, 'Line:', e.loc?.line);
  process.exit(1);
}
NODEEOF

# 3. 중복 선언 체크
echo "🔍 중복 선언 체크..."
node - "$SRC" << 'NODEEOF'
const fs = require('fs');
const content = fs.readFileSync(process.argv[1], 'utf8');
const start = content.indexOf('<script type="text/babel">') + '<script type="text/babel">'.length;
const end = content.lastIndexOf('</script>');
const jsx = content.slice(start, end);
// const 선언 중복 체크
const decls = {};
const matches = jsx.matchAll(/^(?:  )?const ([a-zA-Z][a-zA-Z0-9_]*)\s*=/gm);
for (const m of matches) {
  decls[m[1]] = (decls[m[1]] || 0) + 1;
}
const dups = Object.entries(decls).filter(([k,v]) => v > 1 && !['inpS','i'].includes(k));
if (dups.length > 0) {
  console.warn('⚠️  중복 const:', dups.map(([k,v])=>`${k}(${v}회)`).join(', '));
} else {
  console.log('✅ 중복 선언 없음');
}
NODEEOF

# 4. 버전 업데이트
if [ -n "$VERSION" ]; then
  echo "📌 버전: $VERSION"
  cp "$SRC" index.html
  echo "$VERSION" > version.txt
else
  cp "$SRC" index.html
  # 현재 버전 읽기
  VERSION=$(grep -o 'BLISS_V = "[^"]*"' index.html | grep -o '"[^"]*"' | tr -d '"')
  echo "📌 현재 버전: $VERSION"
  echo "$VERSION" > version.txt
fi

# 5. Git 배포
echo "🚀 GitHub 배포 중..."
git add index.html version.txt
git commit -m "v${VERSION}: 배포" 2>/dev/null || git commit -m "deploy v${VERSION}"
git push origin main

echo ""
echo "✅ 배포 완료! v${VERSION}"
echo "   강제 새로고침: Ctrl+Shift+R"
