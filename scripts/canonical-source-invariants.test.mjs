#!/usr/bin/env node

import assert from 'node:assert/strict';
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

const scriptsDir = dirname(fileURLToPath(import.meta.url));
const pkgRoot = resolve(scriptsDir, '..');
const require = createRequire(import.meta.url);
const forbiddenSourceEntrypoints = [
  ['src', 'next'].join('-'),
  ['NEXT', 'GEN', 'ENABLED'].join('_'),
  ['dist', 'pub'].join('-'),
];

function walkFiles(root, relativeRoot = '') {
  const files = [];
  for (const entry of readdirSync(join(root, relativeRoot), { withFileTypes: true })) {
    const relativePath = relativeRoot ? join(relativeRoot, entry.name) : entry.name;
    const fullPath = join(root, relativePath);
    if (entry.isDirectory()) {
      files.push(...walkFiles(root, relativePath));
    } else if (entry.isFile()) {
      files.push(fullPath);
    }
  }
  return files;
}

function readText(filePath) {
  return readFileSync(filePath, 'utf8');
}

test('package scripts and build scripts do not reference retired source entrypoints', () => {
  const files = [
    join(pkgRoot, 'package.json'),
    ...walkFiles(join(pkgRoot, 'scripts')).filter((filePath) => /\.(?:mjs|js|json)$/.test(filePath)),
  ];

  const hits = [];
  for (const filePath of files) {
    const text = readText(filePath);
    for (const marker of forbiddenSourceEntrypoints) {
      if (text.includes(marker)) {
        hits.push(`${relative(pkgRoot, filePath).replace(/\\/g, '/')}: ${marker}`);
      }
    }
  }

  assert.deepEqual(hits, []);
});

test('postinstall resolves bundled skill registry from the canonical dist output only', () => {
  const postinstallPath = join(pkgRoot, 'scripts', 'postinstall.mjs');
  const text = readText(postinstallPath);

  assert.match(text, /resolve\(pkgRoot, 'dist\/core\/BundledSkillRegistry\.js'\)/);
  assert.equal(text.includes(['dist', 'pub'].join('-')), false);
});

test('build and dist test runners require generated files to map back to src', () => {
  for (const scriptName of ['build.mjs', 'run-tests.mjs']) {
    const scriptPath = join(pkgRoot, 'scripts', scriptName);
    const text = readText(scriptPath);

    assert.match(text, /existsSync\([^)]*pkgRoot[^)]*'src'/s);
    assert.doesNotMatch(text, /isExcluded(?:Test)?Source/);
  }
});

test('i18n locale values interpolate with double braces {{var}}, not single braces {var}', () => {
  // i18next v24 only substitutes {{var}}; a lone {var} renders literally.
  // This previously made the chat search counter show raw "{current}/{total} 个结果"
  // and silently broke ~16 other count/percent strings. Guard the whole class.
  // Negative lookbehind/ahead keep {{var}} (correct) from matching.
  const SINGLE_BRACE_VAR = /(?<!\{)\{[a-zA-Z_][a-zA-Z0-9_]*\}(?!\})/;

  const collectStrings = (obj, prefix, out) => {
    for (const [key, value] of Object.entries(obj)) {
      const path = prefix ? `${prefix}.${key}` : key;
      if (typeof value === 'string') out.push([path, value]);
      else if (value && typeof value === 'object') collectStrings(value, path, out);
    }
    return out;
  };

  const localeDir = join(pkgRoot, 'web', 'src', 'i18n', 'locales');
  const offenders = [];
  for (const name of ['zh.json', 'en.json']) {
    const data = JSON.parse(readText(join(localeDir, name)));
    for (const [key, value] of collectStrings(data, '', [])) {
      if (SINGLE_BRACE_VAR.test(value)) offenders.push(`${name} ${key}: ${JSON.stringify(value)}`);
    }
  }
  assert.deepEqual(
    offenders,
    [],
    `Single-brace interpolation found (i18next requires {{var}}):\n${offenders.join('\n')}`,
  );
});

test('web i18n locale files (zh.json / en.json) have identical top-level key sets', () => {
  // 单一事实源守护：两套 locale 必须一一对应，杜绝只加了一种语言的 key。
  const localeDir = join(pkgRoot, 'web', 'src', 'i18n', 'locales');
  const zh = JSON.parse(readText(join(localeDir, 'zh.json')));
  const en = JSON.parse(readText(join(localeDir, 'en.json')));
  const zhKeys = new Set(Object.keys(zh));
  const enKeys = new Set(Object.keys(en));
  const onlyZh = [...zhKeys].filter((k) => !enKeys.has(k));
  const onlyEn = [...enKeys].filter((k) => !zhKeys.has(k));
  assert.deepEqual(onlyZh, [], `keys present only in zh.json: ${onlyZh.slice(0, 30).join(', ')}`);
  assert.deepEqual(onlyEn, [], `keys present only in en.json: ${onlyEn.slice(0, 30).join(', ')}`);
});

test('electron afterPack pruning removes only Windows-unneeded unpacked artifacts', () => {
  const { pruneUnpackedNodeModules } = require('./prune-electron-unpacked.cjs');
  const dir = join(tmpdir(), `lx-prune-electron-${Date.now()}-${Math.random().toString(16).slice(2)}`);
  const nodeModulesDir = join(dir, 'resources', 'app.asar.unpacked', 'node_modules');

  const write = (relativePath, content = 'x') => {
    const target = join(nodeModulesDir, ...relativePath.split('/'));
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, content);
  };

  try {
    write('@ast-grep/lang-typescript/src/parser.c');
    write('@ast-grep/lang-typescript/src/parser.h');
    write('@ast-grep/lang-typescript/prebuild-linux-x64/tree-sitter-typescript.node');
    write('@ast-grep/lang-typescript/prebuild-darwin-arm64/tree-sitter-typescript.node');
    write('@ast-grep/lang-typescript/prebuild-win32-x64/tree-sitter-typescript.node');
    write('tree-sitter-grammar/prebuilds/linux-x64/parser.node');
    write('tree-sitter-grammar/prebuilds/darwin-arm64/parser.node');
    write('tree-sitter-grammar/prebuilds/win32-x64/parser.node');
    write('native-addon/build/Release/addon.node');
    write('native-addon/bin/helper.exe');
    write('native-addon/bin/runtime.dll');
    write('native-addon/package.json', '{"name":"native-addon"}');
    write('native-addon/README.md');
    write('native-addon/index.js.map');
    write('native-addon/include/addon.h');
    write('native-addon/src/addon.c');

    const result = pruneUnpackedNodeModules(nodeModulesDir);
    assert.ok(result.removed.length >= 6);

    assert.equal(existsSync(join(nodeModulesDir, '@ast-grep/lang-typescript/src/parser.c')), false);
    assert.equal(existsSync(join(nodeModulesDir, '@ast-grep/lang-typescript/prebuild-linux-x64/tree-sitter-typescript.node')), false);
    assert.equal(existsSync(join(nodeModulesDir, '@ast-grep/lang-typescript/prebuild-darwin-arm64/tree-sitter-typescript.node')), false);
    assert.equal(existsSync(join(nodeModulesDir, 'tree-sitter-grammar/prebuilds/linux-x64/parser.node')), false);
    assert.equal(existsSync(join(nodeModulesDir, 'tree-sitter-grammar/prebuilds/darwin-arm64/parser.node')), false);
    assert.equal(existsSync(join(nodeModulesDir, 'native-addon/README.md')), false);
    assert.equal(existsSync(join(nodeModulesDir, 'native-addon/index.js.map')), false);
    assert.equal(existsSync(join(nodeModulesDir, 'native-addon/include/addon.h')), false);
    assert.equal(existsSync(join(nodeModulesDir, 'native-addon/src/addon.c')), false);

    assert.equal(existsSync(join(nodeModulesDir, '@ast-grep/lang-typescript/prebuild-win32-x64/tree-sitter-typescript.node')), true);
    assert.equal(existsSync(join(nodeModulesDir, 'tree-sitter-grammar/prebuilds')), true);
    assert.equal(existsSync(join(nodeModulesDir, 'tree-sitter-grammar/prebuilds/win32-x64/parser.node')), true);
    assert.equal(existsSync(join(nodeModulesDir, 'native-addon/build/Release/addon.node')), true);
    assert.equal(existsSync(join(nodeModulesDir, 'native-addon/bin/helper.exe')), true);
    assert.equal(existsSync(join(nodeModulesDir, 'native-addon/bin/runtime.dll')), true);
    assert.equal(existsSync(join(nodeModulesDir, 'native-addon/package.json')), true);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
