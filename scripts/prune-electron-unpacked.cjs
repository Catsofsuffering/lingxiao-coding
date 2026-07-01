#!/usr/bin/env node
'use strict';

/**
 * electron-builder afterPack hook.
 *
 * Conservative Windows MSI/NSIS/portable optimization:
 * prune files from resources/app.asar.unpacked/node_modules that are not needed
 * by the Windows runtime but noticeably increase disk footprint, directory scans,
 * and antivirus scanning cost.
 *
 * This deliberately does not touch app.asar and does not remove runtime binaries
 * such as .node/.dll/.exe or package.json entry metadata.
 */

const { existsSync, lstatSync, readdirSync, rmSync, statSync } = require('node:fs');
const { join, relative, sep } = require('node:path');

const DOC_OR_SOURCE_EXTENSIONS = new Set([
  '.c',
  '.h',
  '.markdown',
  '.md',
]);

function normalizePath(path) {
  return path.split(sep).join('/');
}

function isWindowsRuntimeDirName(name) {
  return /win32|windows/i.test(name);
}

function shouldPrunePrebuildDir(fullPath, nodeModulesDir) {
  const rel = normalizePath(relative(nodeModulesDir, fullPath));
  const segments = rel.split('/').filter(Boolean);
  const name = segments.at(-1) ?? '';

  // Keep generic containers such as "prebuilds"; prune only platform-specific children.
  if (/^prebuilds?$/i.test(name)) return false;

  // Common forms: prebuild-linux-x64, prebuild-darwin-arm64, prebuild-win32-x64.
  if (/^prebuild[-_]/i.test(name)) return !isWindowsRuntimeDirName(name);

  // Common form under container: prebuilds/linux-x64, prebuilds/darwin-arm64, prebuilds/win32-x64.
  if (segments.some((segment) => /^prebuilds?$/i.test(segment))) {
    return /^(linux|darwin|macos|freebsd|openbsd|android|alpine|musl)[-_]/i.test(name);
  }

  return false;
}

function shouldPruneAstGrepSourceDir(fullPath, nodeModulesDir) {
  const rel = normalizePath(relative(nodeModulesDir, fullPath));
  return /^@ast-grep\/lang-[^/]+\/src(?:\/|$)/.test(rel);
}

function walk(root, visit) {
  if (!existsSync(root)) return;
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    const fullPath = join(root, entry.name);
    if (entry.isDirectory()) {
      const shouldSkipChildren = visit(fullPath, entry) === false;
      if (!shouldSkipChildren) walk(fullPath, visit);
    } else if (entry.isFile()) {
      visit(fullPath, entry);
    }
  }
}

function safeRm(path) {
  rmSync(path, { recursive: true, force: true, maxRetries: 2 });
}

function getSize(path) {
  if (!existsSync(path)) return 0;
  const st = lstatSync(path);
  if (st.isFile()) return st.size;
  let total = 0;
  for (const entry of readdirSync(path, { withFileTypes: true })) {
    total += getSize(join(path, entry.name));
  }
  return total;
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
}

function pruneUnpackedNodeModules(nodeModulesDir) {
  const removed = [];
  const beforeBytes = getSize(nodeModulesDir);

  walk(nodeModulesDir, (fullPath, entry) => {
    if (entry.isDirectory()) {
      if (shouldPruneAstGrepSourceDir(fullPath, nodeModulesDir) || shouldPrunePrebuildDir(fullPath, nodeModulesDir)) {
        const bytes = getSize(fullPath);
        safeRm(fullPath);
        removed.push({ path: normalizePath(relative(nodeModulesDir, fullPath)), bytes, kind: 'dir' });
        return false;
      }
      return true;
    }

    const lowerName = entry.name.toLowerCase();
    const ext = lowerName.endsWith('.markdown')
      ? '.markdown'
      : lowerName.endsWith('.map')
        ? '.map'
        : lowerName.slice(lowerName.lastIndexOf('.'));

    if (lowerName.endsWith('.map') || DOC_OR_SOURCE_EXTENSIONS.has(ext)) {
      const bytes = statSync(fullPath).size;
      safeRm(fullPath);
      removed.push({ path: normalizePath(relative(nodeModulesDir, fullPath)), bytes, kind: 'file' });
    }
    return true;
  });

  const afterBytes = getSize(nodeModulesDir);
  const savedBytes = Math.max(0, beforeBytes - afterBytes);
  return { beforeBytes, afterBytes, savedBytes, removed };
}

async function afterPack(context) {
  const platformName = context?.electronPlatformName || context?.packager?.platform?.name;
  if (platformName && platformName !== 'win32') {
    console.log(`[prune-electron-unpacked] skip platform=${platformName}`);
    return;
  }

  const appOutDir = context?.appOutDir;
  if (!appOutDir) {
    console.log('[prune-electron-unpacked] skip: appOutDir unavailable');
    return;
  }

  const nodeModulesDir = join(appOutDir, 'resources', 'app.asar.unpacked', 'node_modules');
  if (!existsSync(nodeModulesDir)) {
    console.log(`[prune-electron-unpacked] skip: ${nodeModulesDir} not found`);
    return;
  }

  const result = pruneUnpackedNodeModules(nodeModulesDir);
  console.log(
    `[prune-electron-unpacked] removed ${result.removed.length} entries, saved ${formatBytes(result.savedBytes)} ` +
    `(${formatBytes(result.beforeBytes)} -> ${formatBytes(result.afterBytes)})`,
  );
}

module.exports = afterPack;
module.exports.default = afterPack;
module.exports.pruneUnpackedNodeModules = pruneUnpackedNodeModules;
module.exports._internal = {
  shouldPruneAstGrepSourceDir,
  shouldPrunePrebuildDir,
  isWindowsRuntimeDirName,
  normalizePath,
};
