/// <reference types="electron" />
import { app, BrowserWindow, Menu, shell } from 'electron';
import { existsSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { generateDefaultSettings, config as runtimeConfig } from '../config.js';
import { createServer, findAvailablePort, removePortFile, warnIfInsecureHostBinding, writePortFile } from '../server.js';
import { registerCleanup, runAllCleanups } from '../core/index.js';
import { isHardenedMode } from '../core/HardeningPolicy.js';
import { createLogger } from '../core/Log.js';
const desktopLogger = createLogger('lingxiao.desktop');let mainWindow: BrowserWindow | null = null;
let shutdownStarted = false;

const TRUTHY_ENV_VALUES = new Set(['1', 'true', 'yes', 'on']);
const FALSY_ENV_VALUES = new Set(['0', 'false', 'no', 'off']);

function hasArg(name: string): boolean {
  return process.argv.some((arg) => arg === name || arg.startsWith(`${name}=`));
}

function shouldUseReducedEffects(): boolean {
  const envValue = process.env.LINGXIAO_REDUCED_EFFECTS?.trim().toLowerCase();
  if (envValue && TRUTHY_ENV_VALUES.has(envValue)) return true;
  if (envValue && FALSY_ENV_VALUES.has(envValue)) return false;

  if (hasArg('--lingxiao-full-effects')) return false;
  if (hasArg('--lingxiao-reduced-effects') || hasArg('--low-effects')) return true;

  // Windows MSI/NSIS/portable 包默认启用低特效，降低 Electron 渲染端在高频日志/聊天刷新时的重绘成本。
  return app.isPackaged && process.platform === 'win32';
}

function appendUrlParam(url: string, key: string, value: string): string {
  const parsed = new URL(url);
  parsed.searchParams.set(key, value);
  return parsed.toString();
}

// ── Auto-updater ──────────────────────────────────────────────────────────────
// electron-updater 在打包后的 MSI/NSIS 安装版中自动检查 GitHub Releases。
// 开发模式（非 packaged）下跳过，避免无谓报错。

let updateNotificationShown = false;

async function setupAutoUpdater(): Promise<void> {
  if (!app.isPackaged) return;

  let autoUpdater;
  try {
    const mod = await import('electron-updater');
    autoUpdater = mod.autoUpdater;
  } catch {
    desktopLogger.warn('[Updater] electron-updater unavailable, skipping auto-update');
    return;
  }

  autoUpdater.autoDownload = true;
  autoUpdater.autoInstallOnAppQuit = true;

  autoUpdater.on('checking-for-update', () => {
    desktopLogger.info('[Updater] 正在检查更新...');
  });

  autoUpdater.on('update-available', (info) => {
    desktopLogger.info('[Updater] 发现新版本，开始下载...', { version: info.version });
  });

  autoUpdater.on('update-not-available', () => {
    desktopLogger.info('[Updater] 当前版本已是最新。');
  });

  autoUpdater.on('download-progress', (progress) => {
    const percent = progress.percent.toFixed(1);
    desktopLogger.info('[Updater] 下载中', { percent, transferred: progress.transferred, total: progress.total });
  });

  autoUpdater.on('update-downloaded', () => {
    desktopLogger.info('[Updater] 更新已下载，将在退出时自动安装。');
    // 通知主窗口展示更新提示（通过 webContents executeJavaScript 注入 toast）
    if (mainWindow && !updateNotificationShown) {
      updateNotificationShown = true;
      mainWindow.webContents.executeJavaScript(
        `if (window.showToast) { window.showToast('新版本已下载，重启后生效。', 'info', 8000); }`
      ).catch(() => {/* 窗口可能还没准备好 */});
    }
  });

  autoUpdater.on('error', (err) => {
    desktopLogger.error('[Updater] 检查更新失败', { error: err?.message || String(err) });
  });

  // 启动后 5 秒检查更新，之后每 4 小时检查一次
  const initialCheck = setTimeout(() => {
    autoUpdater.checkForUpdates().catch(() => {/* 静默失败 */});
  }, 5_000);
  initialCheck.unref?.();

  const periodicCheck = setInterval(() => {
    autoUpdater.checkForUpdates().catch(() => {/* 静默失败 */});
  }, 4 * 60 * 60 * 1000);
  periodicCheck.unref?.();
}

function resolveIconPath(): string | undefined {
  const __dirname = dirname(fileURLToPath(import.meta.url));
  const candidates = [
    join(__dirname, '..', 'web', 'public', 'logo.svg'),
    join(__dirname, '..', '..', 'web', 'public', 'logo.svg'),
  ];
  return candidates.find((candidate) => existsSync(candidate));
}

function configureDesktopResourcePaths(): void {
  const bundledSkillsDir = app.isPackaged
    ? join(process.resourcesPath, 'skills', 'bundled')
    : join(process.cwd(), 'skills', 'bundled');

  if (!process.env.LINGXIAO_BUNDLED_SKILLS_DIR && existsSync(bundledSkillsDir)) {
    process.env.LINGXIAO_BUNDLED_SKILLS_DIR = bundledSkillsDir;
  }
}

async function startDesktopServer(): Promise<string> {
  configureDesktopResourcePaths();
  generateDefaultSettings();

  const { fastify: server, token } = await createServer();
  const webHost = runtimeConfig.server.host;
  const requestedPort = runtimeConfig.server.random_port ? 0 : runtimeConfig.server.port;

  warnIfInsecureHostBinding(webHost, isHardenedMode());

  let actualPort = requestedPort;
  try {
    await server.listen({ host: webHost, port: requestedPort });
    const address = server.server.address();
    actualPort = (address && typeof address === 'object' ? address.port : null) ?? requestedPort;
  } catch (err: unknown) {
    if (err instanceof Error && 'code' in err && (err as NodeJS.ErrnoException).code === 'EADDRINUSE') {
      actualPort = await findAvailablePort(runtimeConfig.server.port, webHost);
      await server.listen({ host: webHost, port: actualPort });
    } else {
      throw err;
    }
  }

  writePortFile(actualPort, webHost);
  registerCleanup(() => removePortFile(), 2);
  registerCleanup(() => server.close(), 3);

  const displayHost = webHost === '0.0.0.0' ? 'localhost' : webHost;
  const baseUrl = `http://${displayHost}:${actualPort}`;
  const webUrl = token ? `${baseUrl}?token=${encodeURIComponent(token)}` : baseUrl;
  return shouldUseReducedEffects() ? appendUrlParam(webUrl, 'reducedEffects', '1') : webUrl;
}

function createMainWindow(url: string): BrowserWindow {
  const icon = resolveIconPath();
  const window = new BrowserWindow({
    width: 1440,
    height: 960,
    minWidth: 1100,
    minHeight: 720,
    show: false,
    title: 'LingXiao',
    ...(icon ? { icon } : {}),
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  });

  window.once('ready-to-show', () => {
    window.show();
  });

  window.webContents.setWindowOpenHandler(({ url: targetUrl }) => {
    void shell.openExternal(targetUrl);
    return { action: 'deny' };
  });

  void window.loadURL(url);
  return window;
}

async function shutdown(): Promise<void> {
  if (shutdownStarted) return;
  shutdownStarted = true;
  removePortFile();
  await runAllCleanups(10_000);
}

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    void shutdown().finally(() => app.quit());
  }
});

app.on('before-quit', (event) => {
  if (shutdownStarted) return;
  event.preventDefault();
  void shutdown().finally(() => app.quit());
});

void app.whenReady().then(async () => {
  Menu.setApplicationMenu(null);
  const url = await startDesktopServer();
  mainWindow = createMainWindow(url);
  setupAutoUpdater().catch(() => {/* 静默失败 */});

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      mainWindow = createMainWindow(url);
    }
  });
}).catch((err: unknown) => {
  desktopLogger.error('[Desktop] Failed to start', { error: String(err) });
  app.quit();
});
