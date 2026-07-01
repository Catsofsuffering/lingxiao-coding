// Wrapper to run ToolFailureLoopGuard test via node --test with type stripping
import { register } from 'node:module';
import { pathToFileURL } from 'node:url';

register('node:typescript-module', pathToFileURL('./'));

// Fall back to strip-types loader
await import('./ToolFailureLoopGuard.test.ts');
