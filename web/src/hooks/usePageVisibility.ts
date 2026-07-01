import { useEffect, useState } from 'react';

/**
 * Tracks whether the current document is visible to the user.
 *
 * Polling views (Logs, Metrics, Traces) use this to suspend background
 * `setInterval` fetches when the tab is hidden, avoiding wasted network
 * requests and CPU cycles.
 */
export function usePageVisibility(): boolean {
  const [visible, setVisible] = useState(
    typeof document !== 'undefined' ? document.visibilityState === 'visible' : true,
  );

  useEffect(() => {
    const handler = () => setVisible(document.visibilityState === 'visible');
    document.addEventListener('visibilitychange', handler);
    return () => document.removeEventListener('visibilitychange', handler);
  }, []);

  return visible;
}
