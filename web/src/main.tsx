/**
 * Mount point.
 *
 * StrictMode is on deliberately. It double-invokes effects in
 * development, which surfaces exactly the class of bug this rewrite
 * exists to remove: an effect that is not safe to run twice is an
 * effect that is secretly imperative.
 */
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

import { App } from './app';
import { ErrorBoundary } from './components/ErrorBoundary';

const host = document.getElementById('root');
if (!host) throw new Error('missing #root');

createRoot(host).render(
  <StrictMode>
    {/* Outside the page, so a failure anywhere in it — its first
        route included — is a message rather than a blank page (#42). */}
    <ErrorBoundary>
      <App />
    </ErrorBoundary>
  </StrictMode>,
);
