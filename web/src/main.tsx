import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import './index.css';
import App from './App';

function configureReducedEffects(): void {
  const params = new URLSearchParams(window.location.search);
  const requested = params.get('reducedEffects') ?? params.get('lowEffects');
  const envRequested = import.meta.env.VITE_LINGXIAO_REDUCED_EFFECTS;
  const enabled = requested === '1' || requested === 'true' || envRequested === '1' || envRequested === 'true';

  if (enabled) {
    document.documentElement.dataset.reducedEffects = 'true';
  } else {
    delete document.documentElement.dataset.reducedEffects;
  }
}

configureReducedEffects();

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
