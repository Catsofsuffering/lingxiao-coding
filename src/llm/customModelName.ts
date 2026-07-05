export function toCustomModelName(model: string): string {
  const trimmed = model.trim();
  if (!trimmed || trimmed.startsWith('custom-')) return trimmed;
  return `custom-${trimmed}`;
}
