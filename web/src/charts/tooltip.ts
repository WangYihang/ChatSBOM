/**
 * Tooltip content, as data rather than markup.
 *
 * The original took an HTML string and assigned it to `innerHTML`, and
 * those strings were assembled from dataset values — package names among
 * them. Anyone can publish a package called
 * `<img src=x onerror=...>`, so that was untrusted input reaching an
 * HTML sink on a page that also holds an API relay. Structured content
 * has no sink: every field goes through a text node.
 */
export interface TooltipContent {
  title: string;
  lines: string[];
}

/** Render content into a host as text nodes. No HTML parsing anywhere. */
export function renderTooltip(host: HTMLElement, content: TooltipContent): void {
  const title = document.createElement('strong');
  title.textContent = content.title;
  host.replaceChildren(
    title,
    ...content.lines.map((line) => {
      const div = document.createElement('div');
      div.textContent = line;
      return div;
    }),
  );
}
