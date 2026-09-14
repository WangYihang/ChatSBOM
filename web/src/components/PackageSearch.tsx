/**
 * The search input, and the candidates it offers.
 *
 * The input on its own was an exact-name lookup with nothing behind it,
 * which produced a dead end for the most natural thing anyone types:
 * `laravel` answered "No repository in the dataset depends on laravel."
 * while 98 repositories depend on `laravel/framework`. Literally true —
 * no package is named exactly `laravel` — and it reads as "nobody uses
 * Laravel".
 *
 * So the list is not a convenience. It is the difference between a
 * query language and a search box.
 *
 * A combobox rather than a `<datalist>`: the native element cannot show
 * the repository count beside each name, which is the number that makes
 * the choice for you, and its keyboard behaviour differs per browser.
 */
import { useEffect, useId, useRef, useState } from 'react';

import type { PackageMatch } from '../d1/queries';

export function PackageSearch({
  value,
  onChange,
  onChoose,
  candidates,
  /**
   * Whether the exact-name query came back empty.
   *
   * When it did, the candidates are shown whether or not the input has
   * focus: that is the dead end this component exists to remove, and a
   * reader who has already looked away from the field is exactly the
   * one who needs to see it.
   */
  deadEnd,
  children,
}: {
  value: string;
  onChange: (next: string) => void;
  onChoose: (name: string) => void;
  candidates: readonly PackageMatch[];
  deadEnd: boolean;
  /** The filters, which sit beside the input in the same row. */
  children?: React.ReactNode;
}) {
  const [focused, setFocused] = useState(false);
  const [active, setActive] = useState(-1);
  const listId = useId();
  const host = useRef<HTMLDivElement>(null);

  // A candidate identical to what was typed is not a suggestion. The
  // exact-name query has already answered it, and offering it back is
  // the list saying "did you mean what you said".
  const offered = candidates.filter((match) => match.name !== value.trim());
  const open = offered.length > 0 && (focused || deadEnd);

  // A stale highlight survives the list changing under it and lands the
  // reader on a package they never looked at.
  useEffect(() => setActive(-1), [value]);

  useEffect(() => {
    if (!focused) return;
    const away = (event: MouseEvent) => {
      if (!host.current?.contains(event.target as Node)) setFocused(false);
    };
    document.addEventListener('mousedown', away);
    return () => document.removeEventListener('mousedown', away);
  }, [focused]);

  const choose = (name: string) => {
    setFocused(false);
    setActive(-1);
    onChoose(name);
  };

  return (
    <div className="controls" style={{ marginTop: '.5rem' }} ref={host}>
      <div className="searchwrap">
        <input
          id="package"
          type="search"
          autoComplete="off"
          spellCheck={false}
          placeholder="laravel, express, spring-boot-starter-web…"
          aria-label="Package name"
          role="combobox"
          aria-expanded={open}
          aria-controls={listId}
          aria-autocomplete="list"
          {...(open && active >= 0
            ? { 'aria-activedescendant': `${listId}-${active}` }
            : {})}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          onFocus={() => setFocused(true)}
          onKeyDown={(event) => {
            if (!open) return;
            if (event.key === 'ArrowDown') {
              event.preventDefault();
              setActive((i) => (i + 1) % offered.length);
            } else if (event.key === 'ArrowUp') {
              event.preventDefault();
              setActive((i) => (i <= 0 ? offered.length - 1 : i - 1));
            } else if (event.key === 'Enter' && active >= 0) {
              // Only with a highlighted row. Plain Enter must keep
              // meaning "search for what I typed", or a stray keypress
              // silently redirects the query.
              event.preventDefault();
              choose(offered[active]!.name);
            } else if (event.key === 'Escape') {
              setFocused(false);
              setActive(-1);
            }
          }}
        />

        {open ? (
          <ul className="suggestions" id={listId} role="listbox">
            {deadEnd ? (
              <li className="suggest-head" role="presentation">
                Nothing is named {value.trim()}. These are:
              </li>
            ) : null}
            {offered.map((match, index) => (
              <li
                key={match.name}
                id={`${listId}-${index}`}
                role="option"
                aria-selected={index === active}
                className={index === active ? 'active' : undefined}
                // mousedown, not click: the blur from clicking would
                // close the list before a click could land on it.
                onMouseDown={(event) => {
                  event.preventDefault();
                  choose(match.name);
                }}
                onMouseEnter={() => setActive(index)}
              >
                <span className="mono">{match.name}</span>
                <span className="num">
                  {match.repositoryCount.toLocaleString()}
                </span>
              </li>
            ))}
          </ul>
        ) : null}
      </div>

      {children}
    </div>
  );
}
