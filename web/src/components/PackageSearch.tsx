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
  /**
   * The ecosystem travels with the name. Null means the row
   * stands for every ecosystem the name is in — either the
   * store cannot say, or there is only one.
   */
  onChoose: (name: string, ecosystem: string | null) => void;
  candidates: readonly PackageMatch[];
  deadEnd: boolean;
  /** The filters, which sit beside the input in the same row. */
  children?: React.ReactNode;
}) {
  const [focused, setFocused] = useState(false);
  const [active, setActive] = useState(-1);
  const listId = useId();
  const host = useRef<HTMLDivElement>(null);

  // The exact match is kept and marked, not filtered out.
  //
  // It used to be dropped, on the reasoning that the exact-name query
  // had already answered it. That reads the list as a spelling
  // correction, and it is also how you learn what exists: typing
  // `mail` offered `mailparser`, `mailcomposer`, `mailgun-js` and
  // nothing else, so the page implied `mail` was not a package while
  // the table below it was listing `mail`'s 174 dependants. The one
  // hidden row was the highest-count, most-relevant entry in the list.
  const typed = value.trim();
  // The exact name's rows first — all of its ecosystems, in the order
  // the query ranked them — then the rest, order preserved.
  const exact = candidates.filter((match) => match.name === typed);
  const others = candidates.filter((match) => match.name !== typed);
  const offered = [...exact, ...others];
  const open = offered.length > 0 && (focused || deadEnd);

  /** A row's identity is the pair, since a name can appear several times. */
  const rowKey = (match: PackageMatch) =>
    `${match.name}\u0000${match.ecosystem ?? ''}`;

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

  const choose = (name: string, ecosystem: string | null) => {
    setFocused(false);
    setActive(-1);
    onChoose(name, ecosystem);
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
              // The ecosystem travels on this path too: mouse and
              // keyboard selecting different things is the kind of
              // split a reader never sees coming.
              choose(offered[active]!.name, offered[active]!.ecosystem);
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
                key={rowKey(match)}
                id={`${listId}-${index}`}
                role="option"
                aria-selected={index === active}
                className={index === active ? 'active' : undefined}
                // mousedown, not click: the blur from clicking would
                // close the list before a click could land on it.
                onMouseDown={(event) => {
                  event.preventDefault();
                  choose(match.name, match.ecosystem);
                }}
                onMouseEnter={() => setActive(index)}
              >
                <span className="mono">{match.name}</span>
                {/*
                  Which registry this row is for. A name in several
                  ecosystems gets a row each, and the count beside it
                  is that ecosystem's — so picking `mail · gem` (167)
                  rather than `mail · maven` (6) is one click instead
                  of a name plus a filter.
                */}
                {match.ecosystem ? (
                  <span className="tag">{match.ecosystem}</span>
                ) : null}
                {/*
                  Only when there is something to distinguish it from.
                  With `mail` in three ecosystems and no near-misses,
                  every row is the typed name and the tag said nothing
                  three times over.
                */}
                {match.name === typed && others.length > 0 ? (
                  <span className="tag tag-exact">exact</span>
                ) : null}
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
