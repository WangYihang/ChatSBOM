/**
 * The search box's candidate list.
 *
 * The defect it exists for is a dead end, not a missing convenience:
 * typing `laravel` answered "No repository in the dataset depends on
 * laravel." while 98 repositories depend on `laravel/framework`.
 * Literally true, since no package is named exactly `laravel`, and it
 * reads as "nobody uses Laravel". So the assertion that matters most
 * below is the one about `deadEnd`.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { PackageSearch } from '../src/components/PackageSearch';

beforeEach(() => cleanup());

/** Real rows, ranked as the query now ranks them. */
const CANDIDATES = [
  {
    name: 'laravel/framework', ecosystem: 'composer',
    repositoryCount: 98, nameTotal: 98,
  },
  {
    name: 'laravel/serializable-closure', ecosystem: 'composer',
    repositoryCount: 77, nameTotal: 77,
  },
  {
    name: 'laravel/tinker', ecosystem: 'composer',
    repositoryCount: 70, nameTotal: 70,
  },
  {
    name: 'laravel-enso/core', ecosystem: 'composer',
    repositoryCount: 1, nameTotal: 1,
  },
];

function mount(
  props: Partial<React.ComponentProps<typeof PackageSearch>> = {},
) {
  const onChoose = vi.fn();
  const onChange = vi.fn();
  const view = render(
    <PackageSearch
      value="laravel"
      onChange={onChange}
      onChoose={onChoose}
      candidates={CANDIDATES}
      deadEnd={false}
      {...props}
    />,
  );
  return { ...view, onChoose, onChange };
}

const input = () => screen.getByLabelText('Package name');
const options = () => screen.queryAllByRole('option');

describe('PackageSearch', () => {
  it('offers nothing until the box is used', () => {
    mount();
    expect(options()).toHaveLength(0);
    expect(input().getAttribute('aria-expanded')).toBe('false');
  });

  it('offers candidates once the box has focus', () => {
    mount();
    fireEvent.focus(input());
    // The ecosystem sits between the name and the count now, because
    // a name alone does not identify a package.
    expect(options().map((o) => o.textContent)).toEqual([
      'laravel/frameworkcomposer98',
      'laravel/serializable-closurecomposer77',
      'laravel/tinkercomposer70',
      'laravel-enso/corecomposer1',
    ]);
  });

  it('shows them unfocused when the exact name found nothing', () => {
    /**
     * The whole point. A reader who typed `laravel`, read "No
     * repository in the dataset depends on laravel." and looked away
     * from the field is exactly the one who needs to see
     * `laravel/framework` — so focus cannot be the condition.
     */
    mount({ deadEnd: true });
    expect(options().length).toBeGreaterThan(0);
    expect(screen.getByText(/Nothing is named laravel/)).toBeTruthy();
  });

  it('puts the popular package first, not the alphabetical one', () => {
    // `laravel-enso/core` sorts before `laravel/framework` by name —
    // `-` is 0x2D, `/` is 0x2F — which is how forty one-dependant
    // packages used to crowd out the one with 98. The component renders
    // the order it is given; this pins that it does not re-sort.
    mount({ deadEnd: true });
    expect(options()[0]!.textContent).toContain('laravel/framework');
  });

  it('shows the repository count beside each name', () => {
    // The number that makes the choice. It is also why this is a
    // combobox and not a <datalist>, which cannot render it.
    mount({ deadEnd: true });
    expect(options()[0]!.textContent).toContain('98');
  });

  it('chooses a package on click', () => {
    const { onChoose } = mount();
    fireEvent.focus(input());
    fireEvent.mouseDown(options()[0]!);
    expect(onChoose).toHaveBeenCalledWith('laravel/framework', 'composer');
  });

  it('closes the list once something is chosen', () => {
    mount();
    fireEvent.focus(input());
    fireEvent.mouseDown(options()[0]!);
    expect(options()).toHaveLength(0);
  });

  it('walks the list with the arrow keys and takes it with Enter', () => {
    const { onChoose } = mount();
    fireEvent.focus(input());
    fireEvent.keyDown(input(), { key: 'ArrowDown' });
    fireEvent.keyDown(input(), { key: 'ArrowDown' });
    expect(options()[1]!.getAttribute('aria-selected')).toBe('true');
    fireEvent.keyDown(input(), { key: 'Enter' });
    // Keyboard and mouse must select the same thing, ecosystem
    // included — a split there is invisible until it bites.
    expect(onChoose).toHaveBeenCalledWith(
      'laravel/serializable-closure', 'composer',
    );
  });

  it('wraps from the last row to the first', () => {
    mount();
    fireEvent.focus(input());
    fireEvent.keyDown(input(), { key: 'ArrowUp' });
    expect(options()[3]!.getAttribute('aria-selected')).toBe('true');
  });

  it('leaves plain Enter meaning "search for what I typed"', () => {
    /**
     * With no row highlighted, Enter must not silently redirect the
     * query to whatever happens to be first — that is a keypress
     * changing the question.
     */
    const { onChoose } = mount();
    fireEvent.focus(input());
    fireEvent.keyDown(input(), { key: 'Enter' });
    expect(onChoose).not.toHaveBeenCalled();
  });

  it('dismisses the list with Escape', () => {
    mount();
    fireEvent.focus(input());
    fireEvent.keyDown(input(), { key: 'Escape' });
    expect(options()).toHaveLength(0);
  });

  it('offers the name that was typed, marked as the exact one', () => {
    /**
     * This asserted the opposite, on the reasoning that the exact-name
     * query had already answered it. That reads the list as a spelling
     * corrector, and the list is also how a reader learns what exists:
     * typing `mail` offered `mailparser`, `mailcomposer` and
     * `mailgun-js` while the table below listed `mail`'s 174
     * dependants, so the page implied the package did not exist and
     * hid the highest-count row in the list.
     */
    mount({ value: 'laravel/framework' });
    fireEvent.focus(input());
    expect(options()).toHaveLength(4);
    expect(options()[0]!.textContent).toContain('laravel/framework');
    expect(options()[0]!.textContent).toContain('exact');
  });

  it('opens even when the typed name is the only candidate', () => {
    // Previously it stayed shut, for the same reason. One row that
    // says "this exists, in this ecosystem, with this many dependants"
    // is worth showing.
    mount({
      value: 'laravel/framework',
      candidates: [{
        name: 'laravel/framework', ecosystem: 'composer',
        repositoryCount: 98, nameTotal: 98,
      }],
    });
    fireEvent.focus(input());
    expect(options()).toHaveLength(1);
  });

  it('drops the exact tag when every row is the typed name', () => {
    // `mail` in three ecosystems and no near-misses: the tag would be
    // on every row, distinguishing nothing, three times over.
    mount({
      value: 'mail',
      candidates: [
        { name: 'mail', ecosystem: 'gem', repositoryCount: 167, nameTotal: 174 },
        { name: 'mail', ecosystem: 'pypi', repositoryCount: 1, nameTotal: 174 },
      ],
    });
    fireEvent.focus(input());
    expect(options()).toHaveLength(2);
    for (const option of options()) {
      expect(option.textContent).not.toContain('exact');
    }
  });

  it('keeps the exact tag when near-misses share the list', () => {
    mount({ value: 'laravel/framework' });
    fireEvent.focus(input());
    expect(options()[0]!.textContent).toContain('exact');
    expect(options()[1]!.textContent).not.toContain('exact');
  });

  it('offers a row per ecosystem, each with its own count', () => {
    /**
     * `mail` is three packages sharing a name: a Ruby gem with 167
     * dependants, a Maven artifact with 6, a PyPI package with 1.
     * One row for the name would make the reader pick the name and
     * then reach for a separate filter to say which they meant.
     */
    const ecosystems = [
      { name: 'mail', ecosystem: 'gem', repositoryCount: 167, nameTotal: 174 },
      { name: 'mail', ecosystem: 'maven', repositoryCount: 6, nameTotal: 174 },
      { name: 'mail', ecosystem: 'pypi', repositoryCount: 1, nameTotal: 174 },
    ];
    mount({ value: 'mail', candidates: ecosystems });
    fireEvent.focus(input());
    expect(options()).toHaveLength(3);
    const text = options().map((o) => o.textContent ?? '');
    expect(text[0]).toContain('gem');
    expect(text[0]).toContain('167');
    expect(text[1]).toContain('maven');
    expect(text[2]).toContain('pypi');
  });

  it('passes the ecosystem to onChoose, not just the name', () => {
    // Otherwise picking `mail · gem` selects `mail` across all three
    // and the click has said nothing.
    const onChoose = vi.fn();
    mount({
      value: 'mail',
      onChoose,
      candidates: [
        { name: 'mail', ecosystem: 'gem', repositoryCount: 167, nameTotal: 174 },
        { name: 'mail', ecosystem: 'pypi', repositoryCount: 1, nameTotal: 174 },
      ],
    });
    fireEvent.focus(input());
    fireEvent.mouseDown(options()[1]!);
    expect(onChoose).toHaveBeenCalledWith('mail', 'pypi');
  });

  it('drops a stale highlight when the list changes under it', () => {
    /**
     * Otherwise the highlight index outlives the rows it pointed at and
     * Enter lands on a package the reader never looked at.
     */
    const { rerender } = mount();
    fireEvent.focus(input());
    fireEvent.keyDown(input(), { key: 'ArrowDown' });
    expect(options()[0]!.getAttribute('aria-selected')).toBe('true');

    rerender(
      <PackageSearch
        value="laravel/t"
        onChange={vi.fn()}
        onChoose={vi.fn()}
        candidates={[{
          name: 'laravel/tinker', ecosystem: 'composer',
          repositoryCount: 70, nameTotal: 70,
        }]}
        deadEnd={false}
      />,
    );
    expect(
      options().filter((o) => o.getAttribute('aria-selected') === 'true'),
    ).toHaveLength(0);
  });

  it('reports the highlighted row to a screen reader', () => {
    mount();
    fireEvent.focus(input());
    fireEvent.keyDown(input(), { key: 'ArrowDown' });
    expect(input().getAttribute('aria-activedescendant')).toBe(
      options()[0]!.id,
    );
  });

  it('keeps the filters in the same row as the input', () => {
    // They were siblings of the input before; the wrapper must not have
    // moved them onto a line of their own.
    mount({ children: <label className="field">Declared only</label> });
    const row = input().closest('.controls')!;
    expect(row.querySelector('.field')).toBeTruthy();
  });
});
