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
  { name: 'laravel/framework', repositoryCount: 98 },
  { name: 'laravel/serializable-closure', repositoryCount: 77 },
  { name: 'laravel/tinker', repositoryCount: 70 },
  { name: 'laravel-enso/core', repositoryCount: 1 },
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
    expect(options().map((o) => o.textContent)).toEqual([
      'laravel/framework98',
      'laravel/serializable-closure77',
      'laravel/tinker70',
      'laravel-enso/core1',
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
    expect(onChoose).toHaveBeenCalledWith('laravel/framework');
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
    expect(onChoose).toHaveBeenCalledWith('laravel/serializable-closure');
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

  it('does not offer back the name that was typed', () => {
    // The exact-name query has already answered it; offering it is the
    // list saying "did you mean what you said".
    mount({ value: 'laravel/framework' });
    fireEvent.focus(input());
    expect(options().map((o) => o.textContent)).not.toContain(
      'laravel/framework98',
    );
    expect(options()).toHaveLength(3);
  });

  it('stays shut when every candidate is the typed name', () => {
    mount({
      value: 'laravel/framework',
      candidates: [{ name: 'laravel/framework', repositoryCount: 98 }],
    });
    fireEvent.focus(input());
    expect(options()).toHaveLength(0);
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
        candidates={[{ name: 'laravel/tinker', repositoryCount: 70 }]}
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
