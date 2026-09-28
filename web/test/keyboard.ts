/**
 * The Tab key, as far as jsdom lets a test have one (#43).
 *
 * jsdom moves focus when a test says so, with the events that go with
 * it, but has no Tab of its own: pressing it moves nothing. `tab` does
 * what a browser does on a page with no positive `tabindex`, which this
 * one has none of — focus goes to the next element in document order
 * that can take it and is not hidden — so a test can ask whether Tab
 * reaches something, not only whether a script could focus it.
 */
import { act } from '@testing-library/react';

/** What can take focus: links, form controls, and anything with a `tabindex`. */
const FOCUSABLE = 'a[href], button, input, select, textarea, [tabindex]';

/** Every element Tab stops at, in the order it stops at them. */
export function tabOrder(): Element[] {
  return Array.from(document.body.querySelectorAll<HTMLElement | SVGElement>(FOCUSABLE)).filter(
    (element) =>
      element.tabIndex >= 0 &&
      !(element as HTMLButtonElement).disabled &&
      !(element instanceof HTMLInputElement && element.type === 'hidden') &&
      !element.closest('[hidden], [inert]'),
  );
}

/**
 * Give `element` focus, as a click or a script would.
 *
 * Inside `act`, since what focus sets off — a list opening, a tooltip
 * showing — is a React update, and the test reads the page after it.
 */
export function focus(element: Element): void {
  act(() => (element as HTMLElement | SVGElement).focus());
}

/** Press Tab: focus the next stop after the focused element, and return it. */
export function tab(): Element {
  const order = tabOrder();
  const next = order[order.indexOf(document.activeElement!) + 1] ?? order[0];
  if (!next) throw new Error('nothing on the page takes focus');
  focus(next);
  return next;
}
