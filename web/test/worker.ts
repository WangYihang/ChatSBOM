/**
 * What a `?worker` import is in the tests (`vitest.config.ts`): a Web
 * Worker that cannot be started.
 *
 * The build makes a worker's script a file of its own, which the page
 * starts (`src/ask/altcha.ts`). jsdom starts no worker, and the script,
 * imported as a module, would run in the test's own window instead. A
 * test that needs a worker gives the widget one of its own
 * (`altcha.ts`).
 */
export default class Unstarted {
  constructor() {
    throw new Error(
      'No Web Worker runs in the tests: give the widget one of its own (test/altcha.ts).',
    );
  }
}
