/**
 * What the page shows when drawing it fails.
 *
 * Nothing caught a render error, so one left the page blank — and when
 * the cause was in the address, as `#/query/%` was, blank on every
 * reload too, since the hash is kept (#42). That cause is fixed where
 * it was (`parseRoute`); this is for the next one. The page says it
 * could not be drawn, and offers the one way back that does not depend
 * on the address that failed: the overview.
 *
 * It sits outside the page, above the language switch it cannot count
 * on having rendered. It spoke English for that reason, to every reader
 * (#43); it asks for the language the way the switch starts instead —
 * the choice the reader stored, or the browser's.
 */
import { Component, type MouseEvent, type ReactNode } from 'react';

import { preferredLocale } from '../i18n/locale';
import { DICTIONARIES } from '../i18n/strings';

interface State {
  failed: boolean;
}

export class ErrorBoundary extends Component<{ children: ReactNode }, State> {
  override state: State = { failed: false };

  static getDerivedStateFromError(): State {
    return { failed: true };
  }

  /** Go to the overview in place of the address that failed, and draw again. */
  private readonly recover = (event: MouseEvent) => {
    event.preventDefault();
    // Replaced rather than pushed, so Back does not lead to it again.
    window.location.replace('#/overview');
    this.setState({ failed: false });
  };

  override render(): ReactNode {
    if (!this.state.failed) return this.props.children;
    const words = DICTIONARIES[preferredLocale()];
    return (
      <div className="shell">
        <p className="answer error" role="alert">
          {words.boundaryFailed}{' '}
          <a href="#/overview" onClick={this.recover}>
            {words.boundaryBack}
          </a>
        </p>
      </div>
    );
  }
}
