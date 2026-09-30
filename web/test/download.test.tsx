/**
 * The page links to the dataset itself (#154).
 *
 * The service serves the weekly Parquet export beside the page: its
 * manifest at `/export/manifest.json`, and each file the manifest names
 * at `/export/<file>`, which DuckDB reads over HTTP a range at a time.
 * The footer says so, in the reader's language: a link to the manifest,
 * and the query that reads one of its files where it is.
 */
// @vitest-environment jsdom
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { App, Download, EXPORT_MANIFEST } from '../src/app';
import { DICTIONARIES } from '../src/i18n/strings';
import { stubQueries } from './answers';

const EN = DICTIONARIES.en;
const ZH = DICTIONARIES.zh;

beforeEach(() => {
  localStorage.clear();
  window.history.replaceState(null, '', '#/overview');
  vi.stubGlobal('matchMedia', (query: string) => ({
    matches: false,
    media: query,
    addEventListener: () => {},
    removeEventListener: () => {},
  }));
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('the link to the dataset', () => {
  it('is the export’s manifest, where the service serves it', () => {
    expect(EXPORT_MANIFEST).toBe('/export/manifest.json');
    render(<Download words={EN} origin="https://chatsbom.example" />);
    const link = screen.getByRole('link', { name: 'Download the dataset' });
    expect(link.getAttribute('href')).toBe('/export/manifest.json');
  });

  it('shows the query that reads one of its files over HTTP', () => {
    render(<Download words={EN} origin="https://chatsbom.example" />);
    const query = document.querySelector('code');
    expect(query?.textContent).toBe(
      "SELECT * FROM 'https://chatsbom.example/export/<file>'",
    );
    const note = query?.closest('p')?.textContent ?? '';
    expect(note).toContain('DuckDB');
    expect(note).toContain('Parquet');
  });

  it('says both in Chinese, the query’s words left as SQL has them', () => {
    render(<Download words={ZH} origin="https://chatsbom.example" />);
    const link = screen.getByRole('link', { name: '下载数据集' });
    expect(link.getAttribute('href')).toBe('/export/manifest.json');
    expect(document.querySelector('code')?.textContent).toBe(
      "SELECT * FROM 'https://chatsbom.example/export/<文件>'",
    );
    expect(link.closest('p')?.textContent).not.toMatch(/Download|reads/);
  });

  it('is in the page’s footer, at the page’s own address, in either language', async () => {
    stubQueries();
    render(<App />);
    const link = await screen.findByRole('link', { name: 'Download the dataset' });
    expect(link.closest('footer')).not.toBeNull();
    expect(link.getAttribute('href')).toBe(EXPORT_MANIFEST);
    expect(document.querySelector('footer code')?.textContent).toBe(
      `SELECT * FROM '${window.location.origin}/export/<file>'`,
    );

    fireEvent.click(screen.getByRole('button', { name: '中文' }));
    await waitFor(() =>
      expect(screen.getByRole('link', { name: '下载数据集' }).closest('footer')).not.toBeNull(),
    );
  });
});
