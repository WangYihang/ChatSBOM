/**
 * What `npm run lint` checks (#44).
 *
 * JavaScript's and TypeScript's recommended rules, the ones that need no
 * type information, and React's two rules of hooks: a hook is called
 * unconditionally, and an effect names every value it reads among its
 * dependencies. Nothing about layout: there is no formatter.
 *
 * The rules of hooks are the plugin's two, not its whole `recommended`
 * set, which also carries the React Compiler's checks. The compiler is
 * not used here, and those checks ask for rewrites of their own.
 */
import js from '@eslint/js';
import { defineConfig } from 'eslint/config';
import reactHooks from 'eslint-plugin-react-hooks';
import globals from 'globals';
import tseslint from 'typescript-eslint';

export default defineConfig(
  { ignores: ['dist/', '.wrangler/'] },
  {
    // A disable that no longer disables anything is an error, so none
    // outlives the code it excused.
    linterOptions: { reportUnusedDisableDirectives: 'error' },
  },
  js.configs.recommended,
  tseslint.configs.recommended,
  {
    plugins: { 'react-hooks': reactHooks },
    rules: {
      'react-hooks/rules-of-hooks': 'error',
      // An error, not the plugin's warning: a warning passes the lint.
      'react-hooks/exhaustive-deps': 'error',
    },
  },
  {
    files: ['**/*.{ts,tsx}'],
    rules: {
      // tsc reports these, under noUnusedLocals and noUnusedParameters
      // (tsconfig.base.json), and takes a leading underscore to mean a
      // parameter a signature needs and its body does not.
      '@typescript-eslint/no-unused-vars': 'off',
    },
  },
  {
    // The scripts and this file run in Node.
    files: ['**/*.{js,mjs}'],
    languageOptions: { globals: globals.node },
  },
);
