// tests/frontend/format_error_test.mjs
// Unit tests for the pure error-formatting logic used by safeRun.
// Mirrors app.js formatSectionError — zero deps, runs with `node --test`.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

// Pure function extracted from app.js for testability.
// The production code in app.js has the identical implementation.
function formatSectionError(err, label) {
  return `${label}: ${err?.message || 'unknown error'} (see browser console)`;
}

describe('formatSectionError', () => {
  it('formats a standard Error', () => {
    const err = new Error('fetch failed');
    assert.equal(formatSectionError(err, 'Partitions'), 'Partitions: fetch failed (see browser console)');
  });

  it('formats a TypeError', () => {
    const err = new TypeError("Cannot read properties of undefined (reading 'ok')");
    assert.equal(formatSectionError(err, 'Topics'), 'Topics: Cannot read properties of undefined (reading \'ok\') (see browser console)');
  });

  it('formats an error with no message (falsy message)', () => {
    const err = new Error('');
    assert.equal(formatSectionError(err, 'Cluster'), 'Cluster: unknown error (see browser console)');
  });

  it('formats null gracefully', () => {
    assert.equal(formatSectionError(null, 'Metrics'), 'Metrics: unknown error (see browser console)');
  });

  it('formats undefined gracefully', () => {
    assert.equal(formatSectionError(undefined, 'Audit'), 'Audit: unknown error (see browser console)');
  });

  it('handles an object with message property', () => {
    const err = { message: 'custom failure' };
    assert.equal(formatSectionError(err, 'DLQ'), 'DLQ: custom failure (see browser console)');
  });

  it('handles an object without message property', () => {
    const err = { code: 42 };
    assert.equal(formatSectionError(err, 'Settings'), 'Settings: unknown error (see browser console)');
  });

  it('preserves the label exactly', () => {
    const err = new Error('oops');
    assert.equal(formatSectionError(err, 'Consumer Groups'), 'Consumer Groups: oops (see browser console)');
  });
});
