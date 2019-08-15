/* eslint-env node, mocha */
import * as assert from 'assert';
import { Buffer } from 'buffer';

import Storage from '../';

describe('encoding', () => {
  let storage = null;

  beforeEach(async () => {
    storage = new Storage();
    await storage.open();
  });

  afterEach(async () => {
    const s = storage;
    storage = null;

    await s.close();
  });

  it('should encode/decode hash list', () => {
    const encoded = storage.encodeHashList([
      Buffer.from('hello'),
      Buffer.from('world'),
      Buffer.from('what\'s'),
      Buffer.from('up'),
    ]);
    const decoded = storage.decodeHashList(encoded).map((val) => {
      return val.toString();
    });
    assert.deepStrictEqual(decoded, [
      'hello',
      'world',
      'what\'s',
      'up',
    ]);
  });
});
