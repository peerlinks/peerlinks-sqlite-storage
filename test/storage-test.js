/* eslint-env node, mocha */
import * as assert from 'assert';
import { Buffer } from 'buffer';
import { randomBytes } from 'crypto';

import Storage from '../';

describe('NodeStorage', () => {
  let channelId;

  beforeEach(() => {
    channelId = randomBytes(32);
  });

  afterEach(() => {
    channelId = null;
  });

  const genHash = () => {
    return randomBytes(32);
  };

  const msg = (height, hash, parents, content = 'empty') => {
    return {
      channelId,
      hash,
      height,
      parents,
      serializeData() {
        return Buffer.from(`${height}: ${content}`);
      },
    };
  };

  it('should persist messages', async () => {
    const s = new Storage();

    await s.open();

    assert.strictEqual(await s.getMessageCount(channelId), 0);
    let leaves = await s.getLeaves(channelId);
    assert.deepStrictEqual(leaves, []);

    // Insert root
    const rootHash = genHash();
    assert.ok(!(await s.hasMessage(channelId, rootHash)));

    await s.addMessage(msg(0, rootHash, [], 'root'));
    assert.strictEqual(await s.getMessageCount(channelId), 1);

    leaves = await s.getLeaves(channelId);
    assert.strictEqual(leaves.length, 1);
    assert.strictEqual(leaves[0].toString('hex'), rootHash.toString('hex'));

    // Insert child
    const childHash = genHash();
    assert.ok(!(await s.hasMessage(channelId, childHash)));

    await s.addMessage(msg(1, childHash, [ rootHash ], 'child'));
    assert.strictEqual(await s.getMessageCount(channelId), 2);

    leaves = await s.getLeaves(channelId);
    assert.strictEqual(leaves.length, 1);
    assert.strictEqual(leaves[0].toString('hex'), childHash.toString('hex'));

    // Load both messages
    assert.ok(await s.hasMessage(channelId, rootHash));
    const root = await s.getMessage(channelId, rootHash);
    assert.strictEqual(root.toString(), '0: root');

    assert.ok(await s.hasMessage(channelId, childHash));
    const child = await s.getMessage(channelId, childHash);
    assert.strictEqual(child.toString(), '1: child');

    // Load all messages
    const all = await s.getMessages(channelId, [ rootHash, childHash ]);
    all.sort((a, b) => Buffer.compare(a, b));
    assert.strictEqual(all.length, 2);
    assert.strictEqual(all[0].toString(), '0: root');
    assert.strictEqual(all[1].toString(), '1: child');

    // Check offsets
    assert.strictEqual((await s.getMessageAtOffset(channelId, 0)).toString(),
      '0: root');
    assert.strictEqual((await s.getMessageAtOffset(channelId, 1)).toString(),
      '1: child');
    assert.strictEqual(await s.getMessageAtOffset(channelId, 2), undefined);

    // Clear database
    await s.clear();
    assert.strictEqual(await s.getMessageCount(channelId), 0);
    assert.deepStrictEqual(await s.getLeaves(channelId), []);

    await s.close();
  });
});
