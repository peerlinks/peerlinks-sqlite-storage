import sqlite from 'sqlite3';
import { promises as fs } from 'fs';
import * as path from 'path';
import * as os from 'os';
import { promisify } from 'util';

export default class Memory {
  /**
   * In-memory persistence.
   *
   * @class
   */
  constructor(options = {}) {
    this.db = null;
    this.options = options;
  }

  async open() {
    let file;
    if (this.options.file) {
      file = this.options.file;
    } else {
      const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vowlink-'));
      file = path.join(tmpDir, 'tmp.db');
    }

    this.db = await new Promise((resolve, reject) => {
      const db = new sqlite.Database(file, (err) => {
        if (err) {
          return reject(err);
        }
        resolve(db);
      });
    });

    if (this.options.debug) {
      this.db.on('trace', (query) => {
        console.error(query);
      });
    }

    const methods = [
      'close',
      'get',
      'run',
      'all',
    ];

    // NOTE: Lame, but works
    for (const method of methods) {
      this.db[method + 'Async'] = promisify(this.db[method]);
    }

    await this.createTables();
  }

  async close() {
    await this.db.close();
  }

  async createTables() {
    let promise;
    this.db.serialize(() => {
      this.db.run(`
        CREATE TABLE IF NOT EXISTS messages(
          channel_id BLOB,
          hash BLOB,
          height INT,
          blob BLOB,
          PRIMARY KEY(hash ASC)
        );
      `);
      this.db.run(`
        CREATE INDEX IF NOT EXISTS crdt ON messages(
          channel_id ASC,
          height ASC,
          hash ASC
        );
      `);

      this.db.run(`
        CREATE TABLE IF NOT EXISTS leaves(
          channel_id BLOB,
          hash BLOB,
          PRIMARY KEY(hash ASC)
        );
      `);
      promise = this.db.runAsync(`
        CREATE INDEX IF NOT EXISTS channel_id ON leaves(channel_id);
      `);
    });
    return await promise;
  }

  //
  // Messages
  //

  async addMessage(message) {
    let promise;
    this.db.serialize(() => {
      this.db.run('BEGIN TRANSACTION;');

      this.db.run(`
        INSERT INTO messages (channel_id, hash, height, blob)
        VALUES (?, ?, ?, ?);
      `, [
        message.channelId,
        message.hash,
        message.height,
        message.serializeData(),
      ]);

      for (const parentHash of message.parents) {
        this.db.run(`
          DELETE FROM leaves WHERE channel_id == ? AND hash == ?;
        `, message.channelId, parentHash);
      }

      this.db.run(`
        INSERT INTO leaves (channel_id, hash)
        VALUES (?, ?);
      `, message.channelId, message.hash);

      promise = this.db.runAsync('COMMIT TRANSACTION;');
    });
    return await promise;
  }

  async getMessageCount(channelId) {
    const result = await this.db.getAsync(`
      SELECT COUNT(*) AS count FROM messages WHERE channel_id = ?
    `, channelId);
    return result.count;
  }

  async getLeaves(channelId) {
    const result = await this.db.allAsync(`
      SELECT hash FROM leaves WHERE channel_id = ?
    `, channelId);

    return result.map((row) => row.hash);
  }

  async hasMessage(channelId, hash) {
    const result = await this.db.getAsync(`
      SELECT COUNT(*) AS count FROM messages
      WHERE channel_id = ? AND hash = ?
    `, channelId, hash);
    return result.count !== 0;
  }

  async getMessage(channelId, hash) {
    const result = await this.db.getAsync(`
      SELECT blob FROM messages
      WHERE channel_id = ? AND hash = ?
    `, channelId, hash);
    return result ? result.blob : undefined;
  }

  async getMessages(channelId, hashes) {
    const result = await this.db.allAsync(`
      SELECT blob FROM messages
      WHERE channel_id = ? AND hash IN (${hashes.map(() => '?').join(', ')})
    `, channelId, ...hashes);
    return result.map((row) => row.blob);
  }

  async getMessageAtOffset(channelId, offset) {
    const result = await this.db.getAsync(`
      SELECT blob FROM messages
      WHERE channel_id = ?
      ORDER BY height ASC, hash ASC
      LIMIT 1 OFFSET ?
    `, channelId, offset);
    return result ? result.blob : undefined;
  }

  async query(channelId, cursor, isBackward, limit) {
  }

  //
  // Entities (Identity, ChannelList, so on)
  //

  async storeEntity(prefix, id, entity) {
  }

  async retrieveEntity(prefix, id, constructor, options) {
  }

  async removeEntity(prefix, id) {
  }

  async getEntityKeys(prefix) {
  }

  //
  // Miscellaneous
  //

  async clear() {
    await Promise.all([
      this.db.runAsync('DELETE FROM messages'),
      this.db.runAsync('DELETE FROM leaves'),
    ]);
  }
}
