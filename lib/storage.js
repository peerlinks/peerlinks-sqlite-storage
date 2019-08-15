import sqlite from 'sqlite3';
import { promises as fs } from 'fs';
import * as path from 'path';
import * as os from 'os';
import { promisify } from 'util';

export default class SqliteStorage {
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

    if (this.options.trace) {
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
          channel_id,
          height ASC,
          hash ASC
        );
      `);

      this.db.run(`
        CREATE TABLE IF NOT EXISTS parents(
          channel_id BLOB,
          hash BLOB,
          PRIMARY KEY(hash ASC)
        );
      `);
      this.db.run(`
        CREATE INDEX IF NOT EXISTS channel_id ON parents(channel_id);
      `);
      this.db.run(`
        CREATE UNIQUE INDEX IF NOT EXISTS hash ON parents(hash);
      `);

      this.db.run(`
        CREATE TABLE IF NOT EXISTS entities(
          prefix TEXT,
          id TEXT,
          blob BLOB
        );
      `);

      promise = this.db.runAsync(`
        CREATE UNIQUE INDEX IF NOT EXISTS entity_id ON entities(prefix, id);
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
        REPLACE INTO messages (channel_id, hash, height, blob)
        VALUES (?, ?, ?, ?);
      `, [
        message.channelId,
        message.hash,
        message.height,
        message.serializeData(),
      ]);

      for (const parentHash of message.parents) {
        this.db.run(`
          REPLACE INTO parents (channel_id, hash)
          VALUES (?, ?)
        `, message.channelId, parentHash);
      }

      promise = this.db.runAsync('COMMIT TRANSACTION;');
    });
    return await promise;
  }

  async getMessageCount(channelId) {
    const row = await this.db.getAsync(`
      SELECT COUNT(*) AS count FROM messages WHERE channel_id = ?
    `, channelId);
    return row.count;
  }

  async getLeaves(channelId) {
    const rows = await this.db.allAsync(`
      SELECT blob FROM messages
      WHERE messages.channel_id = ? AND
        messages.hash NOT IN (SELECT hash FROM parents WHERE channel_id = ?)
    `, channelId, channelId);

    return rows.map((row) => row.blob);
  }

  async hasMessage(channelId, hash) {
    const row = await this.db.getAsync(`
      SELECT COUNT(*) AS count FROM messages
      WHERE channel_id = ? AND hash = ?
    `, channelId, hash);
    return row.count !== 0;
  }

  async getMessage(channelId, hash) {
    const row = await this.db.getAsync(`
      SELECT blob FROM messages
      WHERE channel_id = ? AND hash = ?
    `, channelId, hash);
    return row ? row.blob : undefined;
  }

  async getMessages(channelId, hashes) {
    const rows = await this.db.allAsync(`
      SELECT blob FROM messages
      WHERE channel_id = ? AND hash IN (${hashes.map(() => '?').join(', ')})
    `, channelId, ...hashes);
    return rows.map((row) => row.blob);
  }

  async getMessagesAtOffset(channelId, offset, limit) {
    const rows = await this.db.allAsync(`
      SELECT blob FROM messages
      WHERE channel_id = ?
      ORDER BY height ASC, hash ASC
      LIMIT ? OFFSET ?
    `, channelId, limit, offset);
    return rows.map((row) => row.blob);
  }

  async query(channelId, cursor, isBackward, limit) {
    limit = Math.max(0, limit);

    let rows;
    if (cursor.hash) {
      if (isBackward) {
        rows = await this.db.allAsync(`
          SELECT hash, blob FROM messages
          WHERE channel_id = ? AND hash < ?
          ORDER BY height DESC, hash DESC
          LIMIT ?
        `, channelId, cursor.hash, limit + 1);
        rows.reverse();
      } else {
        rows = await this.db.allAsync(`
          SELECT hash, blob FROM messages
          WHERE channel_id = ? AND hash >= ?
          ORDER BY height ASC, hash ASC
          LIMIT ?
        `, channelId, cursor.hash, limit + 1);
      }
    } else {
      if (isBackward) {
        throw new Error('Backwards query by height is not supported');
      } else {
        rows = await this.db.allAsync(`
          SELECT hash, blob FROM messages
          WHERE channel_id = ? AND height >= ?
          ORDER BY height ASC, hash ASC
          LIMIT ?
        `, channelId, cursor.height, limit + 1);
      }
    }

    let messages = rows.map((row) => row.blob);
    let backwardHash = null;
    let forwardHash = null;

    if (isBackward) {
      forwardHash = cursor.hash;
      if (rows.length > limit) {
        messages = messages.slice(1);
        backwardHash = rows[0].hash;
      }
    } else {
      backwardHash = rows.length > 0 ? rows[0].hash : null;
      if (rows.length > limit) {
        messages = messages.slice(0, -1);
        forwardHash = rows[rows.length - 1].hash;
      }
    }

    return {
      messages,
      backwardHash,
      forwardHash,
    };
  }

  //
  // Entities (Identity, ChannelList, so on)
  //

  async storeEntity(prefix, id, entity) {
    await this.db.runAsync(`
      INSERT OR REPLACE INTO entities (prefix, id, blob)
      VALUES (?, ?, ?);
    `, [
      prefix,
      id,
      entity.serializeData(),
    ]);
  }

  async retrieveEntity(prefix, id) {
    const row = await this.db.getAsync(`
      SELECT blob FROM entities
      WHERE prefix == ? AND id == ?
    `, prefix, id);

    return row ? row.blob : undefined;
  }

  async removeEntity(prefix, id) {
    await this.db.runAsync(`
      DELETE FROM entities
      WHERE prefix == ? AND id == ?
    `, prefix, id);
  }

  async getEntityKeys(prefix) {
    const rows = await this.db.allAsync(`
      SELECT id FROM entities
      WHERE prefix == ?
    `, prefix);

    return rows.map((row) => row.id);
  }

  //
  // Miscellaneous
  //

  async clear() {
    await Promise.all([
      this.db.runAsync('DELETE FROM messages'),
      this.db.runAsync('DELETE FROM parents'),
      this.db.runAsync('DELETE FROM entities'),
    ]);
  }
}
