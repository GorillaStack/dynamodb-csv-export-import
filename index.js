#!/usr/bin/env node

/* eslint-disable no-await-in-loop, import/no-extraneous-dependencies, no-cond-assign, no-param-reassign, max-classes-per-file */
const { createReadStream } = require('fs');
const parseCSV = require('csv-parse');
const DynamoDB = require('aws-sdk/clients/dynamodb');
const stream = require('stream');
const util = require('util');
const debug = require('debug')('dynamodb-csv-export-import');

const pipeline = util.promisify(stream.pipeline);

async function writeBatch(ddb, tableName, batch) {
  debug('writeBatch: writing to %s batch %O', tableName, batch);
  await ddb.batchWriteItem({
    RequestItems: {
      [tableName]: batch.map(record => ({
        PutRequest: {
          Item: record,
        },
      })),
    },
  }).promise();
}

const transformRecord = record => Object.entries(record)
  .reduce((output, [key, value]) => {
    const [, name, type] = /(\w+) \((\w+)\)/.exec(key);
    if (!value) {
      return output;
    }
    const contents = (['L', 'M', 'BOOL'].includes(type)) ? JSON.parse(value) : value;
    output[name] = {
      [type]: contents,
    };
    return output;
  }, {});

class DynamoDBCSVRecordTransformer extends stream.Transform {
  constructor(options = {}) {
    super({ ...options, objectMode: true });
  }

  _transform(chunk, encoding, callback) {
    try {
      const transformed = transformRecord(chunk);
      debug('transformRecord: transformed record %o', transformed);
      callback(null,transformed);
    } catch (error) {
      callback(error);
    }
  }
}

class DynamoDBWriter extends stream.Writable {
  constructor(options = {}) {
    super({ ...options, objectMode: true });
    this.tableName = options.tableName;
    this.ddb = options.ddb;
    this.batch = [];
  }

  async _write(chunk, encoding, callback) {
    try {
      this.batch.push(chunk);
      if (this.batch.length >= 25) {
        await writeBatch(this.ddb, this.tableName, this.batch);
        this.batch = [];
      }
      callback();
    } catch (error) {
      callback(error);
    }
  }

  async _final(callback) {
    try {
      if (this.batch.length > 0) {
        await writeBatch(this.ddb, this.tableName, this.batch);
      }
      callback();
    } catch (error) {
      callback(error);
    }
  }
}

(async function writePipeline() {
  const filename = process.argv[2];
  const tablename = process.argv[3];
  if (!filename || !tablename) {
    console.log(`Usage: ${process.argv[0]} ${process.argv[1]} <csv_file> <target_table>`);
    return;
  }

  const rs = createReadStream(filename);
  const parser = parseCSV({ delimiter: ',', columns: true });
  const ddb = new DynamoDB({ endpoint: process.env.DYNAMODB_ENDPOINT_URL, region: process.env.AWS_DEFAULT_REGION || 'us-east-1' });
  const transformer = new DynamoDBCSVRecordTransformer();
  const writer = new DynamoDBWriter({ tableName: tablename, ddb });
  try {
    await pipeline(rs, parser, transformer, writer);
    console.log('Import completed');
  } catch (error) {
    console.error(`Fatal error running CSV transform pipeline: `, error);
  }
}());
