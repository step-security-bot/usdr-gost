#!/usr/bin/env node
const tracer = require('dd-trace').init(); // eslint-disable-line no-unused-vars
const bunyan = require('bunyan');
const { getSQSClient } = require('../lib/gost-aws');
const knex = require('../db/connection');
const { processMessages, receiveNextMessageBatch } = require('../lib/grants-ingest');

const logLevel = (process.env.LOG_LEVEL && process.env.LOG_LEVEL.toLowerCase()) || 'info';
const log = bunyan.createLogger({
    name: 'consume-grant-modifications',
    level: logLevel,
    src: logLevel === 'debug',
    queue_url: process.env.GRANTS_INGEST_EVENTS_QUEUE_URL,
});

async function main() {
    let shutDownRequested = false;
    const requestShutdown = (signal) => {
        log.info({ signal }, 'Requesting shutdown...');
        shutDownRequested = true;
    };
    process.on('SIGTERM', requestShutdown);
    process.on('SIGINT', requestShutdown);

    const queueUrl = process.env.GRANTS_INGEST_EVENTS_QUEUE_URL;
    const sqs = getSQSClient();
    while (shutDownRequested === false) {
        log.info('Long-polling next SQS message batch from queue');
        // eslint-disable-next-line no-await-in-loop
        const messages = await receiveNextMessageBatch(sqs, queueUrl);
        if (messages.length > 0) {
            // eslint-disable-next-line no-await-in-loop
            await processMessages(knex, sqs, queueUrl, messages);
        }
    }
    log.info('Shutting down');
}

if (require.main === module) {
    main().then(() => process.exit());
}
