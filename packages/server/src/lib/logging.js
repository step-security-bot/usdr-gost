const { createLogger } = require('bunyan');

const logLevel = (process.env.LOG_LEVEL || 'info').toLowerCase();

module.exports = (name, opts) => createLogger({
    name,
    level: logLevel,
    src: logLevel === 'debug',
    ...opts,
});
