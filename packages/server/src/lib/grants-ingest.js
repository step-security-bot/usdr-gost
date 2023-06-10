const { ReceiveMessageCommand, DeleteMessageCommand } = require('@aws-sdk/client-sqs');
const moment = require('moment');
const log = require('./logging')('grants-ingest');

const opportunityCategoryMap = {
    C: 'Continuation',
    D: 'Discretionary',
    E: 'Earmark',
    M: 'Mandatory',
    O: 'Other',
};

function normalizeDateString(dateString, formats = ['YYYY-MM-DD', 'MMDDYYYY']) {
    const target = 'YYYY-MM-DD';
    const thisLog = log.child({ target, formats, input: dateString });

    for (const fmt of formats) {
        const parsed = moment(dateString, fmt, true);
        if (parsed.isValid()) {
            const result = parsed.format(target);
            thisLog.info(
                { inputFormat: fmt },
                dateString !== result
                    ? 'Normalized date string'
                    : 'Input date string already in target format',
            );
            return result;
        }
        thisLog.warn({ attemptedFormat: fmt },
            'Failed to parse value input date string as date using format');
    }
    throw new Error(`Value ${dateString} could not be parsed from formats ${formats.join(', ')}`);
}

/**
 * receiveNextMessageBatch long-polls an SQS queue for up to 10 messages.
 * @param { import('@aws-sdk/client-sqs').SQSClient } sqs AWS SDK client used to issue commands to the SQS API.
 * @param { string} queueUrl The URL identifying the queue to poll.
 * @returns { Array[import('@aws-sdk/client-sqs').Message] } Received messages, if any.
 */
async function receiveNextMessageBatch(sqs, queueUrl) {
    const resp = await sqs.send(new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        WaitTimeSeconds: 20,
        MaxNumberOfMessages: 10,
    }));

    const messages = (resp && resp.Messages) ? resp.Messages : [];
    if (messages.length === 0) {
        log.info({ queueUrl }, 'Empty message batch received from SQS');
    }
    return messages;
}

function sqsMessageToGrant(jsonBody) {
    const messageData = JSON.parse(jsonBody);
    return {
        status: 'inbox',
        grant_id: messageData.OpportunityId || messageData.grant_id,
        grant_number: messageData.OpportunityNumber,
        agency_code: messageData.AgencyCode,
        award_ceiling: (messageData.AwardCeiling && parseInt(messageData.AwardCeiling, 10))
            ? parseInt(messageData.AwardCeiling, 10) : undefined,
        award_floor: (messageData.AwardFloor && parseInt(messageData.AwardFloor, 10))
            ? parseInt(messageData.AwardFloor, 10) : undefined,
        cost_sharing: messageData.CostSharingOrMatchingRequirement ? 'Yes' : 'No',
        title: messageData.OpportunityTitle,
        cfda_list: (messageData.CFDANumbers || []).join(', '),
        open_date: normalizeDateString(messageData.PostDate),
        close_date: normalizeDateString(messageData.CloseDate || '2100-01-01'),
        notes: 'auto-inserted by script',
        search_terms: '[in title/desc]+',
        reviewer_name: 'none',
        opportunity_category: opportunityCategoryMap[messageData.OpportunityCategory],
        description: messageData.Description,
        eligibility_codes: (messageData.EligibleApplicants || []).join(' '),
        opportunity_status: 'posted',
        raw_body: JSON.stringify(messageData),
    };
}

async function upsertGrant(knex, grant) {
    await knex('grants')
        .insert(grant)
        .onConflict('grant_id')
        .merge({ ...grant, ...{ updated_at: 'now' } })
        .returning('grant_id');
}

async function deleteMessage(sqs, queueUrl, receiptHandle) {
    const command = new DeleteMessageCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandle,
    });
    await sqs.send(command);
}

/**
 * processMessages Saves a batch of SQS messages containing JSON grant data sent
 * from the grants-ingest service to the database. Existing database records that match
 * a message's grant identifier are updated. After each message is processed, it is
 * deleted from the SQS queue.
 *
 * Any errors related to parsing or saving are logged and do not prevent further processing.
 * Errors interacting with SQS are fatal.
 *
 * @param { import('knex').Knex } knex Database client for persisting grants.
 * @param { import('@aws-sdk/client-sqs').SQSClient } sqs AWS SDK client used to issue commands to the SQS API.
 * @param { string } queueUrl The URL identifying the queue to poll.
 * @param { Array[import('@aws-sdk/client-sqs').Message] } messages Messages to process from SQS.
 */
async function processMessages(knex, sqs, queueUrl, messages) {
    let parseErrorCount = 0;
    let successCount = 0;
    let saveErrorCount = 0;

    return Promise.all(messages.map(async (message) => {
        const sqsLogFields = { receiptHandle: message.ReceiptHandle, messageId: message.MessageId };
        let thisLog = log.child({ sqsMessage: sqsLogFields });
        thisLog.info({ sqsMessage: { body: message.Body, ...sqsLogFields } }, 'Procesing message');

        let grant;
        try {
            grant = sqsMessageToGrant(message.Body);
        } catch (e) {
            parseErrorCount += 1;
            thisLog.error(e, 'Error parsing grant from SQS message');
            return;
        }
        thisLog = thisLog.child({ grant_id: grant.grant_id });

        try {
            await upsertGrant(knex, grant);
            successCount += 1;
        } catch (e) {
            saveErrorCount += 1;
            thisLog.error(e, 'Error on grant insert/update');
            return;
        }

        try {
            await deleteMessage(sqs, queueUrl, message.ReceiptHandle);
        } catch (e) {
            thisLog.error(e, 'Error deleting SQS message');
            throw e;
        }

        thisLog.info('Processing completed successfully');
    })).then(() => {
        const hasErrors = parseErrorCount + saveErrorCount > 0;
        const logData = {
            msg: 'Finished processing SQS messages',
            withErrors: hasErrors,
            totals: {
                success: successCount, parseErrors: parseErrorCount, saveErrors: saveErrorCount,
            },
        };
        hasErrors ? log.warn(logData) : log.info(logData);
    });
}

module.exports = {
    processMessages,
    receiveNextMessageBatch,
};
