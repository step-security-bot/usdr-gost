const knex = require('./connection');

function whereAgencyCriteriaMatch(qb, criteria) {
    if (!criteria) {
        return;
    }
    if (criteria.eligibilityCodes) {
        qb.where('eligibility_codes', '~', criteria.eligibilityCodes.join('|'));
    }
    /*
        Ensures that if either description/grant_id/grant_number/title
        matches any of the include keywords we will include the grant.
    */
    const includeKeywords = criteria.includeKeywords || [];
    const excludeKeywords = criteria.excludeKeywords || [];
    if (includeKeywords.length > 0 || excludeKeywords.length > 0) {
        const tsqIncludes = includeKeywords.join(' | ');
        const tsqExcludes = excludeKeywords.map(kw => `!${kw}`).join(' & ');

        let tsqExpression;
        if (includeKeywords.length > 0 && excludeKeywords.length > 0) {
            tsqExpression = `(${tsqIncludes}) & (${tsqExcludes})`;
        } else if (includeKeywords.length > 0) {
            tsqExpression = tsqIncludes;
        } else {
            tsqExpression = tsqExcludes;
        }
        qb.joinRaw(`to_tsquery(?) as tsq`, tsqExpression);
        qb.where((q) => q.where('tsq', '@@', 'title_ts').orWhere('tsq', '@@', 'description_ts'));

        if (includeKeywords.length > 0) {
            qb.where((q) => q
                .where('grant_number', '~*', includeKeywords.join('|'))
                .orWhere('grant_id', '~*', includeKeywords.join('|')));
        }
        if (excludeKeywords.length > 0) {
            qb.where((q) => q
                .where('grant_id', '!~*', criteria.excludeKeywords.join('|'))
                .andWhere('grant_number', '!~*', criteria.excludeKeywords.join('|')));
        }
    }
}

async function hasOutstandingMigrations() {
    const [, newMigrations] = await knex.migrate.list();
    return newMigrations.length > 0;
}

module.exports = {
    whereAgencyCriteriaMatch,
    hasOutstandingMigrations,
};
