const fs = require('fs');
const csv = require('csv-parser');

const sourceFile = 'data/source.csv';
const targetFile = 'data/target.csv';
const mappingFile = 'config/mapping.json';
const resultFile = 'report/result.json';

function readCSV(filePath) {
    return new Promise((resolve) => {
        const results = [];
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => resolve(results));
    });
}

function validateData(sourceData, targetData, mapping, uniqueKey) {
    const result = {
        rowCountCheck: {},
        fieldMappingCheck: [],
        mismatchedRecords: [],
        status: "success"
    };

    // 1. Check row count
    if (sourceData.length !== targetData.length) {
        result.rowCountCheck = {
            status: "failed",
            sourceCount: sourceData.length,
            targetCount: targetData.length,
            message: "Row count mismatch"
        };
        result.status = "failed";
    } else {
        result.rowCountCheck = {
            status: "success",
            count: sourceData.length
        };
    }

    // Convert target data to a map for fast lookup
    const targetMap = new Map();
    targetData.forEach(row => targetMap.set(row[mapping[uniqueKey]], row));

    // 2 & 3. Validate field mapping and values
    sourceData.forEach(srcRow => {
        const targetRow = targetMap.get(srcRow[uniqueKey]);
        if (!targetRow) {
            result.mismatchedRecords.push({
                key: srcRow[uniqueKey],
                error: "Missing target row"
            });
            result.status = "failed";
            return;
        }

        Object.entries(mapping).forEach(([srcField, tgtField]) => {
            if (srcRow[srcField] !== targetRow[tgtField]) {
                result.mismatchedRecords.push({
                    key: srcRow[uniqueKey],
                    field: srcField,
                    sourceValue: srcRow[srcField],
                    targetValue: targetRow[tgtField]
                });
                result.status = "failed";
            }
        });
    });

    return result;
}

function validateFieldMapping(sourceData, targetData, mapping) {
    const sourceFields = Object.keys(sourceData[0]);
    const targetFields = Object.keys(targetData[0]);
    const mappingResults = [];

    Object.entries(mapping).forEach(([srcField, tgtField]) => {
        const sourceExists = sourceFields.includes(srcField);
        const targetExists = targetFields.includes(tgtField);
        const status = sourceExists && targetExists ? "success" : "failed";

        mappingResults.push({
            sourceField: srcField,
            targetField: tgtField,
            status,
            message: status === "failed"
                ? `Missing ${!sourceExists ? 'source field: ' + srcField : ''}${!sourceExists && !targetExists ? ' and ' : ''}${!targetExists ? 'target field: ' + tgtField : ''}`
                : "Field mapping valid"
        });
    });

    return mappingResults;
}

async function runValidation() {
    const [sourceData, targetData] = await Promise.all([
        readCSV(sourceFile),
        readCSV(targetFile)
    ]);

    const mappingData = JSON.parse(fs.readFileSync(mappingFile, 'utf-8'));
    const { fieldMapping, uniqueKey } = mappingData;

    // Validate field existence (mapping)
    const fullFieldMappingCheck = validateFieldMapping(sourceData, targetData, fieldMapping);
    const fieldMappingCheck = fullFieldMappingCheck.filter(f => f.status === 'failed');

    // Validate row counts and value match
    const fullValidation = validateData(sourceData, targetData, fieldMapping, uniqueKey);
    const mismatchedRecords = fullValidation.mismatchedRecords;
    const rowCountCheck = fullValidation.rowCountCheck.status === 'failed' ? fullValidation.rowCountCheck : null;

    const finalResult = {
        ...(rowCountCheck && { rowCountCheck }),
        ...(fieldMappingCheck.length > 0 && { fieldMappingCheck }),
        ...(mismatchedRecords.length > 0 && { mismatchedRecords }),
        status: (rowCountCheck || fieldMappingCheck.length > 0 || mismatchedRecords.length > 0) ? "failed" : "success"
    };

    fs.writeFileSync(resultFile, JSON.stringify(finalResult, null, 2));
    console.log("Validation complete. Result saved to result.json");
}

runValidation();