#!/usr/bin/env node

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import csv from 'csv-parser';
import { createObjectCsvWriter } from 'csv-writer';
import yaml from 'js-yaml';
import dotenv from 'dotenv';
import axios from 'axios';

// Load environment variables
dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
const OPENROUTER_API_URL = 'https://openrouter.ai/api/v1/chat/completions';

/**
 * Sleep for specified milliseconds
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Replace template variables in prompt with actual values
 * Template syntax: {{columnName}}
 */
function fillPromptTemplate(prompt, rowData) {
  return prompt.replace(/\{\{(\w+(?:\s+\w+)*)\}\}/g, (match, columnName) => {
    const value = rowData[columnName];
    return value !== undefined ? value : match;
  });
}

/**
 * Call OpenRouter API (no retry logic - handled at batch level)
 * Returns: { result: string, cost: number, promptTokens: number, completionTokens: number }
 */
async function callOpenRouterAPI(modelName, prompt, plugins = null, webSearchOptions = null) {
  const requestBody = {
    model: modelName,
    messages: [
      {
        role: 'user',
        content: prompt
      }
    ],
    usage: {
      include: true
    }
  };

  // Add plugins if provided
  if (plugins && Array.isArray(plugins) && plugins.length > 0) {
    requestBody.plugins = plugins;
  }

  // Add web_search_options if provided
  if (webSearchOptions) {
    requestBody.web_search_options = webSearchOptions;
  }

  const response = await axios.post(
    OPENROUTER_API_URL,
    requestBody,
    {
      headers: {
        'Authorization': `Bearer ${OPENROUTER_API_KEY}`,
        'Content-Type': 'application/json',
        'HTTP-Referer': 'https://github.com/yourusername/youtube-analytics',
        'X-Title': 'YouTube Analytics CSV Generator'
      },
      timeout: 60000 // 60 second timeout
    }
  );

  const result = response.data.choices[0].message.content.trim();

  // Extract usage information
  const usage = response.data.usage || {};
  const promptTokens = usage.prompt_tokens || 0;
  const completionTokens = usage.completion_tokens || 0;
  const totalTokens = usage.total_tokens || (promptTokens + completionTokens);

  // OpenRouter returns cost when usage accounting is enabled
  const cost = usage.cost || 0;

  return {
    result,
    cost,
    promptTokens,
    completionTokens,
    totalTokens
  };
}

/**
 * Read CSV file into array of objects
 */
async function readCSV(filePath) {
  return new Promise((resolve, reject) => {
    const rows = [];

    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => rows.push(data))
      .on('end', () => resolve(rows))
      .on('error', (error) => reject(error));
  });
}

/**
 * Write data to CSV file
 */
async function writeCSV(filePath, data, headers) {
  const csvWriter = createObjectCsvWriter({
    path: filePath,
    header: headers.map(h => ({ id: h, title: h }))
  });

  await csvWriter.writeRecords(data);
}

/**
 * Process a single row (helper for processBatch)
 */
async function processRow(row, rowIndex, columnInfo, modelName, prompt, plugins, webSearchOptions) {
  // Fill prompt template with row data
  const filledPrompt = fillPromptTemplate(prompt, row);

  // Call API
  const apiResult = await callOpenRouterAPI(modelName, filledPrompt, plugins, webSearchOptions);

  // Store result in row based on column type
  if (typeof columnInfo === 'string') {
    // Single column (backward compatible)
    row[columnInfo] = apiResult.result;
  } else {
    // Grouped columns - parse JSON and distribute to columns
    try {
      // Strip markdown code fences if present (```json ... ``` or ``` ... ```)
      let jsonText = apiResult.result.trim();
      if (jsonText.startsWith('```')) {
        // Remove opening fence (```json or ```)
        jsonText = jsonText.replace(/^```(?:json)?\s*\n?/, '');
        // Remove closing fence (```)
        jsonText = jsonText.replace(/\n?```\s*$/, '');
      }

      const parsed = JSON.parse(jsonText);
      for (const colName of columnInfo.columns) {
        row[colName] = parsed[colName] || '__undetectable__';
      }
    } catch (parseError) {
      // JSON parse error - log and set all to __undetectable__
      console.error(`    ✗ Row ${rowIndex + 1}: Failed to parse JSON - ${parseError.message}`);
      console.error(`      Response: ${apiResult.result.substring(0, 200)}...`);
      for (const colName of columnInfo.columns) {
        row[colName] = '__undetectable__';
      }
    }
  }

  return {
    success: true,
    cost: apiResult.cost,
    promptTokens: apiResult.promptTokens,
    completionTokens: apiResult.completionTokens,
    rowIndex
  };
}

/**
 * Process a single batch of rows with retry logic (only retries failed rows)
 * columnInfo can be:
 * - String: single column name (backward compatible)
 * - Object: { isGroup: true, columns: ["Col1", "Col2", ...] } for grouped columns
 */
async function processBatch(rows, batchStart, batchEnd, columnInfo, modelName, prompt, plugins = null, webSearchOptions = null, maxRetries = 10) {
  // Track rows that need to be processed
  let rowsToProcess = [];
  for (let j = batchStart; j < batchEnd; j++) {
    rowsToProcess.push(j);
  }

  const allResults = [];
  let retryCount = 0;

  while (rowsToProcess.length > 0 && retryCount <= maxRetries) {
    // Create promises for rows that still need processing
    const batchPromises = rowsToProcess.map(rowIndex => {
      const row = rows[rowIndex];
      return processRow(row, rowIndex, columnInfo, modelName, prompt, plugins, webSearchOptions)
        .then(result => ({ success: true, result, rowIndex }))
        .catch(error => ({ success: false, error, rowIndex }));
    });

    // Wait for all promises to settle
    const results = await Promise.all(batchPromises);

    // Separate successful and failed rows
    const succeeded = [];
    const failed = [];

    for (const result of results) {
      if (result.success) {
        succeeded.push(result);
        allResults.push(result.result);
      } else {
        failed.push(result);
      }
    }

    // Update rows to process - only failed ones
    rowsToProcess = failed.map(f => f.rowIndex);

    if (rowsToProcess.length > 0) {
      retryCount++;

      // Log error details from failed rows
      const errorSummary = {};
      for (const failure of failed) {
        const statusCode = failure.error.response?.status || 'N/A';
        const errorData = failure.error.response?.data;

        // Try to extract detailed error message
        let errorMsg = failure.error.message;
        if (errorData) {
          if (errorData.error?.message) {
            errorMsg = errorData.error.message;
          } else if (typeof errorData === 'string') {
            errorMsg = errorData;
          } else if (errorData.message) {
            errorMsg = errorData.message;
          }

          // Include metadata if available
          if (errorData.error?.metadata) {
            errorMsg += ` (${JSON.stringify(errorData.error.metadata)})`;
          }
        }

        const key = `${statusCode}: ${errorMsg}`;
        if (!errorSummary[key]) {
          errorSummary[key] = [];
        }
        errorSummary[key].push(failure.rowIndex + 1);
      }

      if (retryCount > maxRetries) {
        console.error(`  ✗ ${rowsToProcess.length} row(s) FAILED after ${maxRetries} retries`);
        console.error(`    Failed rows: ${rowsToProcess.map(i => i + 1).join(', ')}`);
        console.error(`    Error summary:`);
        for (const [error, rows] of Object.entries(errorSummary)) {
          console.error(`      ${error}`);
          console.error(`        Affected rows: ${rows.join(', ')}`);
        }

        throw new Error(`${rowsToProcess.length} row(s) failed after ${maxRetries} retries`);
      }

      // Exponential backoff: 2^retryCount seconds
      const backoffMs = Math.pow(2, retryCount) * 1000;
      console.warn(`  ⚠ ${rowsToProcess.length} row(s) failed (attempt ${retryCount}/${maxRetries})`);
      console.warn(`    Retrying failed rows: ${rowsToProcess.map(i => i + 1).join(', ')}`);

      // Log error types
      for (const [error, rows] of Object.entries(errorSummary)) {
        console.warn(`    ${error} (rows: ${rows.join(', ')})`);
      }

      // On first retry, log response body from first failure
      if (retryCount === 1 && failed.length > 0 && failed[0].error.response?.data) {
        console.warn(`    Response body: ${JSON.stringify(failed[0].error.response.data)}`);
      }

      console.warn(`    Waiting ${backoffMs/1000}s before retry...`);
      await sleep(backoffMs);
    }
  }

  return allResults;
}

/**
 * Process a single column configuration (single or grouped)
 */
async function processColumn(columnConfig, rows, columnIndex, totalColumns, progressFilePath) {
  // Detect if this is a grouped column or single column
  const isGrouped = !!columnConfig.group;

  let columnName, modelName, batchSize, cooldown, prompt, plugins, webSearchOptions, columnInfo;

  if (isGrouped) {
    // Extract from grouped config
    const group = columnConfig.group;
    columnName = `${group.groupName} (${group.columns.length} columns: ${group.columns.join(', ')})`;
    modelName = group.modelName;
    batchSize = group.batchSize;
    cooldown = group.cooldown;
    prompt = group.prompt;
    plugins = group.modelPlugins || null;
    webSearchOptions = group.webSearchOptions || null;
    columnInfo = { isGroup: true, columns: group.columns };
  } else {
    // Extract from single column config
    columnName = columnConfig.columnName;
    modelName = columnConfig.modelName;
    batchSize = columnConfig.batchSize;
    cooldown = columnConfig.cooldown;
    prompt = columnConfig.prompt;
    plugins = columnConfig.modelPlugins || null;
    webSearchOptions = columnConfig.webSearchOptions || null;
    columnInfo = columnConfig.columnName; // Simple string for backward compatibility
  }

  console.log(`\n${'='.repeat(80)}`);
  console.log(`${isGrouped ? 'Column Group' : 'Column'} ${columnIndex + 1}/${totalColumns}: "${columnName}"`);
  console.log(`${'='.repeat(80)}`);
  console.log(`Model:      ${modelName}`);
  console.log(`Batch size: ${batchSize}`);
  console.log(`Cooldown:   ${cooldown}ms`);
  if (plugins && plugins.length > 0) {
    console.log(`Plugins:    ${JSON.stringify(plugins)}`);
  }
  if (webSearchOptions) {
    console.log(`Web Search: ${JSON.stringify(webSearchOptions)}`);
  }
  console.log(`Total rows: ${rows.length}`);

  const totalRows = rows.length;
  let processedCount = 0;
  let totalCost = 0;
  let totalPromptTokens = 0;
  let totalCompletionTokens = 0;
  const startTime = Date.now();

  // Process rows in batches (rows within batch processed in parallel)
  for (let i = 0; i < totalRows; i += batchSize) {
    const batchEnd = Math.min(i + batchSize, totalRows);
    const batchNumber = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(totalRows / batchSize);
    const batchStartTime = Date.now();

    console.log(`\n  Batch ${batchNumber}/${totalBatches} (rows ${i + 1}-${batchEnd}) - processing ${batchEnd - i} rows in parallel...`);

    // Process batch with retry logic
    const batchResults = await processBatch(rows, i, batchEnd, columnInfo, modelName, prompt, plugins, webSearchOptions);

    // Aggregate results
    let batchCost = 0;
    for (const result of batchResults) {
      processedCount++;
      totalCost += result.cost;
      batchCost += result.cost;
      totalPromptTokens += result.promptTokens;
      totalCompletionTokens += result.completionTokens;
    }

    const batchElapsed = Date.now() - batchStartTime;
    const batchTokens = batchResults.reduce((sum, r) => sum + r.promptTokens + r.completionTokens, 0);

    console.log(`  ✓ Completed in ${(batchElapsed / 1000).toFixed(2)}s | Success: ${processedCount}/${batchEnd}`);
    console.log(`    Batch tokens: ${batchTokens.toLocaleString()} | Batch cost: $${batchCost.toFixed(8)}`);
    console.log(`    Running total: ${(totalPromptTokens + totalCompletionTokens).toLocaleString()} tokens | $${totalCost.toFixed(8)}`)

    // Write progress file with all data so far
    if (progressFilePath) {
      try {
        const allHeaders = Object.keys(rows[0]);
        await writeCSV(progressFilePath, rows, allHeaders);
      } catch (err) {
        console.warn(`    ⚠ Failed to write progress file: ${err.message}`);
      }
    }

    // Apply cooldown between batches (except after last batch)
    if (batchEnd < totalRows && cooldown > 0) {
      console.log(`  ⏸  Cooling down for ${cooldown}ms...`);
      await sleep(cooldown);
    }
  }

  const totalElapsed = Date.now() - startTime;
  const avgTimePerRow = totalElapsed / totalRows;
  const totalTokens = totalPromptTokens + totalCompletionTokens;

  console.log(`\n${'─'.repeat(80)}`);
  console.log(`${isGrouped ? 'Column Group' : 'Column'} "${isGrouped ? columnConfig.group.groupName : columnName}" Summary:`);
  console.log(`  Total time:     ${(totalElapsed / 1000).toFixed(2)}s`);
  console.log(`  Avg per row:    ${avgTimePerRow.toFixed(0)}ms`);
  console.log(`  Rows processed: ${processedCount}/${totalRows}`);
  console.log(`  Total tokens:   ${totalTokens.toLocaleString()} (${totalPromptTokens.toLocaleString()} prompt + ${totalCompletionTokens.toLocaleString()} completion)`);
  if (totalCost > 0) {
    console.log(`  Total cost:     $${totalCost.toFixed(8)}`);
    if (processedCount > 0) {
      console.log(`  Avg cost/row:   $${(totalCost / processedCount).toFixed(8)}`);
    }
  }
  console.log(`${'─'.repeat(80)}`);

  return { cost: totalCost, tokens: totalTokens, promptTokens: totalPromptTokens, completionTokens: totalCompletionTokens };
}

/**
 * Main function
 */
async function main() {
  const scriptStartTime = Date.now();

  try {
    console.log(`\n${'='.repeat(80)}`);
    console.log('CSV Column Generator with OpenRouter AI');
    console.log(`${'='.repeat(80)}\n`);

    // Check for config file argument
    const configPath = process.argv[2] || 'config.yaml';

    console.log('[1/5] Checking configuration...');
    if (!fs.existsSync(configPath)) {
      console.error(`  ✗ Config file not found: ${configPath}`);
      console.log('\nUsage: node generate-csv-column.js [config.yaml]');
      console.log('See config.example.yaml for configuration format');
      process.exit(1);
    }
    console.log(`  ✓ Config file found: ${configPath}`);

    // Check for API key
    if (!OPENROUTER_API_KEY) {
      console.error('  ✗ OPENROUTER_API_KEY not found in .env file');
      process.exit(1);
    }
    console.log(`  ✓ API key loaded (${OPENROUTER_API_KEY.substring(0, 8)}...)`);

    // Load configuration
    console.log('\n[2/5] Loading configuration...');
    const config = yaml.load(fs.readFileSync(configPath, 'utf8'));

    // Validate config
    if (!config.inputFileName || !config.outputFileName || !config.columns) {
      console.error('  ✗ Invalid configuration. Required fields: inputFileName, outputFileName, columns');
      process.exit(1);
    }
    console.log(`  ✓ Configuration loaded successfully`);
    console.log(`    Input:   ${config.inputFileName}`);
    console.log(`    Output:  ${config.outputFileName}`);
    console.log(`    Columns: ${config.columns.length}`);

    // Validate column configs
    for (let i = 0; i < config.columns.length; i++) {
      const col = config.columns[i];

      // Detect if this is a grouped column or single column
      if (col.group) {
        // Grouped column validation
        const group = col.group;
        if (!group.groupName || !group.modelName || !group.prompt || !group.columns || !Array.isArray(group.columns)) {
          console.error(`  ✗ Column ${i + 1} (grouped) missing required fields (groupName, modelName, prompt, columns array)`);
          process.exit(1);
        }
        if (group.columns.length === 0) {
          console.error(`  ✗ Column ${i + 1} (grouped) has empty columns array`);
          process.exit(1);
        }
        // Set defaults for grouped columns
        group.batchSize = group.batchSize || 10;
        group.cooldown = group.cooldown || 0;
      } else {
        // Single column validation (existing logic)
        if (!col.columnName || !col.modelName || !col.prompt) {
          console.error(`  ✗ Column ${i + 1} missing required fields (columnName, modelName, prompt)`);
          process.exit(1);
        }
        // Set defaults for single columns
        col.batchSize = col.batchSize || 10;
        col.cooldown = col.cooldown || 0;
      }
    }

    // Read input CSV
    console.log('\n[3/5] Reading input CSV...');
    if (!fs.existsSync(config.inputFileName)) {
      console.error(`  ✗ Input file not found: ${config.inputFileName}`);
      process.exit(1);
    }

    const rows = await readCSV(config.inputFileName);
    console.log(`  ✓ Loaded ${rows.length} rows`);

    if (rows.length === 0) {
      console.error('  ✗ Input CSV is empty');
      process.exit(1);
    }

    // Get original headers
    const originalHeaders = Object.keys(rows[0]);
    console.log(`  ✓ Original columns (${originalHeaders.length}): ${originalHeaders.join(', ')}`);

    // List new columns to be generated
    const newColumns = config.columns.map(c => c.columnName);
    console.log(`  ➜ Will generate columns (${newColumns.length}): ${newColumns.join(', ')}`);

    // Process each column in series
    console.log('\n[4/5] Processing columns...');
    const totalColumns = config.columns.length;

    // Create progress file path by replacing .csv with .progress
    const progressFilePath = config.outputFileName.replace(/\.csv$/i, '.progress');

    let grandTotalCost = 0;
    let grandTotalTokens = 0;
    let grandTotalPromptTokens = 0;
    let grandTotalCompletionTokens = 0;

    for (let i = 0; i < totalColumns; i++) {
      const columnStats = await processColumn(config.columns[i], rows, i, totalColumns, progressFilePath);
      grandTotalCost += columnStats.cost;
      grandTotalTokens += columnStats.tokens;
      grandTotalPromptTokens += columnStats.promptTokens;
      grandTotalCompletionTokens += columnStats.completionTokens;
    }

    // Get all headers (original + new columns)
    const allHeaders = Object.keys(rows[0]);
    const addedHeaders = allHeaders.filter(h => !originalHeaders.includes(h));

    // Write output CSV
    console.log('\n[5/5] Writing output CSV...');
    await writeCSV(config.outputFileName, rows, allHeaders);
    console.log(`  ✓ Output written to: ${config.outputFileName}`);
    console.log(`  ✓ Total columns: ${allHeaders.length} (${originalHeaders.length} original + ${addedHeaders.length} new)`);

    const totalElapsed = Date.now() - scriptStartTime;
    console.log(`\n${'='.repeat(80)}`);
    console.log('✓ Processing Complete!');
    console.log(`${'='.repeat(80)}`);
    console.log(`Total execution time: ${(totalElapsed / 1000).toFixed(2)}s`);
    console.log(`Output file: ${config.outputFileName}`);
    console.log(`\nOverall Statistics:`);
    console.log(`  Total tokens:   ${grandTotalTokens.toLocaleString()} (${grandTotalPromptTokens.toLocaleString()} prompt + ${grandTotalCompletionTokens.toLocaleString()} completion)`);
    if (grandTotalCost > 0) {
      console.log(`  Total cost:     $${grandTotalCost.toFixed(8)}`);
    }
    console.log(`${'='.repeat(80)}\n`);

  } catch (error) {
    console.error(`\n${'='.repeat(80)}`);
    console.error('✗ Fatal Error');
    console.error(`${'='.repeat(80)}`);
    console.error(`Error: ${error.message}`);
    if (error.stack) {
      console.error(`\nStack trace:\n${error.stack}`);
    }
    console.error(`${'='.repeat(80)}\n`);
    process.exit(1);
  }
}

// Run main function
main();
