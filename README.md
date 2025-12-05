# CSV Column Generator

Generate AI-powered columns for CSV files using OpenRouter.

## Usage

1. Put your OPENROUTER_API_KEY in `.env`
2. Create a config YAML (see `config.yaml`)
3. Run:
   ```bash
   npm install
   node generate-csv-column.js your-config.yaml
   ```

## Features

- **Single or grouped columns**: Generate one column per call, or group multiple columns into one JSON-returning call
- **Web search plugins**: Use OpenRouter's web search with configurable engines and result counts
- **Search context control**: Set search context size (low/medium/high)
- **Progress files**: Auto-saves to `.progress` file after each batch for crash recovery
- **Selective retry**: Only retries failed rows, not entire batches
- **Cost tracking**: Reports tokens and cost per column

## Config Format

```yaml
inputFileName: input/northernlion_sample_100.csv
outputFileName: output/northernlion_sample_100_output.csv

columns:
  # Single column
  - columnName: Summary
    modelName: google/gemini-2.5-flash
    batchSize: 100
    cooldown: 200
    prompt: Summarize this video: {{Video}}

  # Grouped columns with web search
  - group:
      groupName: "Metadata"
      modelName: google/gemini-2.5-flash:online
      batchSize: 50
      cooldown: 200
      modelPlugins:
        - id: "web"
          engine: "exa"
          max_results: 10
      webSearchOptions:
        search_context_size: "high"
      columns:
        - Game Name
        - Category
      prompt: |
        Return JSON: {"Game Name": "...", "Category": "..."}
```

See `config.yaml` for full examples.
