# Bank Statement Processor (Java)

Daily job that reads CSV bank statements and appends per-user CSV outputs.

## Requirements
- Java 11+
- Maven 3.8+

## Build
```bash
mvn -DskipTests package
```

## Run once (process today and exit)
```bash
RUN_ONCE=true mvn exec:java
```

## Run as a daily daemon (schedules at 02:00 local time)
```bash
mvn exec:java
```

## Input
Place CSV files in `input/` with header:
```
transactionId,userId,date,amount,description
```
- `date`: yyyy-MM-dd
- Only rows matching today’s date are processed

## Output
Per-user CSVs are appended in `output/{userId}.csv` with header:
```
transactionId,date,amount,description,sourceFile
```

## Notes
- Configure run-once via env var `RUN_ONCE=true`
- Invalid rows (bad date/amount or missing IDs) are skipped with a warning
