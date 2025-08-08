package com.example;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

import java.io.Reader;
import java.io.Writer;
import java.io.IOException;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class DailyBankStatementProcessor {

    private static final Path INPUT_DIR = Paths.get("input");
    private static final Path OUTPUT_DIR = Paths.get("output");
    private static final int RUN_HOUR = 2;     // 02:00 local time
    private static final int RUN_MINUTE = 0;

    public static void main(String[] args) throws Exception {
        Files.createDirectories(INPUT_DIR);
        Files.createDirectories(OUTPUT_DIR);

        final boolean runOnce = Boolean.parseBoolean(Optional.ofNullable(System.getenv("RUN_ONCE")).orElse("false"));

        Runnable task = () -> {
            try {
                System.out.println("[" + Instant.now() + "] Starting daily bank statement processing...");
                List<BankStatement> todays = readTodaysStatements(INPUT_DIR, LocalDate.now());
                System.out.println("Found " + todays.size() + " transactions for today");
                writeByUser(todays, OUTPUT_DIR);
                System.out.println("[" + Instant.now() + "] Done.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        // Run once on startup
        task.run();

        if (runOnce) {
            return; // exit after one run
        }

        // Schedule daily
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        long initialDelaySeconds = computeInitialDelaySeconds(RUN_HOUR, RUN_MINUTE);
        long periodSeconds = TimeUnit.DAYS.toSeconds(1);
        scheduler.scheduleAtFixedRate(task, initialDelaySeconds, periodSeconds, TimeUnit.SECONDS);
    }

    private static long computeInitialDelaySeconds(int hour, int minute) {
        ZonedDateTime now = ZonedDateTime.now();
        ZonedDateTime next = now.withHour(hour).withMinute(minute).withSecond(0).withNano(0);
        if (!next.isAfter(now)) {
            next = next.plusDays(1);
        }
        return Duration.between(now, next).toSeconds();
    }

    private static List<BankStatement> readTodaysStatements(Path inputDir, LocalDate today) throws IOException {
        List<BankStatement> results = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(inputDir, "*.csv")) {
            for (Path file : stream) {
                results.addAll(readFromFile(file, today));
            }
        }
        return results;
    }

    private static List<BankStatement> readFromFile(Path file, LocalDate today) {
        List<BankStatement> list = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(file);
             CSVReader csv = new CSVReaderBuilder(reader).withSkipLines(1).build()) {

            String[] row;
            while ((row = csv.readNext()) != null) {
                if (row.length < 4) continue;

                String transactionId = safe(row, 0);
                String userId = safe(row, 1);
                String dateStr = safe(row, 2);
                String amountStr = safe(row, 3);
                String description = row.length > 4 ? row[4] : "";

                LocalDate date;
                try {
                    date = LocalDate.parse(dateStr);
                } catch (DateTimeParseException e) {
                    System.err.println("Skipping row with invalid date: " + Arrays.toString(row));
                    continue;
                }
                if (!date.equals(today)) continue;

                BigDecimal amount;
                try {
                    amount = new BigDecimal(amountStr);
                } catch (NumberFormatException e) {
                    System.err.println("Skipping row with invalid amount: " + Arrays.toString(row));
                    continue;
                }

                if (userId.isEmpty() || transactionId.isEmpty()) {
                    System.err.println("Skipping row with missing userId/transactionId: " + Arrays.toString(row));
                    continue;
                }

                list.add(new BankStatement(transactionId, userId, date, amount, description, file.getFileName().toString()));
            }
        } catch (IOException e) {
            System.err.println("Failed to read " + file + ": " + e.getMessage());
        }
        return list;
    }

    private static void writeByUser(List<BankStatement> statements, Path outputDir) {
        Map<String, List<BankStatement>> byUser = statements.stream()
                .collect(Collectors.groupingBy(s -> s.userId));

        byUser.forEach((userId, rows) -> {
            Path out = outputDir.resolve(userId + ".csv");
            boolean exists = Files.exists(out);
            try (Writer writer = new FileWriter(out.toFile(), true);
                 CSVWriter csvWriter = new CSVWriter(writer)) {

                if (!exists) {
                    csvWriter.writeNext(new String[]{"transactionId", "date", "amount", "description", "sourceFile"});
                }
                for (BankStatement s : rows) {
                    csvWriter.writeNext(new String[]{
                            s.transactionId,
                            s.date.toString(),
                            s.amount.toPlainString(),
                            s.description,
                            s.sourceFile
                    });
                }
            } catch (IOException e) {
                System.err.println("Failed to write for user " + userId + ": " + e.getMessage());
            }
        });
    }

    private static String safe(String[] row, int idx) {
        return (idx < row.length && row[idx] != null) ? row[idx].trim() : "";
    }

    private static final class BankStatement {
        final String transactionId;
        final String userId;
        final LocalDate date;
        final BigDecimal amount;
        final String description;
        final String sourceFile;

        BankStatement(String transactionId, String userId, LocalDate date, BigDecimal amount, String description, String sourceFile) {
            this.transactionId = transactionId;
            this.userId = userId;
            this.date = date;
            this.amount = amount;
            this.description = description == null ? "" : description;
            this.sourceFile = sourceFile;
        }
    }
}