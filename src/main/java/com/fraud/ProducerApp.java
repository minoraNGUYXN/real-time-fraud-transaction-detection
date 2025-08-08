package com.fraud;

import org.apache.kafka.clients.producer.*;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ProducerApp {
    private static final int TRANSACTION_COUNT = 1000;
    private static final int SLEEP_INTERVAL_MS = 500;
    private static final String TRANSACTION_CSV = "data/transactions.csv";
    private static final String CUSTOMER_CSV = "data/customers.csv";

    public static void main(String[] args) {
        System.out.println("Starting Transaction Producer from CSV...");

        try (Producer<String, String> producer = createKafkaProducer()) {
            CsvDataManager csvManager = new CsvDataManager(TRANSACTION_CSV, CUSTOMER_CSV);

            for (int i = 0; i < TRANSACTION_COUNT; i++) {
                String transactionCsv = csvManager.getRandomTransactionLine();
                String ccNum = transactionCsv.split(",")[0]; // First column is ccNum

                producer.send(new ProducerRecord<>("transactions", ccNum, transactionCsv),
                        (metadata, exception) -> {
                            if (exception != null) {
                                System.err.println("Failed to send: " + exception.getMessage());
                            } else {
                                System.out.printf("Sent [%d]: %s%n", metadata.offset(),
                                        transactionCsv.substring(0, Math.min(50, transactionCsv.length())) + "...");
                            }
                        });

                TimeUnit.MILLISECONDS.sleep(SLEEP_INTERVAL_MS);
            }

            System.out.printf("Completed sending %d transactions%n", TRANSACTION_COUNT);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Producer interrupted");
        } catch (IOException e) {
            System.err.println("Error reading CSV files: " + e.getMessage());
        }
    }

    private static Producer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "1");
        props.put("retries", 3);
        return new KafkaProducer<>(props);
    }
}