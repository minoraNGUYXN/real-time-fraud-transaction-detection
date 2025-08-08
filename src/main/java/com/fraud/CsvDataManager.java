package com.fraud;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CsvDataManager {
    private final List<String> transactionLines;
    private final Customer.CustomerManager customerManager;
    private final Random random;

    public CsvDataManager(String transactionCsvPath, String customerCsvPath) throws IOException {
        this.transactionLines = loadCsvLines(transactionCsvPath, "transactions");
        this.customerManager = new Customer.CustomerManager();
        this.random = new Random();

        loadCustomers(customerCsvPath);
        System.out.printf("Loaded %d transactions and %d customers%n",
                transactionLines.size(), customerManager.getCacheSize());
    }

    private List<String> loadCsvLines(String csvPath, String dataType) throws IOException {
        if (!Files.exists(Paths.get(csvPath))) {
            throw new IOException(dataType + " CSV file not found: " + csvPath);
        }

        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(csvPath))) {
            reader.readLine(); // Skip header
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    lines.add(line.trim());
                }
            }
        }

        if (lines.isEmpty()) {
            throw new IOException("No " + dataType + " found in CSV file");
        }
        return lines;
    }

    private void loadCustomers(String customerCsvPath) throws IOException {
        List<String> customerLines = loadCsvLines(customerCsvPath, "customers");
        for (String line : customerLines) {
            Customer customer = Customer.fromCsv(line);
            if (customer != null) {
                customerManager.addCustomer(customer);
            }
        }
    }

    public String getRandomTransactionLine() {
        return transactionLines.get(random.nextInt(transactionLines.size()));
    }

    public Customer getCustomer(String ccNum) {
        return customerManager.getCustomer(ccNum);
    }
}