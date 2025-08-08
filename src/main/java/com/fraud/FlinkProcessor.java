package com.fraud;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.*;
import org.jpmml.evaluator.*;
import org.jpmml.model.PMMLUtil;

import java.io.FileInputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;


public class FlinkProcessor {

    public static class FeatureEngineeringFunction extends RichMapFunction<Transaction, EnrichedTransaction> {
        private transient CsvDataManager csvManager;

        @Override
        public void open(Configuration config) throws Exception {
            this.csvManager = new CsvDataManager("data/transactions.csv", "data/customers.csv");
        }

        @Override
        public EnrichedTransaction map(Transaction tx) {
            Customer customer = csvManager.getCustomer(tx.getCcNum());
            if (customer == null) {
                // Fallback to default customer if not found
                customer = new Customer(tx.getCcNum(), 30, 40.0, -74.0);
            }

            double distance = calculateDistance(customer.getLat(), customer.getLng(),
                    tx.getMerchLat(), tx.getMerchLong());
            int hourOfDay = extractHour(tx.getTransTime());

            return new EnrichedTransaction(tx, customer, distance, hourOfDay,
                    Math.abs(tx.getTransDate().hashCode()) % 7);
        }

        private double calculateDistance(double lat1, double lng1, double lat2, double lng2) {
            double deltaLat = Math.toRadians(lat2 - lat1);
            double deltaLng = Math.toRadians(lng2 - lng1);
            double a = Math.sin(deltaLat/2) * Math.sin(deltaLat/2) +
                    Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                            Math.sin(deltaLng/2) * Math.sin(deltaLng/2);
            return 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        }

        private int extractHour(String time) {
            try { return Integer.parseInt(time.split(":")[0]); }
            catch (Exception e) { return 12; }
        }
    }

    public static class FraudDetectionModel extends RichMapFunction<EnrichedTransaction, TransactionWithProbability> {
        private transient Evaluator evaluator;

        @Override
        public void open(Configuration config) throws Exception {
            try (FileInputStream stream = new FileInputStream("model/rf_fraud_model.pmml")) {
                org.dmg.pmml.PMML pmml = PMMLUtil.unmarshal(stream);
                this.evaluator = new ModelEvaluatorBuilder(pmml)
                        .setOutputFilter(OutputFilters.KEEP_FINAL_RESULTS).build();
                this.evaluator.verify();
                System.out.println("Model loaded successfully!");
            }
        }

        @Override
        public TransactionWithProbability map(EnrichedTransaction enriched) {
            try {
                Transaction tx = enriched.getTransaction();
                Map<String, Object> inputs = new LinkedHashMap<>();
                inputs.put("category", tx.getCategory());
                inputs.put("merchant", tx.getMerchant());
                inputs.put("amt", tx.getAmt());
                inputs.put("age", enriched.getCustomer().getAge());
                inputs.put("distance", enriched.getDistance());

                Map<String, ?> outputs = evaluator.evaluate(inputs);
                int prediction = extractPrediction(outputs);
                double probability = extractProbability(outputs);

                Transaction result = new Transaction(tx.getCcNum(), tx.getFirst(), tx.getLast(),
                        tx.getTransNum(), tx.getTransDate(), tx.getTransTime(), tx.getUnixTime(),
                        tx.getCategory(), tx.getMerchant(), tx.getAmt(), tx.getMerchLat(),
                        tx.getMerchLong(), prediction);

                return new TransactionWithProbability(result, probability);
            } catch (Exception e) {
                return new TransactionWithProbability(enriched.getTransaction(), 0.0);
            }
        }

        private int extractPrediction(Map<String, ?> outputs) {
            try {
                Object prob1 = outputs.get("probability(1)");
                if (prob1 instanceof Number) {
                    return ((Number) prob1).doubleValue() > 0.5 ? 1 : 0;
                }
                return 0;
            } catch (Exception e) { return 0; }
        }

        private double extractProbability(Map<String, ?> outputs) {
            try {
                Object prob1 = outputs.get("probability(1)");
                return prob1 instanceof Number ? ((Number) prob1).doubleValue() : 0.0;
            } catch (Exception e) { return 0.0; }
        }
    }

    // Helper classes
    public static class TransactionWithProbability {
        private final Transaction transaction;
        private final double probability;

        public TransactionWithProbability(Transaction transaction, double probability) {
            this.transaction = transaction; this.probability = probability;
        }

        public Transaction getTransaction() { return transaction; }
        public double getProbability() { return probability; }
    }

    public static class EnrichedTransaction {
        private final Transaction transaction;
        private final Customer customer;
        private final double distance;
        private final int hourOfDay, dayOfWeek;

        public EnrichedTransaction(Transaction transaction, Customer customer,
                                   double distance, int hourOfDay, int dayOfWeek) {
            this.transaction = transaction; this.customer = customer; this.distance = distance;
            this.hourOfDay = hourOfDay; this.dayOfWeek = dayOfWeek;
        }

        public Transaction getTransaction() { return transaction; }
        public Customer getCustomer() { return customer; }
        public double getDistance() { return distance; }
        public int getHourOfDay() { return hourOfDay; }
        public int getDayOfWeek() { return dayOfWeek; }
    }

    public static class TransactionParser implements MapFunction<String, Transaction> {
        @Override
        public Transaction map(String csv) {
            return Transaction.fromCsv(csv);
        }
    }

    public static class AlertFormatter implements MapFunction<TransactionWithProbability, String> {
        @Override
        public String map(TransactionWithProbability txWithProb) {
            return String.format(">>>FRAUD ALERT: %s | Probability: %.3f",
                    txWithProb.getTransaction().toCsv(), txWithProb.getProbability());
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "flink_fraud_group");
        kafkaProps.setProperty("auto.offset.reset", "latest");

        DataStream<String> fraudAlerts = env
                .addSource(new FlinkKafkaConsumer<>("transactions", new SimpleStringSchema(), kafkaProps))
                .map(new TransactionParser()).filter(tx -> tx != null)
                .map(new FeatureEngineeringFunction())
                .map(new FraudDetectionModel())
                .filter(txWithProb -> txWithProb.getTransaction().getIsFraud() == 1)
                .map(new AlertFormatter());

        fraudAlerts.addSink(new FlinkKafkaProducer<>("fraud_alerts", new SimpleStringSchema(), kafkaProps));
        fraudAlerts.print();

        System.out.println("Starting Real-time Fraud Detection Pipeline...");
        env.execute("Real-time Fraud Detection with CSV Data");
    }
}