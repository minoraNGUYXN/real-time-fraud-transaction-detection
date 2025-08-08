package com.fraud;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class Customer {
    private final String ccNum;
    private final int age;
    private final double lat, lng;

    public Customer(String ccNum, int age, double lat, double lng) {
        this.ccNum = ccNum; this.age = age; this.lat = lat; this.lng = lng;
    }

    // Parse from CSV line: ccNum,age,lat,lng
    public static Customer fromCsv(String csvLine) {
        String[] parts = csvLine.split(",");
        if (parts.length < 4) return null;
        try {
            return new Customer(parts[0], Integer.parseInt(parts[1]),
                    Double.parseDouble(parts[2]), Double.parseDouble(parts[3]));
        } catch (NumberFormatException e) { return null; }
    }

    // Getters
    public String getCcNum() { return ccNum; }
    public int getAge() { return age; }
    public double getLat() { return lat; }
    public double getLng() { return lng; }

    @Override
    public String toString() {
        return String.format("Customer{ccNum=%s, age=%d, lat=%.6f, lng=%.6f}", ccNum, age, lat, lng);
    }

    // Customer cache management
    public static class CustomerManager {
        private final Map<String, Customer> customerCache = new ConcurrentHashMap<>();

        public void addCustomer(Customer customer) {
            customerCache.put(customer.getCcNum(), customer);
        }

        public Customer getCustomer(String ccNum) {
            return customerCache.get(ccNum);
        }

        public void clearCache() { customerCache.clear(); }
        public int getCacheSize() { return customerCache.size(); }
    }
}