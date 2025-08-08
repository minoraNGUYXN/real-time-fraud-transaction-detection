package com.fraud;

public class Transaction {
    private final String ccNum, first, last, transNum, transDate, transTime, unixTime;
    private final String category, merchant;
    private final double amt, merchLat, merchLong;
    private final int isFraud;

    public Transaction(String ccNum, String first, String last, String transNum,
                       String transDate, String transTime, String unixTime, String category,
                       String merchant, double amt, double merchLat, double merchLong, int isFraud) {
        this.ccNum = ccNum; this.first = first; this.last = last; this.transNum = transNum;
        this.transDate = transDate; this.transTime = transTime; this.unixTime = unixTime;
        this.category = category; this.merchant = merchant; this.amt = amt;
        this.merchLat = merchLat; this.merchLong = merchLong; this.isFraud = isFraud;
    }

    // Parse from CSV line
    public static Transaction fromCsv(String csvLine) {
        String[] parts = csvLine.split(",");
        if (parts.length < 13) return null;
        try {
            return new Transaction(parts[0], parts[1], parts[2], parts[3], parts[4],
                    parts[5], parts[6], parts[7], parts[8],
                    Double.parseDouble(parts[9]), Double.parseDouble(parts[10]),
                    Double.parseDouble(parts[11]), Integer.parseInt(parts[12]));
        } catch (NumberFormatException e) { return null; }
    }

    // Getters
    public String getCcNum() { return ccNum; }
    public String getFirst() { return first; }
    public String getLast() { return last; }
    public String getTransNum() { return transNum; }
    public String getTransDate() { return transDate; }
    public String getTransTime() { return transTime; }
    public String getUnixTime() { return unixTime; }
    public String getCategory() { return category; }
    public String getMerchant() { return merchant; }
    public double getAmt() { return amt; }
    public double getMerchLat() { return merchLat; }
    public double getMerchLong() { return merchLong; }
    public int getIsFraud() { return isFraud; }

    public String toCsv() {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%.2f,%.6f,%.6f,%d",
                ccNum, first, last, transNum, transDate, transTime, unixTime,
                category, merchant, amt, merchLat, merchLong, isFraud);
    }

    @Override
    public String toString() { return toCsv(); }
}