package edu.cs.utexas.HadoopEx;

public class Utils {
    private static final double EPSILON = 1e-9;

    public static final int TAXI_ID_IDX = 0;
    public static final int DRIVER_ID_IDX = 1;
    public static final int PICK_TIME_IDX = 2;
    public static final int TRIP_DUR_IDX = 4;
    public static final int PICK_LONG_IDX = 6;
    public static final int PICK_LAT_IDX = 7;
    public static final int DROP_LONG_IDX = 8;
    public static final int DROP_LAT_IDX = 9;
    public static final int TOTAL_AMT_IDX = 16;

    public static int K = 0;

    public static boolean validGPS(String pickLong, String pickLat, String dropLong, String dropLat){
        return !pickLong.equals("0") && !pickLat.equals("0") && !dropLong.equals("0") && !pickLat.equals("0") &&
        !pickLong.equals("") && !pickLat.equals("") && !dropLong.equals("") && !pickLat.equals("");
    }

    public static String extractHour(String time){
        return time.split(" ")[1].split(":")[0];
    }

    public static boolean validRecord(String[] fields) {
    if (fields.length != 17) {
        System.out.println("Wrong # of commas for the record");
        return false;
    }
    double time = 0.0;

    // Adding 11-15, Check with 16
    double fare = 0.0;
    double surcharge = 0.0;
    double mta = 0.0;
    double toll = 0.0;
    double tip = 0.0;
    double total = 0.0;
    
    try {
        time = Double.parseDouble(fields[4]);
        fare = Double.parseDouble(fields[11]);
        surcharge = Double.parseDouble(fields[12]);
        mta = Double.parseDouble(fields[13]);
        toll = Double.parseDouble(fields[14]);
        tip = Double.parseDouble(fields[15]);
        total = Double.parseDouble(fields[16]);
    } catch (Exception E) {
        E.printStackTrace();
        System.out.println("Cannot parse the fields where dollars are expected");
        return false;
    }
    
    if(total <= 0.0) {
        return false;
    }

    if(time <= 0.0) {
        return false;
    }
    
    if (total > 500.0) {
        System.out.println("Above $500");
        return false;
    }
    
    double sum = fare + surcharge + mta + toll + tip;
    if (Math.abs(total - sum) > EPSILON) {
        System.out.printf("Sum != Total %f %f\n", sum, total);
        return false;
    }

    return true;
    }

}