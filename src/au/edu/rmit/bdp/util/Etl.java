package au.edu.rmit.bdp.util;

import au.edu.rmit.bdp.clustering.model.Centroid;
import au.edu.rmit.bdp.clustering.model.DataPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Stream;

public class Etl {

    public static void main(String[] args) throws IOException {
    }

    // VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,RatecodeID,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount
    // long: 5, lat: 6
    // @inputFile raw data from CSV, to be cleaned before mapreduce
    // @mapReduceInput output dest for raw data, will be input data for mapreduce
    @SuppressWarnings("deprecation")
    public static void extractData(Path inputFile, Configuration conf, Path mapReduceInput, FileSystem fs) throws IOException {
        try (SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs, conf, mapReduceInput, Centroid.class,
                DataPoint.class)) {
            final Centroid defaultCentroid = new Centroid(new DataPoint(0, 0));

            try (Stream<String> stream = new BufferedReader(new InputStreamReader(fs.open(inputFile).getWrappedStream())).lines()) {
                stream.skip(1) // skip fist line
                        .map(s -> {
                            String[] arr = s.split(",");
                            double longitude = Double.parseDouble(arr[5]);
                            double latitude = Double.parseDouble(arr[6]);
                            return new DataPoint(latitude, longitude);
                        })
                        .filter(dp -> dp.getVector().sum() != 0)  // bad point
                        .forEach(dp -> write(defaultCentroid, dp, dataWriter));

            };
        } catch (IOException e) {
            e.printStackTrace();
            throw e; // kinda need some data
        }
    }

    // small helper function for cleaner stream
    private static void write(Centroid cent, DataPoint dp, SequenceFile.Writer writer) {
        try {
            writer.append(cent, dp);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static double[] findExtremes(Path inputFile, Configuration conf, Path mapReduceInput, FileSystem fs) throws IOException {

        double maxLat = -Double.MAX_VALUE;
        double minLat = Double.MAX_VALUE;
        double maxLong = -Double.MAX_VALUE;
        double minLong = Double.MAX_VALUE;

        String line;
        BufferedReader stream = new BufferedReader(new InputStreamReader(fs.open(inputFile)));
        stream.readLine(); // skip first line
        while((line = stream.readLine()) != null) {
            String[] arr = line.split(",");
            double longitude = Double.parseDouble(arr[5]);
            double latitude = Double.parseDouble(arr[6]);
            if (longitude == 0 || latitude == 0) continue;
            if (maxLat < latitude) maxLat = latitude;
            if (minLat > latitude) minLat = latitude;
            if (maxLong < longitude) maxLong = longitude;
            if (minLong > longitude) minLong = longitude;
        }
        System.out.println("Extremes found:\nLatitude: " + maxLat + ", " + minLat + "\nLongitude: " +
                maxLong + ", " + minLong);
        return new double[] {maxLat, minLat, maxLong, minLong};
    }
}
