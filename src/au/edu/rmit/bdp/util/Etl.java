package au.edu.rmit.bdp.util;

import au.edu.rmit.bdp.clustering.model.Centroid;
import au.edu.rmit.bdp.clustering.model.DataPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class Etl {

    public static void main(String[] args) throws IOException {
        java.nio.file.Path path = Paths.get(args[0]);
        System.out.println(path);
    }

    // VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,RatecodeID,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount
    // long: 5, lat: 6
    // @inputFile raw data from CSV, to be cleaned before mapreduce
    // @mapReduceInput output dest for raw data, will be input data for mapreduce
    @SuppressWarnings("deprecation")
    public static void extractData(java.nio.file.Path inputFile, Configuration conf, Path mapReduceInput, FileSystem fs) throws IOException {
        try (SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs, conf, mapReduceInput, Centroid.class,
                DataPoint.class)) {
            final Centroid defaultCentroid = new Centroid(new DataPoint(0, 0));
            try (Stream<String> stream = Files.lines(inputFile)) {
                stream.skip(1) // skip fist line
                        .map(s -> {
                            String[] arr = s.split(",");
                            double longitude = Double.parseDouble(arr[5]);
                            double latitude = Double.parseDouble(arr[6]);
                            return new DataPoint(longitude, latitude);
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
}
