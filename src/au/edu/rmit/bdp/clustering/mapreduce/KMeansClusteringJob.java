package au.edu.rmit.bdp.clustering.mapreduce;

import au.edu.rmit.bdp.clustering.model.Centroid;
import au.edu.rmit.bdp.clustering.model.DataPoint;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static au.edu.rmit.bdp.util.Etl.extractData;

/**
 * K-means algorithm in mapReduce<p>
 *
 * Terminology explained:
 * - DataPoint: A dataPoint is a point in 2 dimensional space. we can have as many as points we want, and we are going to group
 * 				those points that are similar( near) to each other.
 * - cluster: A cluster is a group of dataPoints that are near to each other.
 * - Centroid: A centroid is the center point( not exactly, but you can think this way at first) of the cluster.
 *
 * Files involved:
 * - data.seq: It contains all the data points. Each chunk consists of a key( a dummy centroid) and a value(data point).
 * - centroid.seq: It contains all the centroids with random initial values. Each chunk consists of a key( centroid) and a value( a dummy int)
 * - depth_*.seq: These are a set of directories( depth_1.seq, depth_2.seq, depth_3.seq ... ), each of the directory will contain the result of one job.
 * 				  Note that the algorithm works iteratively. It will keep creating and executing the job before all the centroid converges.
 * 				  each of these directory contains files which is produced by reducer of previous round, and it is going to be fed to the mapper of next round.
 * Note, these files are binary files, and they follow certain protocals so that they can be serialized and deserialized by SequenceFileOutputFormat and SequenceFileInputFormat
 *
 * This is an high level demonstration of how this works:
 *
 * - We generate some data points and centroids, and write them to data.seq and cen.seq respectively. We use SequenceFile.Writer so that the data
 * 	 could be deserialize easily.
 *
 * - We start our first job, and feed data.seq to it, the output of reducer should be in depth_1.seq. cen.seq file is also updated in reducer#cleanUp.
 * - From our second job, we keep generating new job and feed it with previous job's output( depth_1.seq/ in this case),
 * 	 until all centroids converge.
 *
 */
public class KMeansClusteringJob {

    private static final Log LOG = LogFactory.getLog(KMeansClusteringJob.class);

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // First argument is path to raw input data (csv file)
        Path inputData = new Path(args[0]);
        // second argument number of reducers
        int numReducers = args.length >= 2 ? Integer.parseInt(args[1]) : 1;
        // third - eight argument are arguments for initial centroids: # centroids, min lat, min long, max lat, min long
        int numCentroids = 4;

        // default values, take a data sample and find max/min with Etl.findExtremes()
        double maxLat = 40.786888122558594;
        double minLat = 40.644405364990234;
        double maxLong = -73.76023864746094;
        double minLong = -74.0135498046875;

        if (args.length == 7) {
            maxLat = Double.parseDouble(args[3]);
            minLat = Double.parseDouble(args[4]);
            maxLong = Double.parseDouble(args[5]);
            minLong = Double.parseDouble(args[6]);
        }

        int iteration = 1;
        Configuration conf = new Configuration();
        conf.set("num.iteration", iteration + "");

        Path pointDataPath = new Path("clustering/data.seq");
        Path centroidDataPath = new Path("clustering/centroid.seq");
        conf.set("centroid.path", centroidDataPath.toString());
        Path outputDir = new Path("clustering/depth_1");

        Job job = Job.getInstance(conf);
        job.setJobName("KMeans Clustering");

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setJarByClass(KMeansMapper.class);

        FileInputFormat.addInputPath(job, pointDataPath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        if (fs.exists(centroidDataPath)) {
            fs.delete(centroidDataPath, true);
        }

        if (fs.exists(pointDataPath)) {
            fs.delete(pointDataPath, true);
        }

        generateCentroid(conf, centroidDataPath, fs, numCentroids, maxLat, minLat, maxLong, minLong);
        extractData(inputData, conf, pointDataPath, fs);

        job.setNumReduceTasks(numReducers);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Centroid.class);
        job.setOutputValueClass(DataPoint.class);

        job.waitForCompletion(true);

        long counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
        iteration++;
        while (counter > 0) {
            conf = new Configuration();
            conf.set("centroid.path", centroidDataPath.toString());
            conf.set("num.iteration", iteration + "");
            job = Job.getInstance(conf);
            job.setJobName("KMeans Clustering " + iteration);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setJarByClass(KMeansMapper.class);

            pointDataPath = new Path("clustering/depth_" + (iteration - 1) + "/");
            outputDir = new Path("clustering/depth_" + iteration);

            FileInputFormat.addInputPath(job, pointDataPath);
            if (fs.exists(outputDir))
                fs.delete(outputDir, true);

            FileOutputFormat.setOutputPath(job, outputDir);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Centroid.class);
            job.setOutputValueClass(DataPoint.class);
            job.setNumReduceTasks(1);

            job.waitForCompletion(true);
            iteration++;
            counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
        }

        Path result = new Path("clustering/depth_" + (iteration - 1) + "/");

        FileStatus[] stati = fs.listStatus(result);
        for (FileStatus status : stati) {
            if (!status.isDirectory()) {
                Path path = status.getPath();
                if (!path.getName().equals("_SUCCESS")) {
                    LOG.info("FOUND " + path.toString());
                    try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf)) {
                        Centroid key = new Centroid();
                        DataPoint v = new DataPoint();
                        while (reader.next(key, v)) {
                            LOG.info(key + " / " + v);
                        }
                    }
                }
            }
        }
    }

    @SuppressWarnings("deprecation")
    public static void generateCentroid(Configuration conf, Path center, FileSystem fs, int numCentroids,
                                        double maxLat, double minLat, double maxLong, double minLong) throws IOException {
        try (SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center, Centroid.class,
                IntWritable.class)) {
            final IntWritable value = new IntWritable(0);
            for (int i = 0; i < numCentroids; i++) {
                double lat = ThreadLocalRandom.current().nextDouble(minLat, maxLat);
                double lon = ThreadLocalRandom.current().nextDouble(minLong, maxLong);
                centerWriter.append(new Centroid(new DataPoint(lat, lon)), value);
                //System.out.println("Random centroid: " + lat + ", " + lon);
            }
        }
    }

}
