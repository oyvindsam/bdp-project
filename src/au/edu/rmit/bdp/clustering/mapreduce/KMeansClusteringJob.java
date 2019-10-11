package au.edu.rmit.bdp.clustering.mapreduce;

import au.edu.rmit.bdp.clustering.model.Centroid;
import au.edu.rmit.bdp.clustering.model.DataPoint;
import au.edu.rmit.bdp.clustering.model.DpArrayWritable;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static au.edu.rmit.bdp.util.Etl.extractData;
import static au.edu.rmit.bdp.util.Etl.findExtremes;

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

        long startTime = System.currentTimeMillis();
        // First argument is path to raw input data (csv file)
        Path inputData = new Path(args[0]);
        // second argument number of reducers
        int numReducers = args.length >= 2 ? Integer.parseInt(args[1]) : 1;
        // # of centroids. Default 2.
        int numCentroids = args.length >= 3 ? Integer.parseInt(args[2]) : 2;

        boolean defaultCentroids = "d".equals(args[3]);

        // default values, take a data sample and find max/min with Etl.findExtremes()
        double maxLat = 40.86269760131836;
        double minLat = 40.64155960083008;
        double maxLong = -73.76023864746094;
        double minLong = -74.01734924316406;

        // fifth - ninth argument are arguments for initial centroids max-min values: min lat, min long, max lat, min long
        if (args.length == 8) {
            maxLat = Double.parseDouble(args[4]);
            minLat = Double.parseDouble(args[5]);
            maxLong = Double.parseDouble(args[6]);
            minLong = Double.parseDouble(args[7]);
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

        // use default centroids so easier to make statistics
        if (defaultCentroids) {
            generateDefaultCentroid(conf, centroidDataPath, fs, numCentroids);
        } else {
            generateCentroid(conf, centroidDataPath, fs, numCentroids, maxLat, minLat, maxLong, minLong);
        }
        extractData(inputData, conf, pointDataPath, fs);
        //findExtremes(inputData, conf, pointDataPath, fs);

        job.setNumReduceTasks(numReducers);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);


        job.setOutputKeyClass(Centroid.class);
        job.setOutputValueClass(DataPoint.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DpArrayWritable.class);

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

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(DpArrayWritable.class);


            job.setNumReduceTasks(numReducers);

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

        long endTime = System.currentTimeMillis();
        System.out.println("Total time: " + (endTime - startTime));
    }

    @SuppressWarnings("deprecation")
    public static void generateCentroid(Configuration conf, Path center, FileSystem fs, int numCentroids,
                                        double maxLat, double minLat, double maxLong, double minLong) throws IOException {
        try (SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center, Centroid.class,
                IntWritable.class)) {
            for (int i = 0; i < numCentroids; i++) {
                final IntWritable value = new IntWritable(0);

                double lat = ThreadLocalRandom.current().nextDouble(minLat, maxLat);
                double lon = ThreadLocalRandom.current().nextDouble(minLong, maxLong);
                centerWriter.append(new Centroid(new DataPoint(lat, lon)), value);
                System.out.println("Random centroid: " + lat + ", " + lon);
            }
        }
    }

    @SuppressWarnings("deprecation")
    public static void generateDefaultCentroid(Configuration conf, Path center, FileSystem fs, int numCentroids) throws IOException {
        List<Centroid> centers = new ArrayList<>();
        centers.add(new Centroid(new DataPoint(40.85735017051131, -73.97583520817525)));
        centers.add(new Centroid(new DataPoint(40.68861655424816, -73.96573858495647)));
        centers.add(new Centroid(new DataPoint(40.80788839326028, -73.85133394272049)));
        centers.add(new Centroid(new DataPoint(40.73431922011856, -73.97139306295887)));
        centers.add(new Centroid(new DataPoint(40.83950177827124, -73.93830123848582)));
        centers.add(new Centroid(new DataPoint(40.66835303047305, -73.95603291052532)));
        centers.add(new Centroid(new DataPoint(40.70593518625388, -73.8174453420053)));
        centers.add(new Centroid(new DataPoint(40.69313611161207, -74.00557179477212)));
        centers.add(new Centroid(new DataPoint(40.733512456740705, -74.00015919755681)));
        centers.add(new Centroid(new DataPoint(40.64981051798551, -73.825900619623)));
        centers.add(new Centroid(new DataPoint(40.66875092115764, -73.78788263127616)));
        centers.add(new Centroid(new DataPoint(40.71333159456563, -73.843830939004)));
        centers.add(new Centroid(new DataPoint(40.7987311948879, -73.92969174515021)));
        centers.add(new Centroid(new DataPoint(40.79733248094382, -73.82423064039747)));
        centers.add(new Centroid(new DataPoint(40.83338254534093, -73.86255561092706)));
        centers.add(new Centroid(new DataPoint(40.765830481760105, -74.0045637756593)));
        centers.add(new Centroid(new DataPoint(40.66128235833121, -73.84216487335976)));
        centers.add(new Centroid(new DataPoint(40.67297350126494, -73.98181949460644)));
        centers.add(new Centroid(new DataPoint(40.74351955977953, -73.86371431396884)));
        centers.add(new Centroid(new DataPoint(40.72774944929018, -73.97733413739687)));
        centers.add(new Centroid(new DataPoint(40.78587919811453, -73.87216742399652)));
        centers.add(new Centroid(new DataPoint(40.70825361486318, -73.77281441638279)));
        centers.add(new Centroid(new DataPoint(40.69285940887492, -73.9294412351402)));
        centers.add(new Centroid(new DataPoint(40.71586668557954, -73.77946234926674)));
        centers.add(new Centroid(new DataPoint(40.8215990386083, -73.98279179801291)));
        centers.add(new Centroid(new DataPoint(40.79100734092063, -73.92828585102298)));
        centers.add(new Centroid(new DataPoint(40.855373312023914, -73.9372727833193)));
        centers.add(new Centroid(new DataPoint(40.695195104071416, -73.9785846269713)));
        centers.add(new Centroid(new DataPoint(40.72508133054938, -73.91170452722731)));
        centers.add(new Centroid(new DataPoint(40.74869699796766, -73.77021806769295)));
        centers.add(new Centroid(new DataPoint(40.80916041668119, -74.0074764797616)));
        centers.add(new Centroid(new DataPoint(40.647687570357505, -73.77733715364666)));
        centers.add(new Centroid(new DataPoint(40.77375147934253, -73.95588337370204)));
        centers.add(new Centroid(new DataPoint(40.82071280668222, -73.83082074358332)));
        centers.add(new Centroid(new DataPoint(40.8275987107237, -73.99338057070744)));
        centers.add(new Centroid(new DataPoint(40.85028127560577, -73.85560762056159)));
        centers.add(new Centroid(new DataPoint(40.840376487812364, -73.942041545382)));
        centers.add(new Centroid(new DataPoint(40.66977457237654, -73.81022432674847)));
        centers.add(new Centroid(new DataPoint(40.68290006294763, -73.95729128680429)));
        centers.add(new Centroid(new DataPoint(40.78019888633854, -73.83765755028243)));
        centers.add(new Centroid(new DataPoint(40.77841614817231, -73.96705466440804)));
        centers.add(new Centroid(new DataPoint(40.74229328611358, -73.89733032555753)));
        centers.add(new Centroid(new DataPoint(40.67128568660068, -73.90455915663779)));
        centers.add(new Centroid(new DataPoint(40.83130153939308, -73.85342255369862)));
        centers.add(new Centroid(new DataPoint(40.86058153196138, -73.77836573670304)));
        centers.add(new Centroid(new DataPoint(40.71732812292367, -73.81829914115383)));
        centers.add(new Centroid(new DataPoint(40.797558074239916, -73.90669273782882)));
        centers.add(new Centroid(new DataPoint(40.85863609269721, -73.86320126465303)));
        centers.add(new Centroid(new DataPoint(40.85778977479922, -73.76044105675624)));
        centers.add(new Centroid(new DataPoint(40.675789742452466, -73.77029478324643)));

        try (SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center, Centroid.class,
                IntWritable.class)) {
            for (int i = 0; i < numCentroids; i++) {
                final IntWritable value = new IntWritable(i);

                centerWriter.append(centers.get(i), value);
            }
        }
    }

}
