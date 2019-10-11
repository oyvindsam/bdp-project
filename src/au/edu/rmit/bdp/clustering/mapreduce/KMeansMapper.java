package au.edu.rmit.bdp.clustering.mapreduce;

import au.edu.rmit.bdp.clustering.model.Centroid;
import au.edu.rmit.bdp.clustering.model.DataPoint;
import au.edu.rmit.bdp.clustering.model.DpArrayWritable;
import au.edu.rmit.bdp.distance.CosineDistance;
import au.edu.rmit.bdp.distance.DistanceMeasurer;
import de.jungblut.math.DoubleVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * First generic specifies the type of input Key.
 * Second generic specifies the type of input Value.
 * Third generic specifies the type of output Key.
 * Last generic specifies the type of output Value.
 * In this case, the input key-value pair has the same type with the output one.
 *
 * The difference is that the association between a centroid and a data-point may change.
 * This is because the centroids has been recomputed in previous reduce().
 */
public class KMeansMapper extends Mapper<Centroid, DataPoint, IntWritable, DpArrayWritable> {

	//private final List<Centroid> centers = new ArrayList<>();
	private DistanceMeasurer distanceMeasurer;

	// Used for in map combining. Based on cluseter index -> data points
	private final Map<Centroid, List<DataPoint>> map = new HashMap<>();

	/**
	 *
	 * In this method, all centroids are loaded into memory as, in map(), we are going to compute the distance
	 * (similarity) of the data point with all centroids and associate the data point with its nearest centroid.
	 * Note that we load the centroid file on our own, which is not the same file as the one that hadoop loads in map().
	 *
	 *
	 * @param context Think of it as a shared data bundle between the main class, mapper class and the reducer class.
	 *                One can put something into the bundle in KMeansClusteringJob.class and retrieve it from there.
	 *
	 */
	@SuppressWarnings("deprecation")
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		//System.out.println("\n---------- Map setup called");
		// We get the URI to the centroid file on hadoop file system (not local fs!).
		// The url is set beforehand in KMeansClusteringJob#main.
		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);

		// After having the location of the file containing all centroids data,
		// we read them using SequenceFile.Reader, which is another API provided by hadoop for reading binary file
		// The data is modeled in Centroid.class and stored in global variable centers, which will be used in map()
		try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, conf)) {
			Centroid key = new Centroid();
			IntWritable value = new IntWritable();
			int index = 0;
			while (reader.next(key, value)) {
				Centroid centroid = new Centroid(key);
				centroid.setClusterIndex(index++);
				//System.out.println("Centroid data loaded: vector: " + centroid.getCenterVector() + ", index: " + centroid.getClusterIndex());
				map.put(centroid, new ArrayList<>());
			}
		}
		// This is for calculating the distance between a point and another (centroid is essentially a point).
		distanceMeasurer = new CosineDistance();
	}

	/**
	 *
	 * After everything is ready, we calculate and re-group each data-point with its nearest centroid,
	 * and pass the pair to reducer.
	 *
	 * @param centroid key
	 * @param dataPoint value
	 */
	@Override
	protected void map(Centroid centroid, DataPoint dataPoint, Context context) throws IOException,
			InterruptedException {
		//System.out.println("centroid id : " + centroid.getClusterIndex() + " centroid: " + centroid.getCenterVector() + " dp: " + dataPoint.getVector());

		Centroid nearest = null;
		double nearestDistance = Double.MAX_VALUE;
		DoubleVector dataVector = dataPoint.getVector();
		for (Centroid c : map.keySet()) {
			//System.out.println("\nChecking centroid:        " + c.getCenterVector() + ", distance: " + distanceMeasurer.measureDistance(dataVector, c.getCenterVector()));
			//todo: find the nearest centroid for the current dataPoint, pass the pair to reducer
			if (nearestDistance > distanceMeasurer.measureDistance(dataVector, c.getCenterVector())) {
				nearest = c;

				//System.out.println("---- Nearest centroid vector: " + c.getCenterVector() + " for dataPoint: " + dataPoint.getVector());
				nearestDistance = distanceMeasurer.measureDistance(dataVector, c.getCenterVector());
			}
		}
		List<DataPoint> lst = map.get(nearest);
		lst.add(new DataPoint(dataPoint));  // kinda important not to add the reference to the same dataPoint... queue 2 hours debugging.
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
/*		map.forEach((key, value) -> {
			System.out.println("key value : " + key +" / " + value);
		});*/
		for (Map.Entry<Centroid, List<DataPoint>> entry : map.entrySet()) {
			Centroid centroid = entry.getKey();
			List<DataPoint> dataPoints = entry.getValue();

			if (dataPoints.size() == 0) continue;  // do not write empty dataPoint list to disk

			DataPoint[] points = new DataPoint[dataPoints.size() + 1];  // first field for centroid
			points[0] = new DataPoint(centroid); // the first item in the list is the real centroid, rest is dataPoints. Hey im lazy
			for (int i = 0; i < dataPoints.size(); i++) {
				points[i+1] = dataPoints.get(i);
			}

			DpArrayWritable arrayWritable = new DpArrayWritable(points);
			context.write(new IntWritable(centroid.getClusterIndex()), arrayWritable);

		}
	}
}
