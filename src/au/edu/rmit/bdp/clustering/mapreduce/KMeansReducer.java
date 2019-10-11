package au.edu.rmit.bdp.clustering.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import au.edu.rmit.bdp.clustering.model.Centroid;
import au.edu.rmit.bdp.clustering.model.DpArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import au.edu.rmit.bdp.clustering.model.DataPoint;

/**
 * calculate a new centroid for these vertices
 */
public class KMeansReducer extends Reducer<IntWritable, DpArrayWritable, Centroid, DataPoint> {

	/**
	 * A flag indicates if the clustering converges.
	 */
	public static enum Counter {
		CONVERGED
	}

	private final List<Centroid> centers = new ArrayList<>();

	/**
	 * Having had all the dataPoints, we recompute the centroid and see if it converges by comparing previous centroid (key) with the new one.
	 *
	 * @param centroid 		key
	 * @param values	value: a list of dataPoints associated with the key (dataPoints in this cluster)
	 */
	@Override
	protected void reduce(IntWritable clusterIndex, Iterable<DpArrayWritable> values, Context context) throws IOException,
			InterruptedException {
		//System.out.println("clusterIndex: " + clusterIndex);
		List<DataPoint> vectorList = new ArrayList<>();

		// old centroid
		Centroid centroid = null;
		// compute the new centroid
		Centroid newCenter = null;

		for (DpArrayWritable dpArray : values) {
			centroid = null;  // always extract the first value from the (sub) list, since it's the centroid
			for (Writable writable : dpArray.get()) {
				if (centroid == null) {
					centroid = new Centroid(((DataPoint) writable).getVector());  // first element
					//System.out.println("Centroid read: " + centroid.getCenterVector());
				} else {
					DataPoint dp = (DataPoint) writable;
					//System.out.println("DataPoint read: " + dp.getVector());

					vectorList.add(dp);

					if (newCenter == null) newCenter = new Centroid(dp);
					else newCenter.plus(dp);

				}
			}
		}
		newCenter.divideByK();
		//System.out.println("New-center: " + newCenter.getCenterVector());

		centers.add(newCenter);

		// write new key-value pairs to disk, which will be fed into next round mapReduce job.
		for (DataPoint vector : vectorList) {
			//System.out.println("-------Key-value [newCentroid, vector]: " + newCentroid.getCenterVector() + " - " + vector.getVector());
			context.write(newCenter, vector);
		}

		// check if all centroids are converged.
		// If all of them are converged, the counter would be zero.
		// If one or more of them are not, the counter would be greater than zero.
		if (newCenter.update(centroid))
			context.getCounter(Counter.CONVERGED).increment(1);
	}

	/**
	 * Write the recomputed centroids to disk.
	 */
	@SuppressWarnings("deprecation")
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		Configuration conf = context.getConfiguration();
		Path outPath = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath, true);

		try (SequenceFile.Writer out = SequenceFile.createWriter(fs, context.getConfiguration(), outPath,
				Centroid.class, IntWritable.class)) {
			final IntWritable value = new IntWritable(0);

			//todo: serialize updated centroids. as <centroid, intWritable>
			for (Centroid centroid : centers) {
				out.append(centroid, value);
			}

		}
		//System.out.println("Finsisehd reducing!-------");
	}
}
