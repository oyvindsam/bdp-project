package au.edu.rmit.bdp.clustering.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import au.edu.rmit.bdp.clustering.model.Centroid;
import au.edu.rmit.bdp.clustering.model.CentArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * calculate a new centroid for these vertices
 */
public class KMeansReducer extends Reducer<IntWritable, CentArrayWritable, Centroid, IntWritable> {

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
	 * @param clusterIndex 		key
	 * @param values	value: a list of dataPoints associated with the key (dataPoints in this cluster)
	 */
	@Override
	protected void reduce(IntWritable clusterIndex, Iterable<CentArrayWritable> values, Context context) throws IOException,
			InterruptedException {
		System.out.println("clusterIndex: " + clusterIndex);

		Centroid oldCent = null;
		List<Centroid> centroids = new ArrayList<>();
		for (ArrayWritable val : values) {
			Writable[] writables = val.get();

			if (oldCent == null) oldCent = (Centroid) writables[0];
			centroids.add((Centroid) writables[1]);
		}
		System.out.println("OldCentroid: " + oldCent.getCenterVector());
		if (centroids.size() < 1) return;
		Centroid newCenter = centroids.get(0);
		for (int i = 1; i < centroids.size(); i++) {
			newCenter.plus(centroids.get(i));
		}
		newCenter.divideByK();

		System.out.println("adding newCenter: " + newCenter.getCenterVector());
		centers.add(new Centroid(newCenter.getCenterVector()));
		// neccesary?
		context.write(newCenter, clusterIndex);

		if (newCenter.update(oldCent)) {
			System.out.println("CONVERGED---------------");
			context.getCounter(Counter.CONVERGED).increment(1);
		}

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


			//todo: serialize updated centroids. as <centroid, intWritable>
			for (Centroid centroid : centers) {
				System.out.println("Writing centroid: " + centroid.getCenterVector() + " with index " + centroid.getClusterIndex());
				final IntWritable value = new IntWritable(centroid.getClusterIndex());
				out.append(new Centroid(centroid.getCenterVector()), value);
			}

		}
		System.out.println("Finsisehd reducing!-------");
	}
}
