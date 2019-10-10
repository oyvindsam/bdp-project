package au.edu.rmit.bdp.clustering.mapreduce;

import au.edu.rmit.bdp.clustering.model.Centroid;
import au.edu.rmit.bdp.clustering.model.DataPoint;
import de.jungblut.math.DoubleVector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansCombiner extends Reducer<IntWritable, Centroid, IntWritable, Centroid> {

    // do some hacky stuff,
    @Override
    protected void reduce(IntWritable centroidId, Iterable<Centroid> dataPoints, Context context) throws IOException, InterruptedException {
        System.out.println("Combiner called");
        // accumulate all dataPoints values
        Centroid newCentroid = new Centroid();
        newCentroid.setClusterIndex(centroidId.get());
        for (Centroid value : dataPoints) {
            // plus method also logs number of times values has been added to this centroid
            newCentroid.plus(value.getCenterVector());
        }

        context.write(centroidId, newCentroid);

    }
}
