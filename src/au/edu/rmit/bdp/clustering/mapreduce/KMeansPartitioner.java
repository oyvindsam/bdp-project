package au.edu.rmit.bdp.clustering.mapreduce;

import au.edu.rmit.bdp.clustering.model.Centroid;
import au.edu.rmit.bdp.clustering.model.DataPoint;
import org.apache.hadoop.mapreduce.Partitioner;

public class KMeansPartitioner extends Partitioner<Centroid, DataPoint> {

    // Number of partitioner tasks is equal to the number of reduces tasks.
    // for this partition: send all datapoints in a cluster to same reducer
    @Override
    public int getPartition(Centroid centroid, DataPoint dataPoint, int numReduceTasks) {
        return centroid.getClusterIndex();
    }
}
