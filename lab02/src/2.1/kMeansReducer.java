import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Random;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class kMeansReducer extends Reducer<IntWritable, Point, IntWritable, Text> {
    private final Text centroidValue = new Text();
    
    @Override
    protected void reduce(IntWritable centroid_id, Iterable<Point> values, Context context) throws IOException, InterruptedException {
        // read the centroids from context
        Point[] centroids;
        int k_clusters = Integer.parseInt(context.getConfiguraion().get("k_clusters"));
        for(int i=0; i<k_clusters; i++){
            String[] centroid = context.getConfiguraion().getStrings("centroid"+i);
            centroids[i] = new Point(centroid);
        }
        
        int count = 0;
        Point avg = new Point(0, 0);
        for (Point val : values) {
            count ++;
            avg.add(val);
        }
        avg.divide(count);

        centroidValue.set(avg.getString(' '));
        context.write(centroid_id, centroidValue);
    }
}
