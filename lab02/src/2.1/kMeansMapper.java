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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class kMeansMapper extends Mapper<Object, Text, IntWritable, Point> {
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString()," \t\n");

        // read the centroids from context
        int k_clusters = Integer.parseInt(context.getConfiguration().get("k_cluster"));
        Point[] centroids = new Point[k_clusters];
        for(int i=0; i<k_clusters; i++){
            String[] centroid = context.getConfiguration().get("centroid"+i).split(" ", 2);
            centroids[i] = new Point(Float.parseFloat(centroid[0]), Float.parseFloat(centroid[1]));
        }

        // categorize data points
        while(tokenizer.hasMoreTokens()){
            float x = Float.parseFloat(tokenizer.nextToken());
            float y = Float.parseFloat(tokenizer.nextToken());

            Point p = new Point(x, y);
            int min_index = 0;
            float min_dis = p.distance_sqr(centroids[0]);
            for(int i=1; i<k_clusters; i++){
                float temp = p.distance_sqr(centroids[i]);
                if(temp < min_dis){
                    min_dis = temp;
                    min_index = i;
                }
            }

            context.write(new IntWritable(min_index), p);
        }
    }
}