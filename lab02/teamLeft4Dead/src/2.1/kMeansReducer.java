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

public class kMeansReducer extends Reducer<IntWritable, Text, Text, Text> {
    private final Text centroidValue = new Text();
    private final Text centroidId = new Text();
    
    @Override
    protected void reduce(IntWritable centroid_id, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Point avg = new Point(0, 0);
        for (Text val_str : values) {
            String[] val_strs = val_str.toString().split(" ", 2);
            Point temp = new Point(val_strs);
            //System.err.println(val.getString(' '));
            count ++;
            avg.add(temp);
        }
        avg.divide(count);

        centroidValue.set(avg.getString(' '));
        centroidId.set(String.valueOf(centroid_id));
        context.write(centroidId, centroidValue);
    }
}
