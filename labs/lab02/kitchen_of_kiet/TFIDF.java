import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.Set;
import java.util.*;

public class TFIDF {
    
    public static class TFIDFMapper extends Mapper<Object, Text, Text, Text> {

        public Text fileKey = new Text();
        public Text fileValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer iter = new StringTokenizer(value.toString());
            while (iter.hasMoreTokens()) {
                String termID = iter.nextToken();
                String docID = iter.nextToken();
                String freq = iter.nextToken();
                fileKey.set(docID); 
                fileValue.set(termID + " " + freq); 
                context.write(fileKey, fileValue);
            }
        }
    }

    public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
        
        // private TreeMap<Integer, String> word_list = new TreeMap<Integer, String>();
        public Text result = new Text();
        public Map<String, Float> freqMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            int sum = 0;

            for (Text value : values) {
                String[] splitMap = value.toString().split(" ");
                String termID = splitMap[0];
                Float freq = Float.parseFloat(splitMap[1]);
                freqMap.put(termID, freq);
                sum += freq;
            }

                // freqMap.entrySet().stream().forEach(entry -> context.write(key, result.set(entry.getKey() + " " + entry.getValue())));
            for (Map.Entry<String, Float> entry : freqMap.entrySet()) {
                result.set(entry.getKey() + " " + entry.getValue() + " " + sum);
                context.write(key, result);
            }
        }

        // @Override
        // protected void cleanup(Context context) throws IOException, InterruptedException {

        //     for (Map.Entry<Integer, String> entry : word_list.entrySet()) {
        //         String word = entry.getValue();
        //         Integer wordcount = (int)Math.floor(entry.getKey());
        //         String str_wordcount = wordcount.toString();
        //         context.write(new Text(str_wordcount), new Text(word));
        //     }
        // }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TFIDF");
        job.setJarByClass(TFIDF.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
