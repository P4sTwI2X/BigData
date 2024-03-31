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

public class FilterFrequency {

    public static class FilterFrequencyMapper extends Mapper<Object, Text, Text, Text> {

        public Text fileKey = new Text();
        public Text fileValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer iter = new StringTokenizer(value.toString());
            while (iter.hasMoreTokens()) {
                String termID = iter.nextToken();
                String docID = iter.nextToken();
                Float freq = Float.parseFloat(iter.nextToken());

                fileKey.set(termID); 
                fileValue.set(docID + " " + freq); 
                context.write(fileKey, fileValue);
            }
            // if (fields.length == 3) {
            //     int id = Integer.parseInt(fields[0]);
            //     int frequency = Integer.parseInt(fields[1]);
            //     double frequency = Double.parseDouble(fields[2]);
            //     if (highFrequencyIds.contains(id) && frequency >= 3) {
            //         context.write(value, new IntWritable(frequency));
            //     }
            // }
        }
    }

    public static class FilterFrequencyReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();
        Map<String, Float> freqMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (Text value : values) {
                String[] splitMap = value.toString().split(" ");
                String docID = splitMap[0];
                Float freq = Float.parseFloat(splitMap[1]);
                freqMap.put(docID, freq);
                sum += freq;
            }

            if (sum >= 3.0) {
                // freqMap.entrySet().stream().forEach(entry -> context.write(key, result.set(entry.getKey() + " " + entry.getValue())));
                for (Map.Entry<String, Float> entry : freqMap.entrySet()) {
                    result.set(entry.getKey() + " " + entry.getValue());
                    context.write(key, result);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FilterFrequency");
        job.setJarByClass(FilterFrequency.class);
        job.setMapperClass(FilterFrequencyMapper.class);
        job.setReducerClass(FilterFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}