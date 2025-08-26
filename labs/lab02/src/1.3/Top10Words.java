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

public class Top10Words {
    
    public static class Top10WordsMapper extends Mapper<Object, Text, Text, Text> {

        public Text fileKey = new Text();
        public Text fileValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer iter = new StringTokenizer(value.toString());
            while (iter.hasMoreTokens()) {
                String termID = iter.nextToken();
                String docID = iter.nextToken();
                String freq = iter.nextToken();

                fileKey.set(termID); 
                fileValue.set(freq); 
                context.write(fileKey, fileValue);
            }
        }
    }

    public static class Top10WordsReducer extends Reducer<Text, Text, Text, Text> {
        
        private TreeMap<Integer, String> word_list = new TreeMap<Integer, String>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            int sum = 0;
            for (Text value : values) {
                Float f = Float.parseFloat(value.toString().trim());
                sum += f;
            }

            word_list.put(sum, key.toString());

            if (word_list.size() > 10)
                word_list.remove(word_list.firstKey());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (Map.Entry<Integer, String> entry : word_list.entrySet()) {
                String word = entry.getValue();
                Integer wordcount = (int)Math.floor(entry.getKey());
                String str_wordcount = wordcount.toString();
                context.write(new Text(str_wordcount), new Text(word));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top10Words");
        job.setJarByClass(Top10Words.class);
        job.setMapperClass(Top10WordsMapper.class);
        job.setReducerClass(Top10WordsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
