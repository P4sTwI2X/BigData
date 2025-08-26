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

public class TFIDFFinal {
    
    public static class TFIDFFinalMapper extends Mapper<Object, Text, Text, Text> {

        public Text fileKey = new Text();
        public Text fileValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer iter = new StringTokenizer(value.toString());
            while (iter.hasMoreTokens()) {
                String docID = iter.nextToken();
                String termID = iter.nextToken();
                String freq = iter.nextToken();
                String docwords = iter.nextToken();
                fileKey.set(termID); 
                fileValue.set(docID + " " + freq + " " + docwords); 
                context.write(fileKey, fileValue);
            }
        }
    }

    public static class TFIDFFinalReducer extends Reducer<Text, Text, Text, Text> {
        
        // private TreeMap<Integer, String> word_list = new TreeMap<Integer, String>();
        public Text result = new Text();
        public Map<String, String> freqMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            int numDocs = 0;

            for (Text value : values) {
                String[] splitMap = value.toString().split(" ");
                String docID = splitMap[0];
                String freq_docwords = splitMap[1] + " " + splitMap[2];

                freqMap.put(docID, freq_docwords);
                numDocs += 1;
            }

                // freqMap.entrySet().stream().forEach(entry -> context.write(key, result.set(entry.getKey() + " " + entry.getValue())));
            for (Map.Entry<String, String> entry : freqMap.entrySet()) {
                String[] split_freq_docwords = entry.getValue().split(" ");
                Double freq = Double.parseDouble(split_freq_docwords[0]);
                Double docwords = Double.parseDouble(split_freq_docwords[1]);
                Double tf = (freq / docwords);
                Double idf = Math.log(numDocs / docwords);
                Double tfidf = tf * idf;
                String str_tfidf = tfidf.toString();
                result.set(entry.getKey() + " " + str_tfidf);
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
        job.setJarByClass(TFIDFFinal.class);
        job.setMapperClass(TFIDFFinalMapper.class);
        job.setReducerClass(TFIDFFinalReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
