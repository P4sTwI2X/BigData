import java.io.IOException;
import java.io.InputStream;
import java.util.StringTokenizer;
import java.io.FileReader;   //Import classes for handling file I/O
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.io.BufferedReader;
import java.io.File;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.Optional;
import java.io.PrintWriter;
import com.google.common.io.Files;
import com.google.re2j.Pattern;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.jline.utils.Log;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.kerby.asn1.util.IOUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

public class App 
{
    public static class MapTokenizer
        extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();
        private Set<String> stopWords = new HashSet<String>();
        private boolean caseSensitive = false;
        private String input; //Input File
        private final static IntWritable one = new IntWritable(1);
        private final static Pattern WORD = Pattern.compile("\\s*\\b\\s*");
        

        public static Path getPath(InputSplit split) {
            return getFileSplit(split).map(FileSplit::getPath).orElseThrow(() -> 
                new AssertionError("cannot find path from split " + split.getClass()));
        }

        public static Optional<FileSplit> getFileSplit(InputSplit split) {
            if (split instanceof FileSplit) {
                return Optional.of((FileSplit)split);
            } else if (TaggedInputSplit.clazz.isInstance(split)) {
                return getFileSplit(TaggedInputSplit.getInputSplit(split));
            } else {
                return Optional.empty();
            }
        }

        private static final class TaggedInputSplit {
            private static final Class<?> clazz;
            private static final MethodHandle method;

            static {
                try {
                    clazz = Class.forName("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit");
                    Method m = clazz.getDeclaredMethod("getInputSplit");
                    m.setAccessible(true);
                    method = MethodHandles.lookup().unreflect(m).asType(
                        MethodType.methodType(InputSplit.class, InputSplit.class));
                } catch (ReflectiveOperationException e) {
                    throw new AssertionError(e);
                }
            }

            static InputSplit getInputSplit(InputSplit o) {
                try {
                    return (InputSplit) method.invokeExact(o);
                } catch (Throwable e) {
                    throw new AssertionError(e);
                }
            }
        }
        
        //skipping file
        private void parseSkipFile(URI patternURI) {
            Log.info("Added file to cache: " + patternURI);
            try {
                BufferedReader fis = new BufferedReader(new FileReader(new File(patternURI.getPath()).getName()));
                String pattern;
                while((pattern = fis.readLine()) != null) {
                    stopWords.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("ERROR: Something is wrong with " + patternURI);
            }
        }

        //setting up
        protected void setup(Context context) throws IOException, InterruptedException{
            //multiple file reading handling
            if(context.getInputSplit() instanceof FileSplit) {
                this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
            }
            else {
                this.input = context.getInputSplit().toString();
            }

            Configuration config = context.getConfiguration();
            this.caseSensitive = config.getBoolean("App.case.sensitive", false);

            //read stopwords file and store it into hash set
            if(config.getBoolean("App.skip.patterns", false)) {
                URI localPaths[] = context.getCacheFiles();
                parseSkipFile(localPaths[0]);
            }
        }   

        //this will return an array of string
        public String[] splitSpecialCharacter(String word) {
            // Pattern pattern = Pattern.compile("(?<!\\\\w)\\\\W");
            String[] words = word.split("(?<=\\w)(?=\\W)|(?<=\\W)(?=\\w)");
            return words;
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            Path path = getPath(context.getInputSplit());
            String[] splittedPath = path.toString().split("/");

            String fileName = splittedPath[splittedPath.length - 2];
            while(itr.hasMoreTokens()) {
                String w = itr.nextToken();
                if(!fileName.equals("stopwords.txt"))
                {
                    String[] splitted_words = splitSpecialCharacter(w);

                    for (String val: splitted_words) {
                        if(!stopWords.contains(val) ) {
                            word.set(val + "\t" + fileName);
                            context.write(word, one);
                        }
                    }
                }
            }
        }
    }

    public static class MyReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "App");

        //configure output file name
        job.getConfiguration().set("mapreduce.output.basename", "task_1_1");

        for(int i = 0; i < args.length; i++) {
            if("-skip".equals(args[i])) {
                job.getConfiguration().setBoolean("App.skip.patterns", true);
                i += 1;
                job.addCacheFile(new Path(args[i]).toUri());
                Log.info("Added file to cache: " + args[i]);
            }
        }
        job.setJarByClass(App.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0] + "/*/*/"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MapTokenizer.class);
        job.setReducerClass(MyReducer.class);
        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }
}
