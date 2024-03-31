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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class kMeansMain {
    public static Point[] initCentroids(int k) throws Exception {
        Point[] centroids = new Point[k];
        for(int i=0; i<k; i++){
            Point randPoint_center = new Point(0, 0);
            boolean dup;
            do{
                dup = false;
                centroids[i] = Point.randomPoint_inCircle(randPoint_center, 40);
                for(int j=0; j<i; j++){
                    if(centroids[i].equal(centroids[j])){
                        dup = true;
                        break;
                    }
                }
            }while(dup);
        }
        return centroids;
    }
    
    private static Point[] readCentroids(Configuration conf, int k, String path)
      throws IOException, FileNotFoundException {

        // note: there's no guarantee that the number of centroids output is the same as k

        Point[] centroids = new Point[k];
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] status = hdfs.listStatus(new Path(path));

        // fill
        for(int i=0; i<k; i++){
            String[] conf_cen = conf.get("centroid"+i).split(" ", 2);
            centroids[i] = new Point(Float.parseFloat(conf_cen[0]), Float.parseFloat(conf_cen[1]));
        }

        // actual read
        if(!status[0].getPath().toString().endsWith("_SUCCESS")){
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(status[0].getPath())));
            
            for(int j=0; j<k; j++){
                // each line is in : "[index]\t[x] [y]"
                String[] valStrings = br.readLine().split(" \t");
                int centroidId = Integer.parseInt(valStrings[0]);
                Point point = new Point(Float.parseFloat(valStrings[1]), Float.parseFloat(valStrings[2]));
                    
                centroids[centroidId] = Point.copy(point);
            }

            br.close();
        }
        return centroids;
    }

    private static void writeOutput_centroids(Configuration conf, int k, Point[] centroids, String path){
        try{
            FileSystem hdfs = FileSystem.get(conf);
            FSDataOutputStream out = hdfs.create(new Path(path), true);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
            for(int i=0; i<k; i++){
                bw.write(centroids[i].getString(' ') + "\n");
            }
            bw.close();
        }
        catch(IOException e){
            System.err.println(e);
        }
    }

    public static void main(String[] args) throws Exception {
        // The logic here is kinda simple:
        // 0. Input from args: input_dir, output_dir, k_cluster
        // 1. Initialize centroids and save them to conf 
        // (the only way to pass between nodes without having to touch the HDFS, I tried that and it was a pain).
        // 2. Mapper reads the centroids from conf, and datapoints from input.
        // 3. Mapper writes context: <key = centroid_index, value = datapoint>
        // 4. Reducer calculates average point of sets of values.
        // 5. Reducer writes context: <key = centroid_index, value = datapoint> to iter output
        // 6. In main: read centroids from output from Reducer, check complete or update.
        // 7. In main: update input and output for iter
        // 8. Repeat step 2-7 till... you already know.
        // configurations
        // step 0
        if(args.length != 4){
            System.exit(1);
        }

        int iter_cap = Integer.parseInt(args[3]);
        int k_cluster = Integer.parseInt(args[2]);
        String input_dir = args[0];
        String output_dir = args[1];

        // step 1
        Point[] centroids = initCentroids(k_cluster);
        Configuration conf = new Configuration();
        conf.set("k_cluster", String.valueOf(k_cluster));

        for(int i=0; i<k_cluster; i++){
            conf.set("centroid"+i, centroids[i].getString(' '));
            //System.err.println(centroids[i].getString(' '));
        }

        // step 2-8
        String output_iter = "/temp/iter_0";
        for(int iter=0; iter < iter_cap; iter++){
            Job job = Job.getInstance(conf, "output_iter");
            
            job.setJarByClass(kMeansMain.class);
            job.setMapperClass(kMeansMapper.class);
            //job.setCombinerClass(kMeansReducer.class);
            job.setReducerClass(kMeansReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(input_dir));
            FileOutputFormat.setOutputPath(job, new Path(output_iter));

            if(!job.waitForCompletion(true)){
                System.err.println("Exit at iter "+iter+ ".\n");
                System.exit(1);
            }

            // step 6
            Point[] new_centroids = readCentroids(conf, k_cluster, output_iter);

            boolean complete = true;
            for(int j=0; j<k_cluster; j++){
                if(!centroids[j].equal(new_centroids[j])){
                    complete = false;
                    break;
                }
            }

            if(complete){
                break;
            }
            else{
                for(int j=0; j<k_cluster; j++){
                    centroids[j] = Point.copy(new_centroids[j]);
                    conf.unset("centroid"+j);
                    conf.set("centroid"+j, centroids[j].getString(' '));
                }
            }

            // step 7
            output_iter = "/temp/iter_" + String.valueOf(iter+1);
        }   


        // final job to write class assignments
        Job job_final = Job.getInstance(conf, output_dir);
            
        job_final.setJarByClass(kMeansMain.class);
        job_final.setMapperClass(kMeansMapper.class);
        job_final.setReducerClass(kMeansFinal.class);

        job_final.setOutputKeyClass(IntWritable.class);
        job_final.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job_final, new Path(input_dir));
        FileOutputFormat.setOutputPath(job_final, new Path(output_dir));

        if(!job_final.waitForCompletion(true)){
            System.err.println("Exit at final iter.\n");
            System.exit(1);
        }

        // write cluster centers
        writeOutput_centroids(conf, k_cluster, centroids, output_dir+"/task_2_1.clusters");

        System.exit(0);
    }
}