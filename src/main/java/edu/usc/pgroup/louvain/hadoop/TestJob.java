/*
 *  Copyright 2013 University of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.package edu.usc.goffish.gopher.sample;
 */
package edu.usc.pgroup.louvain.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.Iterator;

/**
 * Created by Charith Wickramaarachchi on 6/29/14.
 */
public class TestJob {

    public static class MapJob extends Mapper<Text, BytesWritable, Text, Text> {


        private boolean verbose = false;

        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

            InputStream in = new ByteArrayInputStream(value.getBytes());

            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            int linecount = 0;

            String line = reader.readLine();

            while (line != null) {
                linecount++;
                line = reader.readLine();
            }

            System.out.println("**************************************" + linecount);

            context.write(key,new Text("" + linecount));

        }


    }


    public static class ReduceJob extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRr");
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                context.write(key,new IntWritable(Integer.parseInt(iterator.next().toString())));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJobName(TestJob.class.getName());
        job.setJarByClass(TestJob.class);
        job.setMapperClass(MapJob.class);
        job.setReducerClass(ReduceJob.class);

        // Hello there ZipFileInputFormat!
        job.setInputFormatClass(GraphInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));


        job.waitForCompletion(true);



    }

}
