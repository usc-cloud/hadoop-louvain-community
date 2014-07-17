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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by Charith Wickramaarachchi on 7/16/14.
 */
public class LouvainMR {

    public static final String VERBOSE = "VERBOSE";

    public static final String NB_PASS = "NB_PASS";

    public static final String PRECISION = "PRECISION";

    public static final String DISPLAY_LEVEL = "DISPLAY_LEVEL";


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        int displayLevel = Integer.parseInt(args[2]);

        boolean v = false;
        if(args.length > 3) {
           v = Boolean.parseBoolean(args[3]);
        }

        conf.setInt(DISPLAY_LEVEL,displayLevel);
        conf.setBoolean(VERBOSE,v);

        Job job = new Job(conf);
        job.setJobName(TestJob.class.getName());
        job.setJarByClass(TestJob.class);
        job.setMapperClass(MapCommunity.class);
        job.setReducerClass(ReduceCommunity.class);

        // Hello there ZipFileInputFormat!
        job.setInputFormatClass(GraphInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));


        job.waitForCompletion(true);
    }
}
