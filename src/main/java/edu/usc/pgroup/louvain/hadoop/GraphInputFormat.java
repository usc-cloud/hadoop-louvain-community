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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by Charith Wickramaarachchi on 6/28/14.
 */
public class GraphInputFormat extends FileInputFormat<Text, BytesWritable> {

    @Override
    protected boolean isSplitable( JobContext context, Path filename )
    {
        return false;
    }

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        GraphPartitionRecordReader reader = new GraphPartitionRecordReader();
        reader.initialize(inputSplit,taskAttemptContext);
        return reader;
    }



}
