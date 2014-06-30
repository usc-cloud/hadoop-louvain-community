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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by Charith Wickramaarachchi on 6/28/14.
 */
public class GraphPartitionRecordReader extends RecordReader<Text, BytesWritable> {


    /** Uncompressed file name */
    private Text currentKey;

    private  FileSplit split;

    Configuration conf;

    /** Uncompressed file contents */
    private BytesWritable currentValue = new BytesWritable();

    /** Used to indicate progress */
    private boolean isFinished = false;


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        split = (FileSplit) inputSplit;
        conf = taskAttemptContext.getConfiguration();

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if(isFinished) {
            return false;
        }

        int fileLength = (int)split.getLength();
        byte [] result = new byte[fileLength];



        FileSystem  fs = FileSystem.get(conf);
        Path path = split.getPath();
        currentKey = new Text(path.getName());
        FSDataInputStream in = null;
        try {
            in = fs.open( split.getPath());
            IOUtils.readFully(in, result, 0, fileLength);
            currentValue.set(result, 0, fileLength);

        } finally {
            IOUtils.closeStream(in);
        }

        this.isFinished = true;
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isFinished ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {

    }
}
