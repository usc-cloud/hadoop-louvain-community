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
 * Created by charith on 6/28/14.
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
