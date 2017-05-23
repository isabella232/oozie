package org.apache.oozie.action.hadoop;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class DataLocalityInputFormat extends FileInputFormat<LongWritable, BytesWritable> {

    @Override
    public RecordReader<LongWritable, BytesWritable> getRecordReader(final InputSplit split,
                                                                     final JobConf jobConf,
                                                                     final Reporter reporter) throws IOException {
        return new RecordReader<LongWritable, BytesWritable>() {

            @Override
            public boolean next(final LongWritable key, final BytesWritable value) throws IOException {
                return false;
            }

            @Override
            public LongWritable createKey() {
                return new LongWritable();
            }

            @Override
            public BytesWritable createValue() {
                return new BytesWritable();
            }

            @Override
            public long getPos() throws IOException {
                return 1;
            }

            @Override
            public void close() throws IOException {

            }

            @Override
            public float getProgress() throws IOException {
                return 1.0f;
            }
        };
    }
}
