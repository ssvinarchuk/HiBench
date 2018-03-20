package HiBench;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class StructData {

    private DataOptions options;

    public StructData(DataOptions options) {
        this.options = options;
    }

    public void generate() throws Exception {
        Configuration conf = new Configuration();

        conf.setLong("rowMultiplier", options.getRowMultiplier());

        Job job = new Job(conf, "structGenerator");

        job.setJarByClass(StructData.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(StructDataMapper.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, options.getInputPath());
        FileOutputFormat.setOutputPath(job, options.getResultPath());

        boolean success = job.waitForCompletion(true);
        System.out.println(success);
    }

    public static class StructDataMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        @Override
        public void map(LongWritable key, Text row, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            long rowMultiplier = conf.getLong("rowMultiplier", 1);
            for(int i = 0; i < rowMultiplier; i++) {
                context.write(NullWritable.get(), row);
            }

        }
    }
}
