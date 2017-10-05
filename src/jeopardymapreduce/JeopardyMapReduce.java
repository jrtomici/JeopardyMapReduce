package jeopardymapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JeopardyMapReduce {

    public static class SplitterMapper extends Mapper<Object, Text, Text, IntWritable> {

        private IntWritable val = new IntWritable();
        private Text yearText = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String line = itr.nextToken();
                List<String> items = Arrays.asList(line.split("\\s*,\\s*"));
                for (int i = 0; i < items.size() - 1; i++) {
                    if (items.get(i).length() == 10) {
                        String pointsString = items.get(i + 1);
                        if (!pointsString.equalsIgnoreCase("none")) {
                            int points = Integer.parseInt(pointsString);
                            if (points != 200 && points != 400 && points != 600 && points != 800 && points != 1000 && points != 1200 && points != 1600 && points != 2000) {
                                char[] dateChars = items.get(i).toCharArray();
                                String year = "" + dateChars[0] + dateChars[1] + dateChars[2] + dateChars[3];
                                System.out.println(year + ", " + points);
                                yearText.set(year);
                                val.set(points);
                                context.write(yearText, val);
                            }
                        }
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0, count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            int avg = sum / count;
            result.set(avg);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(JeopardyMapReduce.class);
        job.setMapperClass(SplitterMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}