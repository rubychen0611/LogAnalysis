package Predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;

public class MovingAverage implements Predictor
{
    public static class LogTextOutputFormat extends TextOutputFormat<Text, Text>
    {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) {
            return new Path(getOutputName(context)+".txt");
        }

    }
    public static class MovingAverageMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        private static int k = 1;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String interfaceInfo = fileSplit.getPath().getName();    //得到文件名
            interfaceInfo = interfaceInfo.replace(".txt", "");  //去掉.txt.segmented
            String[] line = value.toString().split("\t");
            if(line.length == 15)
            {
                int sum = 0;
                for(int i = 14-k+1; i <= 14; i++)
                {
                    sum += Integer.parseInt(line[i]);
                }
                double avg = (double)sum / k;
                context.write(new Text(interfaceInfo+"_"+line[0]),new Text(String.format("%.2f", avg)));
            }
        }
    }
    public static class MovingAverageReducer extends Reducer<Text, Text, Text, Text>
    {
        private MultipleOutputs<Text,Text> mos;
        private String outputPath;
        @Override
        public void setup(Context context)
        {
            Configuration conf = context.getConfiguration();
            outputPath = conf.get("outputPath");
            mos = new MultipleOutputs<Text, Text>(context);       //初始化mos
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            String keyInfo[] = key.toString().split("_");
            mos.write("MovingAverage", new Text(keyInfo[1]),values.iterator().next(),outputPath+"/"+keyInfo[0]);
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            mos.close();
        }
    }
    public int predict(String[] args)
    {
        try{
            // 若输出目录存在,则删除
            Path path = new Path(args[1]);
            FileSystem fileSystem = FileSystem.get(new URI(args[1]), new Configuration());
            if (fileSystem.exists(path))
                fileSystem.delete(path, true);

            Configuration conf = new Configuration();
            conf.set("outputPath", args[1]);
            Job job = Job.getInstance(conf,"MovingAverage");


            job.setJarByClass(MovingAverage.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setMapperClass(MovingAverage.MovingAverageMapper.class);
            job.setReducerClass(MovingAverage.MovingAverageReducer.class);

            fileSystem = FileSystem.get(conf);
            FileStatus[] fileStatusArray = fileSystem.globStatus(new Path(args[0]+"/*.txt"));
            for(FileStatus fileStatus : fileStatusArray){
                path = fileStatus.getPath();
                FileInputFormat.addInputPath(job, path);
            }
            MultipleOutputs.addNamedOutput(job, "MovingAverage", LogTextOutputFormat.class, Text.class, Text.class);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            LazyOutputFormat.setOutputFormatClass(job, LogTextOutputFormat.class);

            job.waitForCompletion(true);
            return 0;

        }catch (Exception e)
        {
            e.printStackTrace();
            return -1;
        }
    }
}
