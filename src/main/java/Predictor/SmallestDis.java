package Predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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

public class SmallestDis implements Predictor
{
    public static class LogTextOutputFormat extends TextOutputFormat<Text, Text>
    {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) {
            return new Path(getOutputName(context)+".txt");
        }

    }
    public static class SmallestDisMapper extends Mapper<LongWritable, Text, Text, Text> {
        private MultipleOutputs<Text, Text> mos;
        private String outputPath;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            outputPath = conf.get("outputPath");
            mos = new MultipleOutputs<Text, Text>(context);       //初始化mos
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String interfaceInfo = fileSplit.getPath().getName();    //得到文件名
            interfaceInfo = interfaceInfo.replace(".txt", "");  //去掉.txt.segmented
            String[] line = value.toString().split("\t");
            if (line.length == 15) {
                int min=Integer.MAX_VALUE;
                int max = Integer.MIN_VALUE;
                for (int i = 1; i <= 14; i++) {
                    if(Integer.parseInt(line[i])>max)
                        max=Integer.parseInt(line[i]);
                    else if(Integer.parseInt(line[i])<min)
                        min=Integer.parseInt(line[i]);
                }
                double minDis=Double.MAX_VALUE;
                int minidx=-1;
                for (int result = min; result <= max; result++) {
                    double dis = 0;
                    for (int i = 1; i <= 14; i++)
                    {
                        dis += Math.abs(Integer.parseInt(line[i])-result);
                    }
                    if (dis<minDis)
                    {
                        minDis = dis;
                        minidx =result;
                    }

                }
                mos.write("SmallestDis", new Text(line[0]), new Text(minidx+""), outputPath + "/" + interfaceInfo);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
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
            Job job = Job.getInstance(conf,"SmallestDis");

            job.setJarByClass(SmallestDis.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setMapperClass(SmallestDis.SmallestDisMapper.class);

            fileSystem = FileSystem.get(conf);
            FileStatus[] fileStatusArray = fileSystem.globStatus(new Path(args[0]+"/*.txt"));
            for(FileStatus fileStatus : fileStatusArray){
                path = fileStatus.getPath();
                FileInputFormat.addInputPath(job, path);
            }
            MultipleOutputs.addNamedOutput(job, "SmallestDis", LogTextOutputFormat.class, Text.class, Text.class);
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
