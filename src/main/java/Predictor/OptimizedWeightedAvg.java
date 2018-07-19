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

public class OptimizedWeightedAvg implements Predictor
{
    public static class LogTextOutputFormat extends TextOutputFormat<Text, Text>
    {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) {
            return new Path(getOutputName(context)+".txt");
        }

    }
    public static class OptimizedWeightedAvgMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        private MultipleOutputs<Text,Text> mos;
        private String outputPath;
        private static int window = 14;
        public double[][] weight={
                {0.12,0.04,-0.03,0.08,0.00,0.12,-0.00,-0.09,0.02,0.06,0.07,0.17,0.10,0.10,0.10},
                {0.14,-0.04,-0.02,0.18,0.01,0.12,-0.01,-0.09,0.17,-0.05,0.12,0.01,0.12,0.17,0.25},
                {0.07,-0.11,-0.03,0.00,-0.01,0.11,0.12,0.04,0.11,0.05,0.11,0.09,0.10,0.24,0.14},
                {0.12,0.04,0.02,-0.05,-0.02,-0.05,0.12,-0.00,0.11,0.12,0.12,0.09,0.24,0.21,0.05},
                {0.09,-0.04,0.09,0.09,0.01,-0.11,0.02,0.01,0.11,0.20,0.07,0.08,0.12,0.17,0.18},
                {0.04,0.15,-0.04,0.00,-0.08,-0.05,-0.02,-0.04,0.09,0.09,0.15,0.12,0.11,0.18,0.30},
                {0.07,-0.05,0.06,0.02,-0.05,0.03,-0.02,0.09,0.19,0.08,0.08,0.08,0.10,0.19,0.18},
                {0.10,-0.12,0.03,0.04,0.06,-0.03,0.10,0.09,0.08,0.08,0.08,0.13,0.12,0.21,0.16},
                {0.28,0.07,-0.03,0.10,0.04,-0.13,-0.04,0.03,0.12,0.27,0.16,0.27,-0.01,0.11,0.15},
                {0.10,-0.02,0.01,0.15,0.02,-0.11,-0.04,0.01,0.15,0.29,0.16,0.21,-0.11,0.07,0.25},
                {0.13,-0.02,-0.01,0.16,0.02,-0.10,-0.12,0.13,0.15,0.15,0.16,0.22,-0.03,0.01,0.25},
                {0.12,0.04,-0.03,0.02,0.00,-0.07,-0.01,0.05,0.19,0.17,0.18,0.13,-0.05,0.04,0.37},
                {0.09,0.10,-0.12,0.23,0.05,-0.08,-0.01,0.02,0.13,0.06,0.17,0.15,0.08,0.02,0.23},
                {0.18,0.12,-0.08,0.12,-0.00,-0.09,-0.13,0.04,0.25,0.02,0.25,0.17,0.02,0.07,0.26},
                {0.10,0.01,0.01,0.22,0.04,0.04,-0.21,-0.05,0.02,0.15,0.18,0.11,0.03,0.10,0.40},
                {0.10,0.12,-0.11,0.17,0.06,-0.09,-0.10,0.01,0.26,0.08,0.19,0.15,0.01,0.08,0.25},
                {0.13,0.12,-0.13,-0.02,0.15,-0.05,-0.09,0.04,0.12,0.13,0.15,0.16,-0.12,0.06,0.42},
                {0.07,-0.01,-0.03,0.08,0.10,-0.13,-0.13,0.13,0.14,0.13,0.19,0.17,-0.07,0.03,0.33},
                {0.13,0.20,-0.12,0.13,-0.06,-0.14,-0.03,0.07,-0.07,0.30,0.08,0.00,0.06,0.12,0.34},
                {0.06,0.08,-0.08,0.04,-0.02,-0.09,0.02,0.12,-0.11,0.25,0.14,-0.03,0.02,0.20,0.42},
                {0.09,0.12,-0.03,0.13,-0.03,-0.15,-0.02,-0.01,0.12,0.11,0.11,-0.09,0.21,0.21,0.37},
                {-0.01,0.21,0.12,-0.08,-0.01,-0.10,-0.01,0.06,0.20,-0.06,0.32,0.12,0.20,0.16,0.15},
                {0.07,0.15,0.08,-0.14,-0.01,-0.03,0.05,-0.03,0.23,0.07,0.07,0.11,0.21,0.17,0.26},
                {0.05,-0.03,-0.02,0.04,-0.02,0.03,-0.00,0.13,0.15,0.15,0.18,0.11,0.19,0.11,0.10},
        };
        @Override
        public void setup(Context context)
        {
            Configuration conf = context.getConfiguration();
            outputPath = conf.get("outputPath");
            mos = new MultipleOutputs<Text, Text>(context);       //初始化mos
        }
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String interfaceInfo = fileSplit.getPath().getName();    //得到文件名
            interfaceInfo = interfaceInfo.replace(".txt", "");  //去掉.txt.segmented
            String[] line = value.toString().split("\t");
            // System.out.println(value.toString());
            if(line.length == 15)
            {
                //  double[] first=new double[14];
                int time = Integer.parseInt(line[0].split(":")[0]);
                double sum =weight[time][0];
                for(int i = 1; i <=window; i++)
                {
                    sum+=weight[time][i]*Integer.parseInt(line[i]);
                }
               mos.write("OptimizedWeightedAvg",new Text(line[0]), new Text( String.format("%.2f", sum)), outputPath+"/"+interfaceInfo );

            }
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
            Job job = Job.getInstance(conf,"OptimizedWeightedAvg");

            job.setJarByClass(OptimizedWeightedAvg.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setMapperClass(OptimizedWeightedAvg.OptimizedWeightedAvgMapper.class);

            fileSystem = FileSystem.get(conf);
            FileStatus[] fileStatusArray = fileSystem.globStatus(new Path(args[0]+"/*.txt"));
            for(FileStatus fileStatus : fileStatusArray){
                path = fileStatus.getPath();
                FileInputFormat.addInputPath(job, path);
            }
            MultipleOutputs.addNamedOutput(job, "OptimizedWeightedAvg", LogTextOutputFormat.class, Text.class, Text.class);
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
