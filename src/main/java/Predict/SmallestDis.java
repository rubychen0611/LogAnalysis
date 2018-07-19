package Predict;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class SmallestDis {
    public static class SmallestDisMapper extends Mapper<LongWritable, Text, Text, Text> {
        private MultipleOutputs<Text, Text> mos;
        private String outputPath;
        private static double alpha = 0.87;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            outputPath = "src/main/resources/data/predictset";
            mos = new MultipleOutputs<Text, Text>(context);       //初始化mos
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String interfaceInfo = fileSplit.getPath().getName();    //得到文件名
            interfaceInfo = interfaceInfo.replace(".txt", "");  //去掉.txt.segmented
            String[] line = value.toString().split("\t");
            // System.out.println(value.toString());
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
                //System.out.println(result);
                //context.write(new Text(interfaceInfo+"_"+line[0]),new Text(String.format("%.2f", result)));
                mos.write("SmallestDis", new Text(line[0]), new Text(minidx+""), outputPath + "/" + interfaceInfo);

            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }
    public static void main(String[] args)
    {
        try {
            Job job = Job.getInstance();
            //Configuration conf = new Configuration();
            //Job job = new Job(conf, "invert index");
            job.setJobName("SmallestDis");
            job.setJarByClass(SmallestDis.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(SmallestDisMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("src/main/resources/data/trainingset"));
            MultipleOutputs.addNamedOutput(job, "SmallestDis", ExpSmoothAvg.LogTextOutputFormat.class, Text.class, Text.class);
            FileOutputFormat.setOutputPath(job, new Path("src/main/resources/data/predictset"));
            LazyOutputFormat.setOutputFormatClass(job, ExpSmoothAvg.LogTextOutputFormat.class);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) { e.printStackTrace();
        }
    }
}
