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

public class WeightAvg {
    public static class WeightAvgMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        private MultipleOutputs<Text,Text> mos;
        private String outputPath;
        private static double alpha = 0.87;
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
        private static double blta = 0.5;
        @Override
        public void setup(Context context)
        {
            Configuration conf = context.getConfiguration();
            outputPath = "src/main/resources/data/predictset";
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
               mos.write("Exp",new Text(line[0]), new Text( String.format("%.2f", sum)), outputPath+"/"+interfaceInfo );

            }
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            mos.close();
        }
    }
    public static void main(String[] args)
    {
        try {
            Job job = Job.getInstance();
            //Configuration conf = new Configuration();
            //Job job = new Job(conf, "invert index");
            job.setJobName("WeightAvg");
            job.setJarByClass(WeightAvg.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(WeightAvgMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("src/main/resources/data/trainingset"));
            MultipleOutputs.addNamedOutput(job, "Exp", ExpSmoothAvg.LogTextOutputFormat.class, Text.class, Text.class);
            FileOutputFormat.setOutputPath(job, new Path("src/main/resources/data/predictset"));
            LazyOutputFormat.setOutputFormatClass(job, ExpSmoothAvg.LogTextOutputFormat.class);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) { e.printStackTrace();
        }
    }
}
