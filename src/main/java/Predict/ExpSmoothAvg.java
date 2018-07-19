package Predict;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.IOException;

public class ExpSmoothAvg {
 public static class LogTextOutputFormat extends TextOutputFormat<Text, Text>
    {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException{
            FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
            //System.out.println(committer.getWorkPath()+" "+getOutputName(context));committer.getWorkPath(),
            return new Path(getOutputName(context)+".txt");
        }
       /* public synchronized static String getCostomFileName(String name)
        {

            StringBuilder result = new StringBuilder(name);
            return result.toString();
        }*/
    }
    public static class ExpSmoothAvgMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        private MultipleOutputs<Text,Text> mos;
        private String outputPath;
        private static double alpha = 0.87;
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
              //  System.out.println(time);
                double result = Integer.parseInt(line[1]);
               // first[0]=result;
                for(int i = 2; i <= 14; i++)
                {
                    result= alpha*Integer.parseInt(line[i])+(1-alpha)*result;
                }
                mos.write("Exp",new Text(line[0]), new Text( String.format("%.2f", result)), outputPath+"/"+interfaceInfo );

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
            job.setJobName("ExpSmoothAvg");
            job.setJarByClass(ExpSmoothAvg.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(ExpSmoothAvg.ExpSmoothAvgMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("src/main/resources/data/trainingset"));
            MultipleOutputs.addNamedOutput(job, "Exp", LogTextOutputFormat.class, Text.class, Text.class);
            FileOutputFormat.setOutputPath(job, new Path("src/main/resources/data/predictset"));
            LazyOutputFormat.setOutputFormatClass(job, LogTextOutputFormat.class);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) { e.printStackTrace();
        }
    }
}
