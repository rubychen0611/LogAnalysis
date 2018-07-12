import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.net.URI;
import java.util.*;
public class LogAnalysis
{
    public static class LogTextOutputFormat extends TextOutputFormat<Text, Text>
    {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException{
            FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
            return new Path(committer.getWorkPath(),getOutputName(context)+".txt");
        }
       /* public synchronized static String getCostomFileName(String name)
        {

            StringBuilder result = new StringBuilder(name);
            return result.toString();
        }*/
    }
    public static class LogAnalysisMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

            Text one = new Text("1");
            String[] log = value.toString().split(" ");
            if(log.length == 10)
            {
                String interfaceInfo = log[4];
                interfaceInfo = interfaceInfo.substring(1);
                interfaceInfo = interfaceInfo.replaceAll("/", "-");
                String timeInfo = log[1];
                timeInfo = timeInfo.substring(13);
                String respTimeInfo = log[9];
                String hourInfo = getHourInfo(timeInfo);
                /*Emit <"3_interface", "1">*/
                context.write(new Text("3_" + interfaceInfo), one);
                /*Emit <"3_interface_time", "1">*/
                context.write(new Text("3_" + interfaceInfo + "_" + timeInfo), one);
                /* Emit <"4_interface", "responseTime_1">*/
                context.write(new Text("4_" + interfaceInfo), new Text(respTimeInfo + "_1"));
                /* Emit <"4_interface_hour>, "responceTime_1">*/
                context.write(new Text("4_" + interfaceInfo + "_" + hourInfo), new Text(respTimeInfo + "_1"));
            }
        }
        private String getHourInfo(String time)
        {
            int hour = Integer.parseInt(time.split(":")[0]);
            int nextHour = (hour + 1) % 24;
            return new String(hour+":00-" + nextHour+":00");
        }
    }
    public static class LogAnalysisCombiner extends Reducer<Text, Text, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            String[] keyInfo = key.toString().split("_");
            switch (Integer.parseInt(keyInfo[0]))         //task3
            {
                case 1:break;
                case 2:break;
                case 3:
                    {
                    int sum = 0;
                    for (Text t : values) {
                        sum += Integer.parseInt(t.toString());
                    }
                    context.write(key, new Text("" + sum));
                    break;
                }
                case 4:      //task4
                {
                    int n = 0;
                    double time = 0;
                    for (Text t : values) {
                        String[] valueInfo = t.toString().split("_");
                        time += Integer.parseInt(valueInfo[0]);
                        n += Integer.parseInt(valueInfo[1]);
                    }
                    context.write(key, new Text("" + time / n + "_" + n));
                    break;
                }
            }
        }
    }
    public static class LogAnalysisPartitioner extends HashPartitioner<Text, Text>
    {
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks)
        {
            String[] keyInfo = key.toString().split("_");
            if(keyInfo.length == 2)
                return super.getPartition(key, value, numReduceTasks);
            else    //keyInfo.length == 3
                return super.getPartition(new Text(keyInfo[0]+"_"+keyInfo[1]), value, numReduceTasks);
        }
    }
    public static class LogAnalysisReducer extends Reducer<Text, Text, Text, Text>
    {
        private MultipleOutputs<Text,Text> mos;
        private String[] outputPath;
        @Override
        public void setup(Context context)
        {
            Configuration conf = context.getConfiguration();
            outputPath = new String[4];
            //outputPath[0] = conf.get("outputPath1");
            //outputPath[1] = conf.get("outputPath2");
            outputPath[2] = conf.get("outputPath3");
            outputPath[3] = conf.get("outputPath4");
            mos = new MultipleOutputs<Text, Text>(context);       //初始化mos
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            String[] keyInfo = key.toString().split("_");
            switch (Integer.parseInt(keyInfo[0]))
            {
                case 1: break;
                case 2: break;
                case 3: {
                    int sum = 0;
                    for (Text t : values)
                        sum += Integer.parseInt(t.toString());
                    if(keyInfo.length == 2)
                    {
                        /*output: [interface: sum]*/
                        mos.write("Task3", new Text(keyInfo[1] + ":"), new Text(""+sum) ,outputPath[2]+"/"+keyInfo[1]);
                    }
                    else { //keyInfo.length == 3
                        /*output: [time:sum]*/
                        mos.write("Task3", new Text(keyInfo[2]+":"), new Text(""+sum), outputPath[2]+"/"+keyInfo[1]);
                    }
                    break;
                }
                case 4: {
                    int n = 0;
                    double time = 0;
                    for(Text t: values)
                    {
                        String[] valueInfo = t.toString().split("_");
                        int ni = Integer.parseInt(valueInfo[1]);
                        time += (Double.parseDouble(valueInfo[0]) * ni);
                        n += ni;
                    }
                    if(keyInfo.length == 2)
                    {
                        /*output: [interface: average]*/
                        mos.write("Task4",new Text(keyInfo[1]+":"), new Text(String.format("%.2f", time/n)), outputPath[3]+"/"+keyInfo[1]);
                    }
                    else{   //keyInfo.length == 3
                        mos.write("Task4", new Text(keyInfo[2]+":"), new Text(String.format("%.2f", time/n)),  outputPath[3]+"/"+keyInfo[1]);
                    }
                    break;
                }

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
        try{
            // 若输出目录存在,则删除
            Path path = new Path("/output");
            FileSystem fileSystem = FileSystem.get(new URI(path.toString()), new Configuration());
            if (fileSystem.exists(path))
                fileSystem.delete(path, true);
            for(int i = 1; i <= 2; i++)
            {
                path = new Path(args[i]);
                fileSystem = FileSystem.get(new URI(args[i]), new Configuration());
                if (fileSystem.exists(path))
                    fileSystem.delete(path, true);
            }

            Configuration conf = new Configuration();
            conf.set("outputPath3", args[1]);
            conf.set("outputPath4", args[2]);
            Job job = Job.getInstance(conf,"LogAnalysis");

            job.setJarByClass(LogAnalysis.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setMapperClass(LogAnalysisMapper.class);
            job.setCombinerClass(LogAnalysisCombiner.class);
            job.setPartitionerClass(LogAnalysisPartitioner.class);
            job.setReducerClass(LogAnalysisReducer.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            MultipleOutputs.addNamedOutput(job, "Task3", LogTextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "Task4", LogTextOutputFormat.class, Text.class, Text.class);
            FileOutputFormat.setOutputPath(job, new Path("/output"));

            LazyOutputFormat.setOutputFormatClass(job, LogTextOutputFormat.class);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}