import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) {
            return new Path(getOutputName(context)+".txt");
        }

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
                String ip=log[0];
                String stateCode = log[7];
                String interfaceInfo = log[4];
                interfaceInfo = interfaceInfo.substring(1);
                interfaceInfo = interfaceInfo.replaceAll("/", "-");
                String timeInfo = log[1];
                timeInfo = timeInfo.substring(13);
                String respTimeInfo = log[9];
                String hourInfo = getHourInfo(timeInfo);
                /*Emit <"1_00:00-00:00_state", "1">*/
                context.write(new Text("1_"+ "00:00-00:00_"+stateCode),one);
                /*Emit <"1_time_state>, "1">*/
                context.write(new Text("1_"+ hourInfo+"_"+stateCode),one);
                /*Emit <"2_ip", "1">*/
                context.write(new Text("2_"+ip),one);
                /*Emit <"2_ip_time", "1">*/
                context.write(new Text("2_"+ip+"_"+hourInfo),one);
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
            switch (Integer.parseInt(keyInfo[0]))
            {
                case 1: case 2: case 3:     //task1-3
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
                        int ni = Integer.parseInt(valueInfo[1]);
                        time += (Double.parseDouble(valueInfo[0]) * ni);
                        n += ni;
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
            else    /*keyInfo.length == 3*/
                return super.getPartition(new Text(keyInfo[0]+"_"+keyInfo[1]), value, numReduceTasks);
        }
    }
    public static class LogAnalysisReducer extends Reducer<Text, Text, Text, Text>
    {
        private MultipleOutputs<Text,Text> mos;
        private String[] outputPath;
        private Integer count200=0;
        private Integer count404=0;
        private Integer count500=0;
        private static String CurrentItem = new String();

        @Override
        public void setup(Context context)
        {
            Configuration conf = context.getConfiguration();
            outputPath = new String[4];
            outputPath[0] = conf.get("outputPath1");
            outputPath[1] = conf.get("outputPath2");
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
                case 1:
                    String task1time = keyInfo[1];        //time
                    String stateCode = keyInfo[2];            //stateCode
                    int task1sum = 0;
                    for(Text val: values)
                        task1sum += Integer.parseInt(val.toString());
                    if(CurrentItem.compareTo(task1time)!=0 && CurrentItem.length()!=0)   //新的单词，统计前面所有词频
                    {
                        if(CurrentItem.compareTo("00:00-00:00")==0) {
                            mos.write("Task1", new Text("200" + ":"), new Text(count200.toString()) ,outputPath[0]+"/1");
                            mos.write("Task1", new Text("404" + ":"), new Text(count404.toString()) ,outputPath[0]+"/1");
                            mos.write("Task1", new Text("500" + ":"), new Text(count500.toString()) ,outputPath[0]+"/1");

                        }
                        else
                        {
                            mos.write("Task1", new Text(CurrentItem),new Text(" 200:"+count200.toString()
                                    +" 400:"+count404.toString()+" 500:"+count500.toString()) ,outputPath[0]+"/1");
                        }
                        count200=0;
                        count404=0;
                        count500=0;
                    }
                    if(stateCode.compareTo("200")==0)
                        count200=task1sum;
                    else if(stateCode.compareTo("404")==0)
                        count404=task1sum;
                    else if(stateCode.compareTo("500")==0)
                        count500=task1sum;
                    CurrentItem = task1time;
                     break;
                case 2: {
                    int sum1 = 0;
                    for (Text t : values)
                        sum1 += Integer.parseInt(t.toString());
                    if (keyInfo.length == 2) {
                        /*output: [interface: sum]*/
                        mos.write("Task2", new Text(keyInfo[1] + ":"), new Text("" + sum1), outputPath[1] + "/" + keyInfo[1]);
                    } else { //keyInfo.length == 3
                        /*output: [time:sum]*/
                        mos.write("Task2", new Text(keyInfo[2]), new Text("" + sum1), outputPath[1] + "/" + keyInfo[1]);
                    }
                    break;
                }
                case 3: {
                    int sum = 0;
                    for (Text t : values)
                        sum += Integer.parseInt(t.toString());
                    if(keyInfo.length == 2)
                    {
                        /*output: [interface: sum]*/
                        mos.write("Task3", new Text(keyInfo[1]+":"), new Text(""+sum) ,outputPath[2]+"/"+keyInfo[1]);
                    }
                    else { //keyInfo.length == 3
                        /*output: [time:sum]*/
                        mos.write("Task3", new Text(keyInfo[2]), new Text(""+sum), outputPath[2]+"/"+keyInfo[1]);
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
                        mos.write("Task4", new Text(keyInfo[2]), new Text(String.format("%.2f", time/n)),  outputPath[3]+"/"+keyInfo[1]);
                    }
                    break;
                }

            }
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            if(CurrentItem.compareTo("00:00-00:00")==0) {
                mos.write("Task1", new Text("200" + ":"), new Text(count200.toString()) ,outputPath[0]+"/1");
                mos.write("Task1", new Text("404" + ":"), new Text(count404.toString()) ,outputPath[0]+"/1");
                mos.write("Task1", new Text("500" + ":"), new Text(count500.toString()) ,outputPath[0]+"/1");

            }
            else
            {
                mos.write("Task1", new Text(CurrentItem),new Text(" 200:"+count200.toString()
                        +" 400:"+count404.toString()+" 500:"+count500.toString()) ,outputPath[0]+"/1");
            }
           mos.close();
        }
    }
    public static void main(String[] args)
    {
        try{
            // 若输出目录存在,则删除
            for(int i = 1; i <= 4; i++)
            {
                Path path = new Path(args[i]);
                FileSystem fileSystem = FileSystem.get(new URI(args[i]), new Configuration());
                if (fileSystem.exists(path))
                    fileSystem.delete(path, true);
            }

            Configuration conf = new Configuration();

            conf.set("outputPath1", args[1]);
            conf.set("outputPath2", args[2]);
            conf.set("outputPath3", args[3]);
            conf.set("outputPath4", args[4]);
            conf.set("mapred.textoutputformat.ignoreseparator", "true");
            conf.set("mapred.textoutputformat.separator", " ");

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

            MultipleOutputs.addNamedOutput(job, "Task1", LogTextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "Task2", LogTextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "Task3", LogTextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "Task4", LogTextOutputFormat.class, Text.class, Text.class);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            LazyOutputFormat.setOutputFormatClass(job, LogTextOutputFormat.class);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}