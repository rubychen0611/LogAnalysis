import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
public class GenTrainingSet
{
    public static int startDate = 8;
    public static int endDate = 21;
    public static class TimeTable implements Writable
    {

        private int LEN = endDate-startDate;
        private int[][] dayrecord = new int[24][LEN+1];
        public TimeTable()
        {
            for(int i = 0; i < 24; i++)
                for(int j = 0; j <= LEN; j++)
                    dayrecord[i][j] = 0;
        }
        public int getDayRecord(int hour, int day)
        {
            return dayrecord[hour][day - startDate];
        }
        public void addDayRecord(int hour, int day, int x)
        {
            dayrecord[hour][day - startDate] += x;
        }
        public void write(DataOutput dataOutput) throws IOException
        {
            for(int i = 0; i < 24; i++)
                for(int j = 0; j <= LEN; j++)
                dataOutput.writeInt(dayrecord[i][j]);
        }

        public void readFields(DataInput dataInput) throws IOException
        {
            for(int i = 0; i < 24; i++)
                for(int j = 0; j <= LEN; j++)
                    dayrecord[i][j] = dataInput.readInt();
        }
    }
    public static class LogTextOutputFormat extends TextOutputFormat<Text, IntWritable>
    {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException{
            //FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
            return new Path(getOutputName(context)+".txt");
        }

    }
    public static class GenTrainingSetMapper extends Mapper<LongWritable, Text, Text, TimeTable>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

            String[] log = value.toString().split(" ");
            if(log.length == 10)
            {
                String timeInfo = log[1];
                String dateInfo = timeInfo.substring(1,3);
                int day = Integer.parseInt(dateInfo.substring(0,2));
                if(!(day >= startDate && day <= endDate))
                    return;
                String hourInfo = timeInfo.substring(13);
                int hour = Integer.parseInt(hourInfo.split(":")[0]);
                String interfaceInfo = log[4];
                interfaceInfo = interfaceInfo.substring(1);
                interfaceInfo = interfaceInfo.replaceAll("/", "-");

                /*Emit <"interface", "0,0,0,1,0,...">*/
                TimeTable newTable = new TimeTable();
                newTable.addDayRecord(hour, day, 1);
                context.write(new Text(interfaceInfo), newTable);
            }
        }

    }
    public static class GenTrainingSetCombiner extends Reducer<Text, TimeTable, Text, TimeTable>
    {
        @Override
        public void reduce(Text key, Iterable<TimeTable> values, Context context) throws IOException, InterruptedException
        {
            TimeTable newTable = new TimeTable();
            for (TimeTable v: values)
            {
                for(int i = 0; i < 24; i++)
                {
                    for (int j = startDate; j <= endDate; j++)
                    {
                        int k = v.getDayRecord(i, j);
                        if (k != 0)
                            newTable.addDayRecord(i, j, k);
                    }
                }
            }
            context.write(key, newTable);
        }
    }

    public static class GenTrainingSetReducer extends Reducer<Text, TimeTable, Text, Text>
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
        public void reduce(Text key, Iterable<TimeTable> values, Context context) throws IOException, InterruptedException
        {
            TimeTable newTable = new TimeTable();
            for (TimeTable v: values)
            {
                for(int i = 0; i < 24; i++)
                    for(int j = startDate; j <= endDate; j++) {
                        int k = v.getDayRecord(i,j);
                        if(k != 0)
                            newTable.addDayRecord(i, j, k);
                    }
            }

            for(int i = 0; i < 24; i++)
            {
                StringBuilder line = new StringBuilder();
                for(int j = startDate; j <= endDate; j++)
                {
                    line.append(newTable.getDayRecord(i,j));
                    line.append("\t");
                }
                String hourInfo = getHourInfo(i);
                mos.write("genTrainingSet", new Text(hourInfo), line, outputPath+"/"+key);
            }



        }
        private String getHourInfo(int hour)
        {
            int nextHour = (hour + 1) % 24;
            return new String(String.format("%02d",hour)+":00-" + String.format("%02d",nextHour)+":00");
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
            Path path = new Path(args[1]);
            FileSystem fileSystem = FileSystem.get(new URI(args[1]), new Configuration());
            if (fileSystem.exists(path))
                fileSystem.delete(path, true);

            Configuration conf = new Configuration();
            conf.set("outputPath", args[1]);
            Job job = Job.getInstance(conf,"GenTrainingSet");


            job.setJarByClass(GenTrainingSet.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TimeTable.class);

            job.setMapperClass(GenTrainingSetMapper.class);
            job.setCombinerClass(GenTrainingSetCombiner.class);
            job.setReducerClass(GenTrainingSetReducer.class);

            fileSystem = FileSystem.get(conf);

            FileStatus[] fileStatusArray = fileSystem.globStatus(new Path(args[0]+"/*.log"), new PathFilter() {
                public boolean accept(Path path) {
                    String regex = "^(?!.*?2015-09-22).*$";
                    return  path.toString().matches(regex);
                }
            });
            for(FileStatus fileStatus : fileStatusArray){
                 path = fileStatus.getPath();
                // System.out.println(path.toString());
                 FileInputFormat.addInputPath(job, path);
              }
            //FileInputFormat.addInputPath(job, new Path(args[0]));
            MultipleOutputs.addNamedOutput(job, "genTrainingSet", LogTextOutputFormat.class, Text.class, Text.class);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            LazyOutputFormat.setOutputFormatClass(job, LogTextOutputFormat.class);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}