import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
public class LogAnalysis
{
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private Set<String> stopwords;
        private Path localFile[];

        /*public void setup(Context context) throws IOException, InterruptedException
        {
            stopwords = new TreeSet<String>();
            Configuration conf = context.getConfiguration();
            //localFile = new Path("stopwords/People_List_unique.txt");     //获得停词表(人名表)
            localFile = DistributedCache.getLocalCacheFiles(conf);
            String line;
            BufferedReader br = new BufferedReader(new FileReader(localFile[0].toString()));
            while((line = br.readLine()) != null)
            {
                stopwords.add(line);
            }
        }*/

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();    //得到文件名
            fileName = fileName.replace(".txt.segmented", "");  //去掉.txt.segmented
            fileName = fileName.replace(".TXT.segmented", "");  //去掉.TXT.segmented
            fileName = fileName.replace(".", "");  //去掉.
            String temp;
            Text word = new Text();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens())
            {
                temp = itr.nextToken();
                // if(!stopwords.contains(temp))
                // {
                word.set(temp + "," + fileName);
                context.write(word, new IntWritable(1));
                // }
            }
        }
    }
    public static class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for(IntWritable val: values)
            {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static class NewPartitioner extends HashPartitioner<Text, IntWritable>
    {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks)
        {
            String term = key.toString().split(",")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }
    public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text>
    {
        private Text word1 = new Text(), word2 = new Text();
        private String temp = new String();
        static Text CurrentItem = new Text(" ");
        static List<String> postingList = new ArrayList<String>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            word1.set(key.toString().split(",")[0]);        //word
            temp = key.toString().split(",")[1];            //filename
            for(IntWritable val: values)
                sum += val.get();
            word2.set(temp + ":" + sum);
            if(!CurrentItem.equals(word1) && !CurrentItem.equals(" "))   //新的单词，统计前面所有词频
            {
                StringBuilder out = new StringBuilder();
                long count = 0;
                int num = 0;
                for(String p: postingList)
                {
                    num++;
                    out.append(p);
                    out.append(";");
                    count += Long.parseLong(p.substring(p.indexOf(":") + 1));
                }
                if(count > 0)
                {
                    String avg = new Formatter().format("%.1f", (double)count / num).toString();
                    context.write(CurrentItem, new Text(avg + "," + out.toString()));
                }
                postingList = new ArrayList<String>();
                CurrentItem = new Text(word1);
            }
            //CurrentItem = new Text(word1);
            postingList.add(word2.toString());  //旧的单词，添加进postingList
        }

        public void cleanup(Context context) throws IOException, InterruptedException
        {
            StringBuilder out = new StringBuilder();
            long count = 0;
            for(String p: postingList)
            {
                out.append(p);
                out.append(";");
                count += Long.parseLong(p.substring(p.indexOf(":") + 1));
            }
            if(count > 0)
                context.write(CurrentItem, new Text(out.toString()));
        }
    }
    public static void main(String[] args)
    {
        //FileUtil.deleteDir("output");

       /* String[] otherArgs = new String[]{"input","output"};
        if (otherArgs.length != 2) {
            System.err.println("Usage:Merge and duplicate removal <in> <out>");
            System.exit(2);
        }*/
        try{
            Configuration conf = new Configuration();
            //  DistributedCache.addCacheFile(new URI("hdfs://localhost:127.0.0.1/stopwords/PeopleListunique.txt"), conf);
            Job job = Job.getInstance(conf,"LogAnalysis");
            job.setJarByClass(LogAnalysis.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(InvertedIndexMapper.class);
            job.setCombinerClass(SumCombiner.class);
            job.setPartitionerClass(NewPartitioner.class);
            job.setReducerClass(InvertedIndexReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}