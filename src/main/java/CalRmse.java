import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CalRmse {
    public static class CalRmseMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] log = value.toString().split("\t");
            String time = log[0];
            String url = ((FileSplit) context.getInputSplit()).getPath() .getName().replace(".txt","");
            String num = log[1];
            context.write(new Text(time),new Text(url +"_"+num));
        }

    }
    public static class CalRmseReducer extends Reducer<Text, Text, Text, Text>
    {
        Map<String,Double> rmseMap=new HashMap<String, Double>();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            Map<String,Double> keyMap = new HashMap<String, Double>();
            for(Text val: values)
            {
                String[] array = val.toString().split("_");
                String url = array[0];
                String num = array[1];
                if(keyMap.containsKey(url))
                    keyMap.put(url,Double.parseDouble(num)-keyMap.get(url));
                else
                    keyMap.put(url,Double.parseDouble(num));
            }
            double sum=0;
            int count=keyMap.size();
            for(String ke : keyMap.keySet())
            {
                Double val = keyMap.get(ke);
                sum += val*val;
            }
            rmseMap.put(key.toString(),Math.sqrt(sum/count));
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            double sum=0;
            for(String ke : rmseMap.keySet())
            {
                Double val = rmseMap.get(ke);
                sum += val;
            }
            System.out.println(sum/rmseMap.size());
            context.write(new Text("rmse:"),new Text(""+sum/rmseMap.size()));
        }
    }
    public static int run(String[] args)
    {
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "CalRmse");
            job.setJarByClass(CalRmse.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(CalRmseMapper.class);
            job.setReducerClass(CalRmseReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileInputFormat.addInputPath(job, new Path("/user/2018st21/testset"));
            FileOutputFormat.setOutputPath(job, new Path(args[1]+"/RMSE"));
            job.waitForCompletion(true);
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }
}
