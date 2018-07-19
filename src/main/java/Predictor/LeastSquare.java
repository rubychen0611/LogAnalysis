package Predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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

public class LeastSquare implements Predictor
{
    public static class LeastSquareMethod
    {

        private double[] x;
        private double[] y;
        private double[] weight;
        private int n;
        private double[] coefficient;

        /**
         * Constructor method.
         *
         * @param x Array of x
         * @param y Array of y
         * @param n The order of polynomial
         */
        public LeastSquareMethod(double[] x, double[] y, int n)
        {
            if (x == null || y == null || x.length < 2 || x.length != y.length
                    || n < 2) {
                throw new IllegalArgumentException(
                        "IllegalArgumentException occurred.");
            }
            this.x = x;
            this.y = y;
            this.n = n;
            weight = new double[x.length];
            for (int i = 0; i < x.length; i++) {
                weight[i] = 1;
            }
            compute();
        }

        /**
         * Constructor method.
         *
         * @param x      Array of x
         * @param y      Array of y
         * @param weight Array of weight
         * @param n      The order of polynomial
         */
        public LeastSquareMethod(double[] x, double[] y, double[] weight, int n)
        {
            if (x == null || y == null || weight == null || x.length < 2
                    || x.length != y.length || x.length != weight.length || n < 2) {
                throw new IllegalArgumentException(
                        "IllegalArgumentException occurred.");
            }
            this.x = x;
            this.y = y;
            this.n = n;
            this.weight = weight;
            compute();
        }

        /**
         * Get coefficient of polynomial.
         *
         * @return coefficient of polynomial
         */
        public double[] getCoefficient()
        {
            return coefficient;
        }

        /**
         * Used to calculate value by given x.
         *
         * @param x x
         * @return y
         */
        public double fit(double x)
        {
            if (coefficient == null) {
                return 0;
            }
            double sum = 0;
            for (int i = 0; i < coefficient.length; i++) {
                sum += Math.pow(x, i) * coefficient[i];
            }
            return sum;
        }


        /*
         * Calculate the reciprocal of x.
         *
         * @param x x
         *
         * @return the reciprocal of x
         */
        private double calcReciprocal(double x)
        {
            if (coefficient == null) {
                return 0;
            }
            double sum = 0;
            for (int i = 1; i < coefficient.length; i++) {
                sum += i * Math.pow(x, i - 1) * coefficient[i];
            }
            return sum;
        }

        /*
         * This method is used to calculate each elements of augmented matrix.
         */
        private void compute()
        {
            if (x == null || y == null || x.length <= 1 || x.length != y.length
                    || x.length < n || n < 2) {
                return;
            }
            double[] s = new double[(n - 1) * 2 + 1];
            for (int i = 0; i < s.length; i++) {
                for (int j = 0; j < x.length; j++) {
                    s[i] += Math.pow(x[j], i) * weight[j];
                }
            }
            double[] b = new double[n];
            for (int i = 0; i < b.length; i++) {
                for (int j = 0; j < x.length; j++) {
                    b[i] += Math.pow(x[j], i) * y[j] * weight[j];
                }
            }
            double[][] a = new double[n][n];
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    a[i][j] = s[i + j];
                }
            }

            // Now we need to calculate each coefficients of augmented matrix
            coefficient = calcLinearEquation(a, b);
        }

        /*
         * Calculate linear equation.
         *
         * The matrix equation is like this: Ax=B
         *
         * @param a two-dimensional array
         *
         * @param b one-dimensional array
         *
         * @return x, one-dimensional array
         */
        private double[] calcLinearEquation(double[][] a, double[] b)
        {
            if (a == null || b == null || a.length == 0 || a.length != b.length) {
                return null;
            }
            for (double[] x : a) {
                if (x == null || x.length != a.length)
                    return null;
            }

            int len = a.length - 1;
            double[] result = new double[a.length];

            if (len == 0) {
                result[0] = b[0] / a[0][0];
                return result;
            }

            double[][] aa = new double[len][len];
            double[] bb = new double[len];
            int posx = -1, posy = -1;
            for (int i = 0; i <= len; i++) {
                for (int j = 0; j <= len; j++)
                    if (a[i][j] != 0.0d) {
                        posy = j;
                        break;
                    }
                if (posy != -1) {
                    posx = i;
                    break;
                }
            }
            if (posx == -1) {
                return null;
            }

            int count = 0;
            for (int i = 0; i <= len; i++) {
                if (i == posx) {
                    continue;
                }
                bb[count] = b[i] * a[posx][posy] - b[posx] * a[i][posy];
                int count2 = 0;
                for (int j = 0; j <= len; j++) {
                    if (j == posy) {
                        continue;
                    }
                    aa[count][count2] = a[i][j] * a[posx][posy] - a[posx][j]
                            * a[i][posy];
                    count2++;
                }
                count++;
            }

            // Calculate sub linear equation
            double[] result2 = calcLinearEquation(aa, bb);

            // After sub linear calculation, calculate the current coefficient
            double sum = b[posx];
            count = 0;
            for (int i = 0; i <= len; i++) {
                if (i == posy) {
                    continue;
                }
                sum -= a[posx][i] * result2[count];
                result[i] = result2[count];
                count++;
            }
            result[posy] = sum / a[posx][posy];
            return result;
        }
    }

        public static class LogTextOutputFormat extends TextOutputFormat<Text, Text>
    {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) {
            return new Path(getOutputName(context)+".txt");
        }

    }
    public static class LeastSquareMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String interfaceInfo = fileSplit.getPath().getName();    //得到文件名
            interfaceInfo = interfaceInfo.replace(".txt", "");  //去掉.txt.segmented
            String[] line = value.toString().split("\t");
            if(line.length == 15)
            {
                double x[] = new double[14], y[] = new double[14];
                double weight[] =new double[14];
                double sum = 0;

                for(int i = 0; i < 13; i++)
                {
                    weight[i] = (double)i / 105;
                    sum += weight[i];
                }
                weight[13] = 1 - sum;

                for(int i = 0; i <= 13; i++)
                {
                    x[i] = i+1;
                    y[i] = Double.parseDouble(line[i+1]);
                }
                LeastSquareMethod squareMethod = new LeastSquareMethod(x, y, weight,3);

                double result = squareMethod.fit(15);

                context.write(new Text(interfaceInfo+"_"+line[0]),new Text(String.format("%.2f", result)));
            }
        }
    }
    public static class LeastSquareReducer extends Reducer<Text, Text, Text, Text>
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
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            String keyInfo[] = key.toString().split("_");
            mos.write("LeastSquare", new Text(keyInfo[1]),values.iterator().next(),outputPath+"/"+keyInfo[0]);
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
            Job job = Job.getInstance(conf,"LeastSquare");


            job.setJarByClass(LeastSquare.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setMapperClass(LeastSquare.LeastSquareMapper.class);
            job.setReducerClass(LeastSquare.LeastSquareReducer.class);

            fileSystem = FileSystem.get(conf);
            FileStatus[] fileStatusArray = fileSystem.globStatus(new Path(args[0]+"/*.txt"));
            for(FileStatus fileStatus : fileStatusArray){
                path = fileStatus.getPath();
                FileInputFormat.addInputPath(job, path);
            }
            MultipleOutputs.addNamedOutput(job, "LeastSquare", LogTextOutputFormat.class, Text.class, Text.class);
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
