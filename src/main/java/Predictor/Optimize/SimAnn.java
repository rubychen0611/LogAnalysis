package Predictor.Optimize;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Random;

public class SimAnn {
    private static double eps =1e-8;     //搜索停止条件阀值
    private static double delta= 0.98;   //温度下降速度
    private static double T = 100;        //初始温度
    private static Random random=new Random(1);
    private static double[][] weight={
            {0.10 ,0.04 ,-0.00 ,0.03 ,-0.03 ,0.05 ,0.03 ,-0.03 ,0.09 ,0.10 ,0.10 ,0.10 ,0.10 ,0.10 ,0.10},
            {0.10 ,-0.07 ,0.01 ,0.05 ,0.04 ,0.05 ,0.07 ,0.00 ,0.11 ,-0.04 ,0.11 ,0.08 ,0.11 ,0.17 ,0.26},
            {0.10 ,-0.07 ,0.04 ,-0.01 ,0.03 ,0.01 ,0.05 ,0.05 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12},
            {0.10 ,-0.00 ,0.04 ,-0.06 ,0.04 ,-0.00 ,0.04 ,0.03 ,0.13 ,0.13 ,0.13 ,0.13 ,0.13 ,0.13 ,0.13},
            {0.10 ,-0.04 ,0.02 ,0.02 ,0.02 ,-0.03 ,0.01 ,0.02 ,0.13 ,0.14 ,0.13 ,0.14 ,0.13 ,0.14 ,0.14},
            {0.10 ,0.10 ,-0.01 ,0.01 ,-0.06 ,-0.03 ,-0.03 ,-0.02 ,0.14 ,0.09 ,0.09 ,0.10 ,0.10 ,0.20 ,0.22},
            {0.10 ,-0.05 ,0.04 ,0.03 ,0.00 ,-0.02 ,0.03 ,0.05 ,0.11 ,0.11 ,0.11 ,0.11 ,0.11 ,0.11 ,0.11},
            {0.10 ,-0.07 ,0.03 ,0.04 ,0.03 ,0.00 ,0.04 ,0.03 ,0.11 ,0.11 ,0.11 ,0.11 ,0.11 ,0.11 ,0.11},
            {0.10 ,-0.05 ,0.01 ,0.04 ,0.03 ,-0.03 ,0.04 ,0.04 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12},
            {0.10 ,-0.01 ,-0.03 ,0.07 ,0.01 ,-0.00 ,-0.02 ,0.01 ,0.14 ,0.14 ,0.13 ,0.13 ,0.05 ,0.10 ,0.20},
            {0.10 ,-0.02 ,0.03 ,0.10 ,-0.00 ,-0.06 ,-0.06 ,0.04 ,0.16 ,0.14 ,0.13 ,0.14 ,0.01 ,0.05 ,0.22},
            {0.10 ,0.02 ,-0.02 ,0.09 ,-0.02 ,-0.06 ,-0.02 ,0.04 ,0.15 ,0.14 ,0.14 ,0.17 ,0.02 ,0.06 ,0.22},
            {0.10 ,0.01 ,-0.09 ,0.09 ,0.05 ,0.01 ,0.06 ,0.06 ,0.12 ,0.12 ,0.12 ,0.12 ,0.09 ,0.12 ,0.14},
            {0.10 ,0.03 ,-0.08 ,0.06 ,0.04 ,0.01 ,0.02 ,0.02 ,0.15 ,0.15 ,0.14 ,0.18 ,0.03 ,0.09 ,0.22},
            {0.10 ,0.05 ,-0.04 ,0.15 ,0.07 ,-0.08 ,-0.14 ,0.01 ,0.09 ,0.12 ,0.20 ,0.18 ,0.00 ,0.03 ,0.36},
            {0.10 ,0.02 ,-0.06 ,0.13 ,0.10 ,-0.03 ,-0.08 ,0.01 ,0.19 ,0.16 ,0.13 ,0.14 ,0.04 ,0.03 ,0.23},
            {0.10 ,0.01 ,-0.06 ,0.03 ,0.08 ,-0.05 ,-0.04 ,0.10 ,0.15 ,0.14 ,0.17 ,0.13 ,-0.03 ,0.05 ,0.30},
            {0.10 ,-0.07 ,-0.04 ,0.09 ,0.09 ,-0.02 ,-0.03 ,0.12 ,0.14 ,0.12 ,0.14 ,0.12 ,-0.00 ,0.07 ,0.26},
            {0.10 ,-0.01 ,-0.07 ,0.03 ,0.04 ,-0.05 ,0.04 ,0.16 ,0.03 ,0.12 ,0.03 ,0.12 ,0.12 ,0.12 ,0.32},
            {0.10 ,0.00 ,-0.05 ,0.02 ,0.02 ,-0.04 ,0.01 ,0.11 ,-0.04 ,0.14 ,0.13 ,0.12 ,0.13 ,0.13 ,0.34},
            {0.10 ,-0.01 ,0.07 ,0.10 ,-0.01 ,-0.14 ,-0.03 ,0.08 ,0.08 ,0.07 ,0.13 ,-0.07 ,0.13 ,0.19 ,0.39},
            {0.10 ,0.05 ,0.02 ,0.03 ,0.03 ,-0.07 ,-0.00 ,0.23 ,0.12 ,-0.09 ,0.12 ,0.02 ,0.10 ,0.12 ,0.35},
            {0.10 ,0.02 ,-0.07 ,0.01 ,0.03 ,-0.01 ,0.02 ,0.11 ,0.11 ,-0.11 ,0.11 ,0.11 ,0.11 ,0.22 ,0.37},
            {0.10 ,0.01 ,-0.04 ,0.01 ,0.02 ,0.00 ,0.03 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12 ,0.13},

    };
    private static double rmse =3674.626;
    private static double foot = 0.01;
    private static String trainfilepath="/home/hadoop/BIgData/git/LogAnalysis/src/main/resources/data/trainingset";
    private static String testfilepath="/home/hadoop/BIgData/git/LogAnalysis/src/main/resources/data/testset";
    public static void main(String[] args) {
      //  System.out.print(calrmse(weight));
        for (int i=0;i<24;i++)
        {
            setup();
            SimAnnLoop(i);
        }
        System.out.println(rmse);
        System.out.println("{");
        for (int i=0;i<weight.length;i++)
        {
            System.out.print("{");
            for (int j=0;j<weight[0].length-1;j++)
            {
                System.out.print(String.format("%.2f",weight[i][j])+",");
            }
            System.out.println(String.format("%.2f",weight[i][weight[0].length-1])+"},");
        }
        System.out.print("}");
    }
    public static void setup()
    {
        eps =0.1;//1e-8;     //搜索停止条件阀值
        delta= 0.98;   //温度下降速度
        T = 1;//100;
    }
    public static void SimAnnLoop(int time)
    {
        while (T>eps)
        {
            double[][] newWeight=new double[weight.length][weight[0].length];
            for (int i=0;i<weight.length;i++)
                for (int j=0;j<weight[0].length;j++)
                    newWeight[i][j]=weight[i][j];
            for (int i=0;i<10;i++)//4782969
            {
                for (int j=0;j<=14;j++)
                {
                    int rand = random.nextInt();
                    if (rand%3==1)
                        newWeight[time][j]=weight[time][j]+foot;
                    else if (rand%3==2)
                        newWeight[time][j]=weight[time][j]-foot;
                }
                double newrmse = calrmse(newWeight);
                double de = newrmse-rmse;
                if(de<0)
                {
                    for (int t=0;t<=14;t++)
                        weight[time][t]=newWeight[time][t];
                    rmse=newrmse;
                }
                else
                {
                    double dr=random.nextFloat();
                    if (Math.exp(de/T)>dr&&Math.exp(de/T)<1)
                    {
                        for (int t=0;t<=14;t++)
                            weight[time][t]=newWeight[time][t];
                        rmse=newrmse;
                    }
                }


            }
            T=T*delta;
        }
    }
    public static double calrmse(double[][] newweight)
    {
        try {

            File file = new File(trainfilepath);
            if (!file.isDirectory()) {
            } else if (file.isDirectory()) {
                String[] filelist = file.list();
                BufferedReader[] trainbr=new BufferedReader[filelist.length];
                BufferedReader[] testbr=new BufferedReader[filelist.length];
                for (int i = 0; i < filelist.length; i++) {
                    File readfile = new File(trainfilepath + "/" + filelist[i]);
                    trainbr[i] = new BufferedReader(new FileReader(readfile));
                    readfile = new File(testfilepath + "/" + filelist[i]);
                    testbr[i] = new BufferedReader(new FileReader(readfile));
                }
                double result=0;
                for (int time=0;time<24;time++)
                {
                    double[][] traindataset=new double[filelist.length][15];//*(14-window)window+1
                    double dis=0;
                    for (int i=0;i<trainbr.length;i++)
                    {
                        String t=trainbr[i].readLine();
                        String[] line=t.split("\t");
                        double sum =newweight[time][0];
                        for(int j = 1; j <=14; j++)
                        {
                            sum+=newweight[time][j]*Integer.parseInt(line[j]);

                        }

                        String s = testbr[i].readLine();
                        int real=Integer.parseInt(s.split("\t")[1]);
                        dis+=(sum-real)*(sum-real);
                    }
                    result+=Math.sqrt(dis/trainbr.length);
                }
                result=result/24;
                for (int i = 0; i < filelist.length; i++) {
                    trainbr[i].close();
                    testbr[i].close();
                }
                return result;

            }
            return -1;

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("readfile()   Exception:" + e.getMessage());
        }
        return -1;
    }


}
