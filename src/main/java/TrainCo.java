import java.io.*;

public class TrainCo {
    private static String trainfilepath="/home/hadoop/BIgData/git/LogAnalysis/src/main/resources/data/trainingset";
    private static String testfilepath="/home/hadoop/BIgData/git/LogAnalysis/src/main/resources/data/testset";
    //private static int window = 13;
    public static void main(String[] args) {
        readfile();
    }
    public static boolean readfile() {
        try {

            File file = new File(trainfilepath);
            if (!file.isDirectory()) {
//                System.out.println("文件");
//                System.out.println("path=" + file.getPath());
//                System.out.println("absolutepath=" + file.getAbsolutePath());
//                System.out.println("name=" + file.getName());

            } else if (file.isDirectory()) {
               // System.out.println("文件夹");
                String[] filelist = file.list();
              //  double[][][] traindataset=new double[24][filelist.length][window];
                BufferedReader[] trainbr=new BufferedReader[filelist.length];
                BufferedReader[] testbr=new BufferedReader[filelist.length];
                for (int i = 0; i < filelist.length; i++) {
                    File readfile = new File(trainfilepath + "/" + filelist[i]);
                    trainbr[i] = new BufferedReader(new FileReader(readfile));
                    readfile = new File(testfilepath + "/" + filelist[i]);
                    testbr[i] = new BufferedReader(new FileReader(readfile));
                }
                for (int time=0;time<24;time++)
                {
                    double[][] traindataset=new double[filelist.length][15];//*(14-window)window+1
                    for (int i=0;i<trainbr.length;i++)
                    {
                        String t=trainbr[i].readLine();
                        String[] data=t.split("\t");
                       /* for (int x=0;x+window<14;x++)
                        {
                            for (int j=0;j<=window;j++)
                                traindataset[i+filelist.length*x][j]=Integer.parseInt(data[j+1+x]);
                        }*/
                       double alpha=0.87;
                         double result = Integer.parseInt(data[1]);
                        for(int j = 2; j <= 14; j++)
                        {
                            //if(Integer.parseInt(line[i])!=max&&Integer.parseInt(line[i])!=min)
                                result= alpha*Integer.parseInt(data[j])+(1-alpha)*result;
                        }
                       for (int j=0;j<14;j++)
                           traindataset[i][j]=Integer.parseInt(data[j+1]);
                        traindataset[i][14]=result;
                       // for (int x=0;x<(15-window);x++)
                        //{
                         //   for (int j=0;j<window;j++)
                           //     traindataset[i+x*filelist.length][j]=Integer.parseInt(data[j+1+x]);
                        //}

                      //  String s = testbr[i].readLine();
                      //  traindataset[i][14]=Integer.parseInt(s.split("\t")[1]);
                    }
                    LinearRegression m = new LinearRegression(traindataset,filelist.length,15,0.001,50000);//*(15-window)window*(14-window)window+1
                 //   m.printTrainData();
                    m.trainTheta();
                    m.printTheta();
                    System.out.println();
                   // System.in.read();
                }


            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("readfile()   Exception:" + e.getMessage());
        }
        return true;
    }
}
