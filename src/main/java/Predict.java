import Predictor.Average;

public class Predict
{
    public static void main(String[] args)
    {
        if(Average.run(args)==0)
            System.out.println("----------Prediction finished.----------");
        if(CalRmse.run(args)==0)
            System.out.println("----------Calculated RMSE successfully.----------");
    }
}
