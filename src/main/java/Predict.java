import Predictor.Average;
import Predictor.WeightedAverage;

public class Predict
{
    public static void main(String[] args)
    {
        if(WeightedAverage.run(args)==0)
            System.out.println("----------Prediction finished.----------");
        if(CalRmse.run(args)==0)
            System.out.println("----------Calculated RMSE successfully.----------");
    }
}
