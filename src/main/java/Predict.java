import Predictor.Average;
import Predictor.LeastSquare;
import Predictor.MovingAverage;
import Predictor.WeightedAverage;

public class Predict
{
    public static void main(String[] args)
    {
        if(LeastSquare.run(args)==0)
            System.out.println("----------Prediction finished.----------");
        if(CalRmse.run(args)==0)
            System.out.println("----------Calculated RMSE successfully.----------");
    }
}
