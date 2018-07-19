import Predictor.*;
public class LogPredict
{
    public static void main(String[] args)
    {
        Predictor predictor = new OptimizedWeightedAvg();
        if(predictor.predict(args) == 0)
            System.out.println("----------Prediction finished.----------");
        if(CalRmse.run(args)==0)
            System.out.println("----------Calculated RMSE successfully.----------");
    }
}