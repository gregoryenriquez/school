package Iris;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import java.io.IOException;


public class IrisReducer  extends Reducer <Text,Text,Text,Text> {
   String[] tempString;
   float tempSepalLength, tempSepalWidth, tempPetalLength, tempPetalWidth;
   float totalSepalLength, totalSepalWidth, totalPetalLength,  totalPetalWidth;
   float minSepalLength, maxSepalLength, meanSepalLength, minSepalWidth, maxSepalWidth, meanSepalWidth, minPetalLength, 
           maxPetalLength, meanPetalLength, minPetalWidth, maxPetalWidth, meanPetalWidth;

   public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

       minSepalLength = minPetalLength = minSepalWidth = minPetalWidth = Float.MAX_VALUE;
       maxSepalLength = maxPetalLength = maxSepalWidth = maxPetalWidth = Float.MIN_VALUE;
    
       int count = 0;

	meanSepalWidth = 0.0f;
	meanSepalLength = 0.0f;
	meanPetalWidth = 0.0f;
	meanPetalLength = 0.0f;

	totalSepalWidth = 0.0f;
	totalSepalLength = 0.0f;
	totalPetalWidth = 0.0f;
	totalPetalLength = 0.0f;

      for(Text value: values) {
         // use String split() method to split value and assign to tempString
        tempString = value.toString().split("_");
        
         // convert tempString elements to temp sepal/petal length/width vars
        tempSepalLength = Float.parseFloat(tempString[0]);
        tempSepalWidth = Float.parseFloat(tempString[1]);
        tempPetalLength = Float.parseFloat(tempString[2]);
        tempPetalWidth = Float.parseFloat(tempString[3]);


         // determine if you have min/max sepal/petal length/widths and assign to min/max sepal/petal lenght/widths 
         // accordingly

        // check max
        if (tempSepalLength > maxSepalLength) {
          maxSepalLength = tempSepalLength;
        }

        if (tempSepalWidth > maxSepalWidth) {
          maxSepalWidth = tempSepalWidth;
        }

        if (tempPetalLength > maxPetalLength) {
          maxPetalLength = tempPetalLength;
        }

        if (tempPetalWidth > maxPetalWidth) {
          maxPetalWidth = tempPetalWidth;
        }

        // check min

        if (tempSepalLength < minSepalLength) {
          minSepalLength = tempSepalLength;
        }

        if (tempSepalWidth < minSepalWidth) {
          minSepalWidth = tempSepalWidth;
        }

        if (tempPetalLength < minPetalLength) {
          minPetalLength = tempPetalLength;
        }

        if (tempPetalWidth < minPetalWidth) {
          minPetalWidth = tempPetalWidth;
        }

         //  calculate running totals for sepal/petal length/widths for use in calculation of means
        totalSepalLength += tempSepalLength;
        totalSepalWidth += tempSepalWidth;
        totalPetalLength += tempPetalLength;
        totalPetalWidth += tempPetalWidth;

         //  increment counter for use in calculation of means
       
        count++;
 
      } // END - for value in values  
     
      // calculate mean sepal/petal length/width 

       meanSepalLength = totalSepalLength / count;
       meanSepalWidth = totalSepalWidth / count;
       meanPetalLength = totalPetalLength / count;
       meanPetalWidth = totalPetalWidth / count;

      // TODO generate string output per the requirement
      // minSepalLength\tmaxSepalLength\tmeanSepalLength\t ...
      // min­sw max­sw mean­sw min­sl max­sl mean­sl min­pw max­pw mean­pw min­pl max­pl mean­pl
      String output = String.format("%f %f %f %f %f %f %f %f %f %f %f %f",minSepalWidth, maxSepalWidth, meanSepalWidth,
                                                              minSepalLength, maxSepalLength, meanSepalLength,
                                                              minPetalWidth, maxPetalWidth, meanPetalWidth,
                                                              minPetalLength, maxPetalLength, meanPetalLength);

      // TODO emit output to context
      context.write(key, new Text(output));

   }
}
