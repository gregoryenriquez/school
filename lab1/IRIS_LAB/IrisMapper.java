package Iris;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.StringTokenizer;

public class IrisMapper  extends Mapper <LongWritable,Text,Text,Text> {
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      
      // TODO create array of string tokens from record assuming space-separated fields using split() method of String class
  String[] recordTokens = value.toString().split("\\s+");
  String sepalLength, sepalWidth, petalLength, petalWidth, species;

  // TODO pull out sepal length from columns var
  sepalLength = recordTokens[0];
  //  TODO pull out sepal width from columns var
  sepalWidth = recordTokens[1];
  // TODO pull out petal length from columns var
  petalLength = recordTokens[2];
  // TODO pull out petal width from columns var
  petalWidth = recordTokens[3];
  // TODO pull out flower id from columns var
  species = recordTokens[4];
  
      // TODO write output to context as key-value pair where key is 
      // flowerId and value is underscore-separated concatenation of 
      // sepal/petal length/widths
  String outputValue = String.format("%s_%s_%s_%s", sepalLength, sepalWidth, petalLength, petalWidth);

  context.write(new Text(species), new Text(outputValue));   
   }
}
