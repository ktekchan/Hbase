import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Job3 {

   public static class Mapper2 extends TableMapper<Text, Text>{

      private Text key2 = new Text();
      private Text value2 = new Text();

      @Override
      public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException{

         byte[] stockName = value.getValue(Bytes.toBytes("stock"),Bytes.toBytes("stockName"));
         byte[] xi = value.getValue(Bytes.toBytes("Vol1"), Bytes.toBytes("Xi"));

         String stock = new String(stockName);
         String xiVal = new String(xi);

         key2.set(stock);
         value2.set(xiVal);

         context.write(key2,value2);
      }
   }

   public static class Reducer2 extends TableReducer<Text, Text, ImmutableBytesWritable>  {

      private Text valToWrite2 = null;
      private int Ncount = 0;
      private double Xsum = 0;
      private double Xbar = 0;
      private double Vol = 0;
      private double interimXiSum = 0;
      ArrayList<Double> Xi = new ArrayList<Double>();
      ArrayList<Double> interimXi = new ArrayList<Double>();


      @Override
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

         try{
            for(Text value: values){
               Xi.add(Double.parseDouble(value.toString()));
               Ncount++;
            }

            for(Double x : Xi)
               Xsum += x;

            Xbar = Xsum/(Ncount);

            for (Double x : Xi){
               interimXi.add(x-Xbar);
            }

            for (Double x : interimXi){
               interimXiSum += Math.pow(x, 2);
            }

            Vol = Math.sqrt(((1/(double)(Ncount-1)))*interimXiSum);

         }
         catch(ArrayIndexOutOfBoundsException e){
            System.out.println("Print stack trace in Stock_Map2!");
            e.printStackTrace();
         }

         byte vol[] = Bytes.toBytes(String.valueOf(Vol));

         String keyString = key.toString();
         String keyRead[] = null;
         keyRead = keyString.split("/");
         byte stock[] = Bytes.toBytes(keyRead[0]);
         byte rowid[] = Bytes.toBytes(key.toString());

         Put put = new Put(rowid);
         put.add(Bytes.toBytes("stock"), Bytes.toBytes("stockName"), stock);
         put.add(Bytes.toBytes("stockVolatility"), Bytes.toBytes("volatility"), vol);

         context.write(new ImmutableBytesWritable(rowid), put);

         Ncount = 0;
         Xsum = 0;
         Xbar = 0;
         Vol = 0;
         interimXiSum = 0;
         Xi.clear();
         interimXi.clear();

      }
   }
}
