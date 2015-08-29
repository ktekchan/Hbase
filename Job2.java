import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Job2 {

   public static class Mapper1 extends TableMapper<Text, Text>{

      private Text key1 = new Text();
      private Text value1 = new Text();

      @Override
      public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException{

         byte[] stockName = value.getValue(Bytes.toBytes("stock"),Bytes.toBytes("name"));
         byte[] adjPrice = value.getValue(Bytes.toBytes("price"), Bytes.toBytes("price"));
         byte[] year = value.getValue(Bytes.toBytes("time"), Bytes.toBytes("yr"));
         byte[] month = value.getValue(Bytes.toBytes("time"), Bytes.toBytes("mm"));
         byte[] day = value.getValue(Bytes.toBytes("time"), Bytes.toBytes("dd"));

         String stock = new String(stockName);
         String price = new String(adjPrice);
         String yy = new String(year);
         String mm = new String(month);
         String dd = new String (day);

         key1.set(stock+"/"+yy+"/"+mm);
         value1.set(price+"/"+dd);

         context.write(key1, value1);
      }
   }

   public static class Reducer1 extends TableReducer<Text, Text, ImmutableBytesWritable>  {

      private Text valToWrite = null;

      @Override
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

         ArrayList<ArrayList<String>> valAndDate = new ArrayList<ArrayList<String>>();
         String valComponents[] = null;
         int indexCount = 0;

         for(Text value: values){

            //read the value and remove date from it
            String valClose = new String();
            valClose = value.toString();

            valComponents = valClose.split("/");

            //2d Arraylist with 1st column having the date(day) and 2nd column having the closing value
            try{
               ArrayList<String> temp = new ArrayList<String>();
               temp.add(0, valComponents[1]);
               temp.add(1, valComponents[0]);
               valAndDate.add(temp);
               indexCount++;
            }

            catch(ArrayIndexOutOfBoundsException e){
               System.out.println("Print Stack trace!!");
               e.printStackTrace();
            }

            catch(IndexOutOfBoundsException e){
               System.out.println("Print Index Stack trace!!");
               e.printStackTrace();
            }

         }

         //Sort the arraylist according to the date
         Collections.sort(valAndDate, new Comparator<ArrayList<String>>() {
            @Override
            public int compare(ArrayList<String> o1, ArrayList<String> o2) {
               return o1.get(0).compareTo(o2.get(0));
            }
         });


         double val1;
         double val2;
         double Xi;

         //Calculate xi
         val1 = Double.parseDouble(valAndDate.get(0).get(1));
         val2 = Double.parseDouble(valAndDate.get(indexCount-1).get(1));
         Xi = (val2-val1)/val1;
         byte xiVal[] = Bytes.toBytes(String.valueOf(Xi));
         //valToWrite = new Text(String.valueOf(Xi));

         //Clear Arraylist
         valAndDate.clear();

         String keyString = key.toString();
         String keyRead[] = null;
         keyRead = keyString.split("/");
         byte stock[] = Bytes.toBytes(keyRead[0]);
         byte rowid[] = Bytes.toBytes(key.toString());

         Put put = new Put(rowid);
         put.add(Bytes.toBytes("stock"), Bytes.toBytes("stockName"), stock);
         put.add(Bytes.toBytes("Vol1"), Bytes.toBytes("Xi"), xiVal);

         context.write(new ImmutableBytesWritable(rowid), put);
      }
   }
}
