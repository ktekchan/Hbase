
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class Main {
    public static void main(String[] args){

        Configuration conf = HBaseConfiguration.create();
        try {


        		//Create first table to read the data and enter in table
                HBaseAdmin admin = new HBaseAdmin(conf);
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("raw"));
                tableDescriptor.addFamily(new HColumnDescriptor("stock"));
                tableDescriptor.addFamily(new HColumnDescriptor("time"));
                tableDescriptor.addFamily(new HColumnDescriptor("price"));
                if ( admin.isTableAvailable("raw")){
                        admin.disableTable("raw");
                        admin.deleteTable("raw");
                }
                admin.createTable(tableDescriptor);

                Job job = Job.getInstance();
                job.setJarByClass(Main.class);
                FileInputFormat.addInputPath(job, new Path(args[0]));
                job.setInputFormatClass(TextInputFormat.class);
                job.setMapperClass(Job1.Map.class);
                TableMapReduceUtil.initTableReducerJob("raw", null, job);
                job.setNumReduceTasks(0);           
                job.waitForCompletion(true);
                admin.close();
                
                
                //Create second table to enter the results of first reducer
                HTableDescriptor tableDescriptor1 = new HTableDescriptor(TableName.valueOf("raw1"));
    			tableDescriptor1.addFamily(new HColumnDescriptor("stock"));
    			tableDescriptor1.addFamily(new HColumnDescriptor("Vol1"));
    			
    			if (admin.isTableAvailable("raw1")){
    				admin.disableTable("raw1");
    				admin.deleteTable("raw1");
    			}

    			admin.createTable(tableDescriptor1);
    	
    			Scan scan = new Scan();
    			scan.setCaching(500);
    			scan.setCacheBlocks(false);
    			
    			Job job2 = Job.getInstance();
    			job2.setJarByClass(Main.class);
    			TableMapReduceUtil.initTableMapperJob("raw",scan,Job2.Mapper1.class,Text.class,Text.class,job2);	
    			TableMapReduceUtil.initTableReducerJob("raw1",Job2.Reducer1.class,job2);
    			job2.setNumReduceTasks(1);
    			job2.waitForCompletion(true);
    			admin.close();
    			
    			
    			//Create third table to enter out from second reducer
    			HTableDescriptor tableDescriptor2 = new HTableDescriptor(TableName.valueOf("raw2"));
    			tableDescriptor2.addFamily(new HColumnDescriptor("stock"));
    			tableDescriptor2.addFamily(new HColumnDescriptor("stockVolatility"));
    			
    			if (admin.isTableAvailable("raw2")){
    				admin.disableTable("raw2");
    				admin.deleteTable("raw2");
    			}

    			admin.createTable(tableDescriptor2);
    	
    			Scan scan1 = new Scan();
    			scan1.setCaching(500);
    			scan1.setCacheBlocks(false);
    			
    			Job job3 = Job.getInstance();
    			job3.setJarByClass(Main.class);
    			TableMapReduceUtil.initTableMapperJob("raw1",scan1,Job3.Mapper2.class,Text.class,Text.class,job3);	
    			TableMapReduceUtil.initTableReducerJob("raw2",Job3.Reducer2.class,job3);
    			job3.setNumReduceTasks(1);
    			job3.waitForCompletion(true);
    			admin.close();
    			
    			//---------------------------------------------------------------------------------//
    			HTable table = new HTable(conf, "raw2");
    			Scan scan2 = new Scan();
    			ResultScanner scanner = table.getScanner(scan2);

    			HashMap<Double, String> Names = new HashMap<Double, String>();
    			ArrayList<ArrayList<Double>> Vol = new ArrayList<ArrayList<Double>>();
    			int companyCount = 0;
    			
    			for (Result scannerResult: scanner) {
    				
    				byte[] stockName = scannerResult.getValue(Bytes.toBytes("stock"),Bytes.toBytes("stockName"));
    	     	 	byte[] volatility = scannerResult.getValue(Bytes.toBytes("stockVolatility"), Bytes.toBytes("volatility"));
    	     	 	
    	     	 	String stock = new String(stockName);
    	    	 	String vol = new String(volatility);
    	    	 	

                    ArrayList<Double> temp1 = new ArrayList<Double>();
                    temp1.add(0, Double.parseDouble(vol));
                    temp1.add(1, (double)(companyCount));

                    Names.put((double)(companyCount), stock);

                    Vol.add(temp1);
                    companyCount++;
    			}
    			
    			 Collections.sort(Vol, new Comparator<ArrayList<Double>>() {
                 @Override
                 	public int compare(ArrayList<Double> o1, ArrayList<Double> o2) {                                
                	 	return o1.get(0).compareTo(o2.get(0));
                 	}
                 
    			 });
    			 
    			 String finalVol = new String();
    			 String finalStock = new String();
    			 int i = 0;
                 int count = 0 ;
                 
                 System.out.println("-------------------Lowest-------------------");
                 System.out.println();
                 
                 while(count != 10){
                         if(Vol.get(i).get(0) == 0.0){
                                 i++;
                                 continue;
                         }
                         else{
                                 finalVol = String.valueOf(Vol.get(i).get(0));
                                 finalStock = String.valueOf(Names.get(Vol.get(i).get(1)));

                                 System.out.println(finalStock + "\t" + finalVol);
                                 count++;
                                 i++;
                         }
                 }

                 i = 1;
                 count = 0;
                 
                 System.out.println("-------------------Highest-------------------");
                 System.out.println();
                 
                 while(count != 10){
                         if(Vol.get(companyCount-i).get(0) == 0.0 || (Vol.get(companyCount-i).get(0)).isNaN()){
                                 i++;
                                 continue;
                         }
                         else{
                        	 
                        	 finalVol = String.valueOf(Vol.get(companyCount-i).get(0));
                             finalStock = String.valueOf(Names.get(Vol.get(companyCount-i).get(1)));

                             System.out.println(finalStock + "\t" + finalVol);
                             count++;
                             i++;
                        }
                 }


    			
    			System.out.println("Successful!");
        }
        catch (Exception e) {
                e.printStackTrace();
        }
    }
}

