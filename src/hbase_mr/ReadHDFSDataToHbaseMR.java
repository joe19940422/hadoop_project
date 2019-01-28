/**
 * 
 */
/**
 * @author joe
 *
 */
package hbase_mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReadHDFSDataToHbaseMR extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        
        int run = ToolRunner.run(new ReadHDFSDataToHbaseMR(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] arg0) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS", "hdfs://172.30.10.229:9000/");
        conf.set("hbase.zookeeper.quorum", "172.30.10.241:2181,172.30.10.242:2181,172.30.10.243:2181");
       // System.setProperty("HADOOP_USER_NAME", "hadoop");
        FileSystem fs = FileSystem.get(conf);
//        conf.addResource("config/core-site.xml");
//        conf.addResource("config/hdfs-site.xml");
        
        Job job = Job.getInstance(conf);
        
        job.setJarByClass(ReadHDFSDataToHbaseMR.class);
        
        job.setMapperClass(HDFSToHbaseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        TableMapReduceUtil.initTableReducerJob("unicom", HDFSToHbaseReducer.class, job,null,null,null,null,false);
      
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);
        
        Path inputPath = new Path("/user/hdfs/population/phone/stay_month/20170901");
        Path outputPath = new Path("/phone/output/");
        
        if(fs.exists(outputPath)) {
            fs.delete(outputPath,true);
        }
        
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        boolean isDone = job.waitForCompletion(true);
        
        return isDone ? 0 : 1;
    }
    
    
    public static class HDFSToHbaseMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {    
            context.write(value, NullWritable.get());
        }
        
    }
    
    /**
     * 95015,Íõ¾ý,ÄÐ,18,MA
     * */
    public static class HDFSToHbaseReducer extends TableReducer<Text, NullWritable, NullWritable>{
        
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values,Context context)
                throws IOException, InterruptedException {
            
            String[] split = key.toString().split(",");
            
            Put put = new Put(split[0].getBytes());
            
            put.addColumn("stay_month".getBytes(), "stime".getBytes(), split[1].getBytes());
            put.addColumn("stay_month".getBytes(), "etime".getBytes(), split[2].getBytes());
            put.addColumn("stay_month".getBytes(), "grid_id".getBytes(), split[3].getBytes());
            put.addColumn("stay_month".getBytes(), "poi_id".getBytes(), split[4].getBytes());
            put.addColumn("stay_month".getBytes(), "ptype".getBytes(), split[5].getBytes());
            put.addColumn("stay_month".getBytes(), "zone_id".getBytes(), split[6].getBytes());
            put.addColumn("stay_month".getBytes(), "is_core".getBytes(), split[7].getBytes());
            put.addColumn("stay_month".getBytes(), "province".getBytes(), split[8].getBytes());
          
            put.addColumn("stay_month".getBytes(), "date".getBytes(), split[10].getBytes());
            
            context.write(NullWritable.get(), put);
        
        }
        
    }
    
}