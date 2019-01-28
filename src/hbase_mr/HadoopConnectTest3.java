package hbase_mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
 
/***
 * 使用MapReduce向HBase中导入数据
 * @author joe
 * 
 */
public class HadoopConnectTest3
{
	public static class HBaseHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context)
		{
			String value_str = value.toString();
			//将Text型数据转为字符串：
			//1080x1920|Vivo X9|a8622b8a26ae679f0be82135f6529902|中国|湖南|益阳|wifi|2017-12-31 11:55:58
			String values[] = value_str.split(",");
		//	-1000000170699773791,2017-09-01 07:33:42,2017-09-01 20:03:07,15015403456115280,1,1,110106,N,011,V0110000,20170901	
			String uid =values[0];
			String stime=values[1];
			String etime=values[2];
			String date=values[10];
			String time = stime.split(" ")[1];//07:33:42
			String time2 = etime.split(" ")[1];//20:03:07
		//	time.replace(time, ":");
			String rowkey = uid+"-"+date+time.replace(":", "")+'-'+time2.replace(":", "");;
			
			Put p1 = new Put(Bytes.toBytes(rowkey));
			//使用行键新建Put对象
		//	p1.addColumn(Bytes.toBytes("stay_month"), Bytes.toBytes(hms), Bytes.toBytes(value_str));
			
			   
            p1.addColumn("stay_month".getBytes(), "stime".getBytes(), values[1].getBytes());
            p1.addColumn("stay_month".getBytes(), "etime".getBytes(), values[2].getBytes());
            p1.addColumn("stay_month".getBytes(), "grid_id".getBytes(), values[3].getBytes());
            p1.addColumn("stay_month".getBytes(), "poi_id".getBytes(), values[4].getBytes());
            p1.addColumn("stay_month".getBytes(), "ptype".getBytes(), values[5].getBytes());
            p1.addColumn("stay_month".getBytes(), "zone_id".getBytes(), values[6].getBytes());
            p1.addColumn("stay_month".getBytes(), "is_core".getBytes(), values[7].getBytes());
            p1.addColumn("stay_month".getBytes(), "province".getBytes(), values[8].getBytes());          
            p1.addColumn("stay_month".getBytes(), "date".getBytes(), values[10].getBytes());
			
			
			//向put中增加一列，列族为d，列名为时分秒，值为原字符串
			if (!p1.isEmpty())
			//如果Put对象不为空（有列）
			{
				ImmutableBytesWritable ib = new ImmutableBytesWritable();
				//新建一个ImmutableBytesWritable对象
				ib.set(Bytes.toBytes("unicom"));
				//将ImmutableBytesWritable对象的值设为表名
				try
				{
					context.write(ib, p1);
					//尝试将此键值对作为mapper的输出
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
	}
 
	private static final String HDFS = "hdfs://172.30.10.229:9000";// HDFS路径
	private static final String INPATH = HDFS + "/user/hdfs/population/phone/stay_month/20170901";// 输入文件路径
 
	public int run() throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = HBaseConfiguration.create();
		//任务的配置设置，configuration是一个任务的配置对象，封装了任务的配置信息
		conf.set("hbase.zookeeper.quorum", "172.30.10.241:2181,172.30.10.242:2181,172.30.10.243:2181");
		//设置zookeeper
		conf.set("hbase.rootdir", "hdfs://172.30.10.229:9000/hbase-2.1.1");
		//设置hbase根目录
		conf.set("zookeeper.znode.parent", "/hbase-2.1.1");
 
		Job job = Job.getInstance(conf, "HFile bulk load test");
		// 生成一个新的任务对象并
		job.setJarByClass(HadoopConnectTest3.class);
		//设置driver类
		job.setMapperClass(HBaseHFileMapper.class); 
		// 设置任务的map类和,map类输出结果是ImmutableBytesWritable和put类型
		TableMapReduceUtil.initTableReducerJob("unicom", null, job);
		// TableMapReduceUtil是HBase提供的工具类，会自动设置mapreuce提交到hbase任务的各种配置，封装了操作，只需要简单的设置即可
		//设置表名为clientdata_test5，reducer类为空，job为此前设置号的job
		job.setNumReduceTasks(0);
		// 设置reduce过程，这里由map端的数据直接提交，不要使用reduce类，因而设置成null,并设置reduce的个数为0
		FileInputFormat.addInputPath(job, new Path(INPATH));
		// 设置输入文件路径
		return (job.waitForCompletion(true) ? 0 : -1);
	}
 
	public static void main(String[] args)
	{
		try
		{
			new HadoopConnectTest3().run();
		}
		catch (ClassNotFoundException | IOException | InterruptedException e)
		{
			e.printStackTrace();
		}
	}
 
}
