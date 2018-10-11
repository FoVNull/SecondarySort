import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SecondarySortDriver extends Configured implements Tool {

    @Override
    public int run(String[] args)throws Exception{//驱动器，放在main函数里直接定义应该也行
        Configuration conf=new Configuration();
        Job job=new Job(conf);
        job.setJarByClass(SecondarySortDriver.class);
        job.setJobName("SecondarySortDriver");

//        Path inputPath=new Path("/secondary_sort/input/Ssort_input.txt");
//        Path outputPath=new Path("/secondary_sort/output");
        Path inputPath=new Path("E:/DateA/Ssort_input.txt");
        Path outputPath=new Path("E:/DateA/output/");
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setOutputKeyClass(DateTemperaturePair.class);//输出格式,键值排序
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setPartitionerClass(DateTemperaturePartitioner.class);
        job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);

        boolean status=job.waitForCompletion(true);
        return status?0:1;
    }

    public static void main(String[] args) throws Exception {
        /*if (args.length!=2){
            throw new IllegalArgumentException("必须是一个输入地址一个输出地址");
        }*/
        int returnStatus= ToolRunner.run(new SecondarySortDriver(),args);
        System.exit(returnStatus);
    }
}
