import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair, Text> {//定义分区器
    @Override
    public int getPartition(DateTemperaturePair pair, Text text,int numberOfPartitions){
        return Math.abs(pair.getYearMonth().hashCode()%numberOfPartitions);//确保分区数非负
    }
}
