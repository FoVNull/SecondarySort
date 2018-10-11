import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class SecondarySortMapper extends Mapper<LongWritable, Text, DateTemperaturePair, Text> {//定义map函数，数据生成键值
    /*
    *@param key hadoop自动生成
    * @param value  "YYYY,MM,DD,temperature"
     */
    private final Text theTemPerature = new Text();
    private final DateTemperaturePair reduceKey=new DateTemperaturePair();//key变成java对象进行处理


    protected void map(LongWritable key,Text value,
                      Mapper<LongWritable, Text, DateTemperaturePair, Text>.Context context)
                    throws IOException, InterruptedException {
        //将原始数据分成key
        String[] tokens=value.toString().split(",");
        String yearMonth=tokens[0]+tokens[1];
        String day=tokens[2];
        int temperature=Integer.parseInt(tokens[3]);

        //准备规约器(reducer)键值
        reduceKey.setYearMonth(yearMonth);
        reduceKey.setDay(day);
        reduceKey.setTemperature(temperature);
        theTemPerature.set(tokens[3]);
        // TODO Auto-generated method stub
        context.write(reduceKey,theTemPerature);
    }

}
