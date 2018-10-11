import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer extends Reducer<DateTemperaturePair, Text, Text, Text> {

    @Override
    protected void reduce(DateTemperaturePair key, Iterable<Text> values,
                          Reducer<DateTemperaturePair, Text, Text, Text>.Context context)
                            throws IOException, InterruptedException {
        StringBuilder sortedTemperatureList=new StringBuilder();  //values是温度列表
        for(Text value:values){ //values表示所有的年月下的温度
            sortedTemperatureList.append(value.toString());
            sortedTemperatureList.append(",");
        }
        context.write(key.getYearMonth(), new Text(sortedTemperatureList.toString()));
    }



}
