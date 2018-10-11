import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateTemperatureGroupingComparator extends WritableComparator{//分组比较器
    public DateTemperatureGroupingComparator(){
        super(DateTemperaturePair.class,true);
    }

    @Override
    public int compare(WritableComparable wc1,WritableComparable wc2) {
        DateTemperaturePair pair = (DateTemperaturePair) wc1;
        DateTemperaturePair pair2=(DateTemperaturePair) wc2;
        return  pair.getYearMonth().compareTo(pair2.getYearMonth());
    }

}
