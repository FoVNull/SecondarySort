import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {//将(日期,温度)定义为Java对象
    private Text yearMonth=new Text();
    private Text day=new Text();
    private IntWritable temperature=new IntWritable();
    @Override
    public int compareTo(DateTemperaturePair pair){
        int compareValue=this.yearMonth.compareTo(pair.getYearMonth());
        if(compareValue==0){
            compareValue=temperature.compareTo(pair.getTemperature());
        }
        return -1*compareValue;
        //return compareValue;升序
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        yearMonth.readFields(in);
        day.readFields(in);
        temperature.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        yearMonth.write(out);
        day.write(out);
        temperature.write(out);
    }
    @Override
    public String toString() {
        // TODO Auto-generated method stub
         StringBuilder builder = new StringBuilder();
         builder.append("DateTemperaturePair{yearMonth=");
         builder.append(yearMonth);
         builder.append(", day=");
         builder.append(day);
         builder.append(", temperature=");
         builder.append(temperature);
         builder.append("}");
         return builder.toString();
         }

    @Override
    public boolean equals(Object o) {
        // TODO Auto-generated method stub
         if (this == o) {
          return true;
         }
         if (o == null || getClass() != o.getClass()) {
         	           return false;
         }
         DateTemperaturePair that = (DateTemperaturePair) o;
         if (temperature != null ? !temperature.equals(that.temperature) : that.temperature != null) {
                   return false;
         }
         if (yearMonth != null ? !yearMonth.equals(that.yearMonth) : that.yearMonth != null) {
                  return false;
         }
         return true;
         }
    @Override
    public int hashCode() {
        int result = yearMonth != null ? yearMonth.hashCode() : 0;
        result = 31 * result + (temperature != null ? temperature.hashCode() : 0);
        return result;
    }


    public Text getYearMonth(){
        return yearMonth;
    }
    public IntWritable getTemperature(){
        return temperature;
    }
    public  Text  getDay(){
        return day;
    }
    public void setYearMonth(String yearMonth) {
        this.yearMonth.set(yearMonth);
    }
    public void setDay(String day){
        this.day.set(day);
    }
    public void setTemperature(int temperature){
        this.temperature.set(temperature);
    }

}
