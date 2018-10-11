# SecondarySort
二次排序的MapReduce以及Spark实现
<hr>
MapReduce----JDK1.8；Win10；IntelliJ IDEA 18.2.3；hadoop-3.0.3<br>
SecondarySortDriver   驱动器，定义IO<br>
SecondarySortMapper   map()定义，将数据输出为键值<br>
SecondarySortMapper   reduce()定义，按格式输出，value作为温度的列表<br>
DateTemperatureGroupingComparator 定义键的分组，并定义比较方法，分组比较器<br>
DateTempearturePair    (日期，温度)定义为Java对象，覆写多个Mapper，Reducer中的函数<br>
DateTemperaturePartitioner   分区器<br>
<br><hr>

