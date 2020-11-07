package JA.YC;

public class MyMapper extends com.aliyun.odps.mapred.MapperBase
{
    private com.aliyun.odps.data.Record key, value;
    
    public void setup(TaskContext context) throws java.io.IOException
    {
    	key = context.createMapOutputKeyRecord();
    	value = context.createMapOutputValueRecord();
    }

    public void map(long recordNum, com.aliyun.odps.data.Record record, com.aliyun.odps.mapred.Mapper.TaskContext context) throws java.io.IOException
    {
    	long dase = record.getBigint("dase");
    	if(dase > 28)
    		return;
    	
    	key.setBigint(0, record.getBigint("item_id"));
    	key.setString(1, record.getString("store_code"));
    	key.setDouble(2, record.getDouble("a"));
    	key.setDouble(3, record.getDouble("b"));
    	value.setBigint(0, dase);
    	value.setDouble(1, record.getDouble("qan") / record.getDouble("dsqan"));
    	value.setDouble(2, record.getDouble("qanall") / record.getDouble("dsqan"));
    	
    	context.write(key, value);
    }
}
