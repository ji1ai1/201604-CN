package JA.YC;

public class MyReducer extends com.aliyun.odps.mapred.ReducerBase
{	
	private com.aliyun.odps.data.Record result;
	private final long rx = 28;
	
	public void setup(TaskContext context) throws java.io.IOException
	{
		result = context.createOutputRecord();
	}
    
	public void reduce(com.aliyun.odps.data.Record key, java.util.Iterator<com.aliyun.odps.data.Record> values, com.aliyun.odps.mapred.Reducer.TaskContext context) throws java.io.IOException
	{
		result.setBigint(0, key.getBigint(0));		//ITEM_ID
		result.setString(1, key.getString(1));		//STORE_CODE
    	
		double a = key.getDouble(2);
		double b = key.getDouble(3);
		
		double[] a_qan = new double[(int)rx];
		double[] a_qanall = new double[(int)rx];
		double[] a_qantz = new double[(int)rx];
		double sqan = 0, sqanall = 0;
    	
		for(int i = 0; i < rx; i++)
		{
			com.aliyun.odps.data.Record val = values.next();
			long dase = val.getBigint(0);
    		
			double qan = val.getDouble(1);
			double qanall = val.getDouble(2);
	    	
			a_qan[(int)(dase - 1)] = qan;
			a_qanall[(int)(dase - 1)] = qanall;
			sqan += qan;
			sqanall += qanall;

			if(!values.hasNext())
				break;
		}
    	
		if(sqanall != 0)
			for(int i = 0; i < rx; i++)
				a_qantz[i] = a_qan[i] * 0.5 + a_qanall[i] * sqan / sqanall * 0.5;
    	
		double bcf;
		for(int i = 0; i < rx - 1; i++)
			bcf += Math.pow(a_qan[i] - a_qan[i + 1], 2);
		bcf = Math.pow(bcf, 0.5);
    	
		
		if(sqan == 0)
			bcf = 1;
		else
			bcf = bcf / sqan;
    	
		double zyyc = 0;
		for(int i = 1; i <= 13; i++)
			zyyc += zyt(a_qantz, a, b, i, 11 * bcf) * 640000 / i; 
    	
		result.setDouble(2, zyyc * 14);
		context.write(result);
	}
    
	private double cbt(double yc, double[] a_q, double a, double b, double alpha)
	{
		double cb = 0;
		for(int i = 0; i < a_q.length; i++)
		{
			if(yc > a_q[i])
				cb += (yc - a_q[i]) / (i + alpha) * b;
			else
				cb += (a_q[i] - yc) / (i + alpha) * a;
		}
		return cb;
	}

	private double zyt(double[] a_q, double a, double b, int d, double alpha)
	{
		double[] a_qa = new double[a_q.length / d];
		for(int sx = 0; sx < a_q.length / d * d; sx++)
			a_qa[sx / d] += a_q[sx] / d;
    	
		double zycb = Double.MAX_VALUE;
		double zyyc = -1;
		long zys = 0;
		for(int i = 0; i < a_qa.length; i++)
		{
			double c = cbt(a_qa[i], a_qa, a, b, alpha);
			if(c < zycb)
			{
				zyyc = a_qa[i];
				zycb = c;
				zys = 1;
			}
			else if(c == zycb)
			{
				zyyc = (zyyc * zys + a_qa[i]) / (1 + zys);
				zys++;
			}
		}
    	
		return zyyc;
	}
}
