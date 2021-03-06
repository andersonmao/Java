package data;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.JedisPool;
import data.DataColumn.DataColumnCriteria;
import data.DataColumn.DataColumnOperator;
import data.DataColumn.DataColumnSort;
import data.DataColumn.DataColumnType;

/**
 * @author Anderson Mao, 2014-06-16
 */
public class DataCacheMultiTest {
	DataCacheRedisImpl cache = new DataCacheRedisImpl();
	{
		cache.setJedisPool(new JedisPool("192.168.9.220") );
	}
	
	String tableName = "MultiUser";
	
	public static void main(String[] args){
		DataCacheMultiTest inst = new DataCacheMultiTest();
		inst.init();
		inst.query();
	}
	
	public void init(){
		int count = 1000;
		log("Init: begin: count="+count);
		String idColumnName = "PK";
		List<DataColumn> columnList = new ArrayList<DataColumn>();
		columnList.add(new DataColumn("PK", DataColumnType.LONG) );
		columnList.add(new DataColumn("name", DataColumnType.STRING) );
		columnList.add(new DataColumn("age", DataColumnType.LONG) );
		columnList.add(new DataColumn("address", DataColumnType.STRING) );
		columnList.add(new DataColumn("create", DataColumnType.DATE) );
		columnList.add(new DataColumn("other11", DataColumnType.STRING) );
		columnList.add(new DataColumn("other12", DataColumnType.STRING) );
		columnList.add(new DataColumn("other13", DataColumnType.STRING) );
		columnList.add(new DataColumn("other14", DataColumnType.STRING) );
		columnList.add(new DataColumn("other15", DataColumnType.STRING) );
		columnList.add(new DataColumn("other16", DataColumnType.STRING) );
		columnList.add(new DataColumn("other17", DataColumnType.STRING) );
		columnList.add(new DataColumn("other18", DataColumnType.STRING) );
		columnList.add(new DataColumn("other19", DataColumnType.STRING) );
		columnList.add(new DataColumn("other20", DataColumnType.STRING) );
		columnList.add(new DataColumn("other21", DataColumnType.STRING) );
		columnList.add(new DataColumn("other22", DataColumnType.STRING) );
		columnList.add(new DataColumn("other23", DataColumnType.STRING) );
		columnList.add(new DataColumn("other24", DataColumnType.STRING) );
		columnList.add(new DataColumn("other25", DataColumnType.STRING) );
		columnList.add(new DataColumn("other26", DataColumnType.STRING) );
		columnList.add(new DataColumn("other27", DataColumnType.STRING) );
		columnList.add(new DataColumn("other28", DataColumnType.STRING) );
		columnList.add(new DataColumn("other29", DataColumnType.STRING) );
		columnList.add(new DataColumn("other30", DataColumnType.STRING) );
		
		List<Map<String, Object> > rowList = new ArrayList<Map<String, Object> >();
		int stepCount = count/10;
		for(int i=0; i<count; i++){
			Map<String, Object> user = new HashMap<String, Object>();
			user.put("PK", ""+i);
			user.put("name", "User"+i);
			user.put("age", ""+ (i % 100) );
			user.put("address", "ShangHaiA"+i);
			for(int j=11; j<=30; j++){
				user.put("other"+j, "BlaBlaBla");
			}
			rowList.add(user);
			//
			int step = (i+1);
			if(step % stepCount == 0){
				log("Init: step: ["+step+"/"+count+"]");
			}
		}
		log("Init: set");
		cache.set(tableName, idColumnName, columnList, rowList);
		log("Init: end");
	}
	
	public void query(){
		log("Query: --------------------------------------------------");
		DataColumnCriteria criteria = new DataColumnCriteria()
			.add("address", DataColumnOperator.LIKE, "ShangHaiA")
			.add("age", DataColumnOperator.GE, "50");
		DataColumnSort sort = new DataColumnSort("name", false);
		List<Map<String, Object> > queryList = cache.get(tableName, 10, 0, criteria, sort, null);
		log("Query: size="+queryList.size() );
		if(queryList.size()>0){
			for(Map<String, Object> user: queryList){
				log("Query: id="+user.get("PK")+", name="+user.get("NAME"));
			}
		}
		log("Query: count="+cache.count(tableName, criteria) );
	}
	
	private static void log(String value) {
		System.out.println((new Date()).toString()+": "+value);
	}
}
