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
 * @author Anderson Mao, 2014-06-13
 */
public class DataCacheTest {
	DataCacheRedisImpl cache = new DataCacheRedisImpl();
	{
		cache.setJedisPool(new JedisPool("192.168.9.220") );
	}
	
	String tableName = "TestUser";
	
	public static void main(String[] args){
		DataCacheTest inst = new DataCacheTest();
		inst.init();
		inst.query();
		inst.querySql();
	}
	
	public void init(){
		String idColumnName = "PK";
		List<DataColumn> columnList = new ArrayList<DataColumn>();
		columnList.add(new DataColumn("PK", DataColumnType.LONG) );
		columnList.add(new DataColumn("NAME", DataColumnType.STRING) );
		columnList.add(new DataColumn("AGE", DataColumnType.LONG) );
		columnList.add(new DataColumn("ADDRESS", DataColumnType.STRING) );
		columnList.add(new DataColumn("CREATE", DataColumnType.DATE) );
		
		List<Map<String, Object> > rowList = new ArrayList<Map<String, Object> >();
		Map<String, Object> user1 = new HashMap<String, Object>();
		user1.put("PK", "1");
		user1.put("NAME", "UserA");
		user1.put("AGE", "30");
		user1.put("ADDRESS", "LianHuaA");
		user1.put("CREATE", "20140613");
		rowList.add(user1);
		
		Map<String, Object> user2 = new HashMap<String, Object>();
		user2.put("PK", "2");
		user2.put("NAME", "UserB");
		user2.put("AGE", "40");
		user2.put("ADDRESS", "LianHuaB");
		rowList.add(user2);
		
		Map<String, Object> user3 = new HashMap<String, Object>();
		user3.put("PK", "3");
		user3.put("NAME", "UserC");
		user3.put("AGE", "40");
		user3.put("ADDRESS", "ShangHaiA");
		rowList.add(user3);
		
		for(int i=10; i<20; i++){
			Map<String, Object> user = new HashMap<String, Object>();
			user.put("PK", ""+i);
			user.put("NAME", "User"+i);
			user.put("AGE", ""+(40+i));
			user.put("ADDRESS", "ShangHaiA");
			rowList.add(user);
		}
		
		cache.set(tableName, idColumnName, columnList, rowList);
		
		log("Get user 1:");
		Map<String, Object> userGet = cache.get(tableName, "1");
		for(String k: userGet.keySet() ){
			String v = userGet.get(k).toString();
			log("  "+k+"="+v);
		}
		
	}
	
	public void query(){
		log("Query: --------------------------------------------------");
		DataColumnCriteria criteria = new DataColumnCriteria();
		criteria.add("NAME", DataColumnOperator.LIKE, "Use");
		criteria.add("AGE", DataColumnOperator.GE, "40");
		DataColumnSort sort = new DataColumnSort("NAME", false);
		List<String> columnNameList = new ArrayList<String>();
		columnNameList.add("PK");
		columnNameList.add("NAME");
		List<Map<String, Object> > queryList = cache.get(tableName, 10, 0, criteria, sort, columnNameList);
		log("Query: size="+queryList.size() );
		if(queryList.size()>0){
			for(Map<String, Object> user: queryList){
				log("Query: id="+user.get("PK")+", name="+user.get("NAME")+", colCount="+user.size() );
			}
		}
		log("Query: count="+cache.count(tableName, criteria) );
	}
	
	public void query2(){
		log("Query: --------------------------------------------------");
		DataColumnCriteria criteria = new DataColumnCriteria()
			.add("ADDRESS", DataColumnOperator.LIKE, "LianHua")
			.add("AGE", DataColumnOperator.GE, "40");
		DataColumnSort sort = new DataColumnSort("AGE", false);
		List<Map<String, Object> > queryList = cache.get(tableName, 10, 0, criteria, sort, null);
		log("Query: size="+queryList.size() );
		if(queryList.size()>0){
			for(Map<String, Object> user: queryList){
				log("Query: id="+user.get("PK")+", name="+user.get("NAME"));
			}
		}
		log("Query: count="+cache.count(tableName, criteria) );
	}
	
	public void querySql(){
		log("Query SQL: --------------------------------------------------");
		String sql = "age < = 50 and age>=30 And (name like 'A' OR name like 'B' or name not like 'C')";
		DataColumnSort sort = new DataColumnSort("NAME", false);
		List<String> columnNameList = new ArrayList<String>();
		columnNameList.add("PK");
		columnNameList.add("NAME");
		columnNameList.add("AGE");
		List<Map<String, Object> > queryList = cache.get(tableName, 10, 0, sql, sort, columnNameList);
		log("Query SQL: size="+queryList.size() );
		if(queryList.size()>0){
			for(Map<String, Object> user: queryList){
				log("Query: id="+user.get("PK")+", name="+user.get("NAME")+", age="+user.get("AGE") );
			}
		}
		log("Query SQL: count="+cache.count(tableName, sql) );
	}
	
	private static void log(String value) {
		System.out.println((new Date()).toString()+": "+value);
	}
}
