package data;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.MongoClient;

import data.DataColumn.DataColumnType;

public class DataCacheMongoTest {
	DataCacheMongoImpl cache = new DataCacheMongoImpl();
	
	MongoTemplate template = null;
	{
		try{
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			template = new MongoTemplate(mongoClient, "test");
			cache.setMongoTemplate(template);
		}catch(Exception ex){
			log("Error in initial: "+ex.getMessage() );
		}
	}
	
	public static void main(String[] args){
		DataCacheMongoTest inst = new DataCacheMongoTest();
		inst.init();
		inst.queryOne();
		inst.querySql();
	}
	
	private String tableName = "TestUser";
	
	public void init(){
		String idColumnName = "PK";
		List<DataColumn> columnList = new ArrayList<DataColumn>();
		columnList.add(new DataColumn("PK", DataColumnType.LONG) );
		columnList.add(new DataColumn("NAME", DataColumnType.STRING) );
		columnList.add(new DataColumn("AGE", DataColumnType.LONG) );
		columnList.add(new DataColumn("ADDRESS", DataColumnType.STRING) );
		columnList.add(new DataColumn("CREATE", DataColumnType.DATE) );
		
		List<Map<String, String> > rowList = new ArrayList<Map<String, String> >();
		addUser(rowList, "1", "UserA", "30", "LianHuaA", "20140620");
		addUser(rowList, "2", "UserB", "40", "LianHuaB", "20140620");
		addUser(rowList, "3", "UserC", "50", "LianHuaC", "20140620");
		
		cache.set(tableName, idColumnName, columnList, rowList);
	}
	
	public void queryOne(){
		log("Get user 1:");
		Map<String, String> userGet = cache.get(tableName, "1");
		for(String k: userGet.keySet() ){
			String v = userGet.get(k);
			log("  "+k+"="+v);
		}
	}
	
	public void querySql(){
		String sql = "name not like 'userb' and age >= 40";
		List<Map<String, String> > rowList = cache.get(tableName, 10, 0, sql, null, null);
		for(Map<String, String> row: rowList){
			log("get: "+row.toString() );
		}
	}
	
	private void addUser(List<Map<String, String> > rowList, String pk, String name, String age, String address, String create){
		Map<String, String> user = new HashMap<String, String>();
		user.put("PK", pk);
		user.put("NAME", name);
		user.put("AGE", age);
		user.put("ADDRESS", address);
		user.put("CREATE", create);
		rowList.add(user);
	}
	
	private static void log(String value) {
		System.out.println((new Date()).toString()+": "+value);
	}
}
