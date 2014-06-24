package data;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import data.DataSql.DataSqlNode;

/**
 * @author Anderson Mao, 2014-06-17
 */
public class DataSqlTest {
	public static void main(String[] args){
		DataSqlTest inst = new DataSqlTest();
		inst.testParse();
	}
	
	public void testParse(){
		log("testParse: begin");
		//
		List<String> sqlList = new ArrayList<String>();
		sqlList.add("");
		sqlList.add("()");
		sqlList.add("name != 'abc'");
		sqlList.add("name ='abc' AND (name not like 'EFG' or age >= 10.0)" );
		//
		for(String sql: sqlList){
			log("--------------------------------------------------------------------------------");
			DataSqlNode node = DataSql.parseSql(sql);
			log("SQL: "+sql);
			log("Node: "+node);
		}
		//
		log("testParse: end");
	}
	
	private static void log(String value) {
		System.out.println((new Date()).toString()+": "+value);
	}
}
