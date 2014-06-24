package data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Anderson Mao, 2014-06-19
 */
public class DataUtil {
	public static String[] toArray(List<String> list){
		String[] array = list.toArray(new String[0]);
		return array;
	}
	
	public static Map<String, String> toMap(List<String> keyList, List<String> valueList){
		Map<String, String> map = new HashMap<String, String>();
		for(int i=0; i<keyList.size(); i++){
			map.put(keyList.get(i), valueList.get(i) );
		}
		return map;
	}
	
	public static List<String> toUpperCase(List<String> valueList){
		// [USE_UPPER_CASE_COLUMN_NAME]
		if(valueList == null){
			return valueList;
		}
		List<String> newList = new ArrayList<String>(valueList.size() );
		for(String s: valueList){
			newList.add(s.toUpperCase() );
		}
		return newList;
	}
	
	public static Map<String, String> toUpperCase(Map<String, String> row){
		// [USE_UPPER_CASE_COLUMN_NAME]
		Map<String, String> newRow = new HashMap<String, String>();
		for(String key: row.keySet() ){
			newRow.put(key.toUpperCase(), row.get(key) );
		}
		return newRow;
	}
}
