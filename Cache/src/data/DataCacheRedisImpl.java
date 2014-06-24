package data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import data.DataColumn.DataColumnCriteria;
import data.DataColumn.DataColumnCriterion;
import data.DataColumn.DataColumnOperator;
import data.DataColumn.DataColumnSort;
import data.DataColumn.DataColumnSortColumn;
import data.DataColumn.DataColumnType;
import data.DataSql.DataSqlNode;
import data.DataSql.DataSqlOperator;

/**
 * @author Anderson Mao, 2014-06-13
 */
public class DataCacheRedisImpl implements DataCache {
	private static final Logger logger = Logger.getLogger(DataCacheRedisImpl.class);
	private static void debug(String message){
		if(Logger.getRootLogger().getAllAppenders().hasMoreElements() ){
			logger.debug(message);
		}else{
			System.out.println((new Date()).toString()+": "+DataCacheRedisImpl.class.getSimpleName()+": "+message);
		}
	}
	
	private static final int REDIS_PIPELINE_BATCH_SIZE = 1000;
	
	private JedisPool jedisPool = null;
	public void setJedisPool(JedisPool jedisPool){
		this.jedisPool = jedisPool;
	}
	
	private Jedis jedisFromPool = null;
	private synchronized Jedis getJedis(){
		if(jedisFromPool == null){
			jedisFromPool = jedisPool.getResource();
		}
		return jedisFromPool;
	}
	
	private Map<String, DataTable> dataTableCache = new HashMap<String, DataTable>();

	@Override
	public void set(String tableName, String idColumnName, List<DataColumn> columnList, List<Map<String, Object>> rowList) throws DataException {
		Jedis jedis = getJedis();
		try{
			debug("set: begin: table="+tableName);
			DataTable dataTable = new DataTable(tableName, idColumnName, columnList);
			dataTableCache.put(tableName, dataTable);
			// Use Redis Pipeline to save multi objects
			Pipeline p = jedis.pipelined();
			//// Reset
			// Reset all columns
			for(DataColumn col: columnList){
				String colName = col.getName();
				String indexKey = dataTable.getIndexKey(colName);
				String hashKey = dataTable.getHashKey(colName);
				p.del(hashKey);
				p.del(indexKey);
			}
			// Reset query and sort
			String idKey = dataTable.getIdListKey();
			p.del(idKey);
			p.set(dataTable.getQueryNameKey(), "");
			p.set(dataTable.getSortNameKey(), "");
			//// Add
			int columnSize = columnList.size();
			int rowSize = rowList.size();
			List<String> idList = new ArrayList<String>(rowSize );
			List<Map<String, String> > hashList = new ArrayList<Map<String, String>>(columnSize);
			Map<String, Map<String, Double> > indexMap = new HashMap<String, Map<String, Double> >();
			for(int i=0; i< columnSize; i++){
				hashList.add(new HashMap<String, String>(rowSize) );
				DataColumn col = columnList.get(i);
				DataColumnType type = col.getType();
				if(!type.equals(DataColumnType.STRING) ){
					String colName = col.getName();
					indexMap.put(colName, new HashMap<String, Double>(rowSize) );
				}
			}
			// Loop
			int count = 0;
			for(Map<String, Object> row: rowList){
				// [USE_UPPER_CASE_COLUMN_NAME]
				row = DataUtil.toUpperCase(row);
				//
				String id = dataTable.getId(row);
				p.hmset(dataTable.getKey(id), DataUtil.toStringMap(row) );
				idList.add(id);
				
				for(int i=0; i< columnSize; i++){
					DataColumn col = columnList.get(i);
					Map<String, String> hash = hashList.get(i);
					String colName = col.getName();
					DataColumnType type = col.getType();
					String value = "";
					if(row.get(colName) !=null){
						value = row.get(colName).toString();
					}
					// Save String to HashSet; Save Number/Data to HashSet and SortedSet
					if(type.equals(DataColumnType.STRING) ){
						if(value==null){
							value = "";
						}
						hash.put(id, value);
					}else{
						Double factor = null;
						if(value==null || value.equals("")){
							value = "0";
						}
						factor = Double.parseDouble(value);
						hash.put(id, factor.toString() );
						Map<String, Double> index = indexMap.get(colName);
						index.put(id, factor);
					}
				}
				//
				count++;
				if(REDIS_PIPELINE_BATCH_SIZE >0 && (count % REDIS_PIPELINE_BATCH_SIZE) == 0){
					p.sync();
					p = jedis.pipelined();
				}
			}
			p.sync();
			p = null;
			// Save loop result. Use new jedis pipeline
			debug("set: index: table="+tableName);
			jedis.rpush(idKey, DataUtil.toArray(idList) );
			for(int i=0; i< columnSize; i++){
				String hashKey = dataTable.getHashKey(columnList.get(i).getName() );
				Map<String, String> hash = hashList.get(i);
				jedis.hmset(hashKey, hash);
			}
			for(String colName: indexMap.keySet() ){
				Map<String, Double> index = indexMap.get(colName);
				String indexKey = dataTable.getIndexKey(colName);
				jedis.zadd(indexKey, index);
			}
			debug("set: end");
		}catch(Exception ex){
			throw new DataException(ex);
		}
	}

	@Override
	public Map<String, Object> get(String tableName, String id) throws DataException {
		Jedis jedis = getJedis();
		DataTable dataTable = getDataTable(tableName);
		Map<String, String> obj = jedis.hgetAll(dataTable.getKey(id) );
		return convertToObjectRow(dataTable, obj);
	}

	@Override
	public List<Map<String, Object> > get(String tableName, int pageSize, int pageNumber, DataColumnCriteria criteria, DataColumnSort sort, List<String> columnNameList) throws DataException {
		if(criteria == null){
			criteria = new DataColumnCriteria();
		}
		return getList(tableName, pageSize, pageNumber, criteria, null, sort, columnNameList);
	}
	
	@Override
	public List<Map<String, Object> > get(String tableName, int pageSize, int pageNumber, String sql, DataColumnSort sort, List<String> columnNameList) throws DataException{
		if(sql == null){
			sql = "";
		}
		return getList(tableName, pageSize, pageNumber, null, sql, sort, columnNameList );
	}
	
	private List<Map<String, Object> > convertToObjectList(DataTable dataTable, List<Map<String, String>> rowList){
		if(rowList == null){
			return null;
		}
		List<Map<String, Object> > myList = new ArrayList<Map<String, Object> >(rowList.size() );
		for(Map<String, String> row: rowList){
			myList.add(convertToObjectRow(dataTable, row) );
		}
		//
		return myList;
	}
	
	private Map<String, Object> convertToObjectRow(DataTable dataTable, Map<String, String> row){
		Map<String, Object> myRow = new HashMap<String, Object>();
		for(DataColumn col: dataTable.getColumnList() ){
			String columnName = col.getName();
			if(row.containsKey(columnName) ){
				Object value = DataColumn.parseDataColumnValue(col.getType(), row.get(columnName) );
				myRow.put(columnName, value);
			}
		}
		//
		return myRow;
	}
	
	private List<Map<String, Object> > getList(String tableName, int pageSize, int pageNumber, DataColumnCriteria criteria, String sql, DataColumnSort sort, List<String> columnNameList) throws DataException {
		Jedis jedis = getJedis();
		DataTable dataTable = getDataTable(tableName);
		columnNameList = DataUtil.toUpperCase(columnNameList);// [USE_UPPER_CASE_COLUMN_NAME]
		// Default
		if(sort==null){
			sort = new DataColumnSort(dataTable.getIdColumnName(), true);
		}
		String sortName = null;
		// [FORK_BY_CRITERIA_OR_SQL]
		List<DataColumnCriterion> criterionList = null;
		DataSqlNode dataSqlNode = null;
		if(criteria != null){
			criterionList = criteria.getCriterionList();
			sortName = getSortName(criterionList, sort);
		}else{
			dataSqlNode = DataSql.parseSql(sql);
			sortName = getSortName(dataSqlNode, sort);
		}
		//
		String sortNameKey = dataTable.getSortNameKey();
		String sortListKey = dataTable.getSortListKey();
		String oldSortName =jedis.get(sortNameKey);
		List<String> idList = null;
		if(sortName.equals(oldSortName) ){// Use sort cache
			idList = jedis.lrange(sortListKey, 0, -1);
		}else{
			// Run Query
			// [FORK_BY_CRITERIA_OR_SQL]
			if(criteria != null){
				criterionList = criteria.getCriterionList();
				idList = query(dataTable, criterionList);
			}else{
				debug("getList: sql=" + ((dataSqlNode==null) ? "" : dataSqlNode.toString() ) );
				idList = query(dataTable, dataSqlNode);
			}
			// Run Sort (Only support two columns)
			List<DataColumnSortColumn> sortColumnList = sort.getSortColumnList();
			String columnName = dataTable.getIdColumnName();
			Boolean columnAsc = true;
			if(sortColumnList.size() >=1){
				DataColumnSortColumn firstSortColumn = sortColumnList.get(0);
				if(dataTable.isColumn(firstSortColumn.columnName) ){
					columnName = firstSortColumn.columnName;
					columnAsc = firstSortColumn.columnAsc;
				}
			}
			String secondColumnName = null;
			Boolean secondColumnAsc = true;
			if(sortColumnList.size() >=2){
				DataColumnSortColumn secondSortColumn = sortColumnList.get(1);
				if(dataTable.isColumn(secondSortColumn.columnName) ){
					secondColumnName = secondSortColumn.columnName;
					secondColumnAsc = secondSortColumn.columnAsc;
				}
			}
			//
			String hashKey = dataTable.getHashKey(columnName);
			Map<String, String> columnValueMap = getHash(hashKey, idList);
			DataColumnType columnType = dataTable.getColumnType(columnName);;
			
			Map<String, String> secondColumnValueMap = null;
			DataColumnType secondColumnType = null;
			if(secondColumnName !=null ){
				String secondHashKey = dataTable.getHashKey(secondColumnName);
				secondColumnValueMap = getHash(secondHashKey, idList);
				secondColumnType = dataTable.getColumnType(secondColumnName);
			}
			DataComparator comp = new DataComparator(columnValueMap, columnType, columnAsc, secondColumnValueMap, secondColumnType, secondColumnAsc);
			Collections.sort(idList, comp);
			// Save sort cache
			jedis.set(sortNameKey, sortName);
			jedis.del(sortListKey);
			if(idList.size() >0){
				String[] idArray = DataUtil.toArray(idList);
				jedis.rpush(sortListKey, idArray);
			}
		}
		// Get page
		int pageStart = pageNumber * pageSize;
		pageStart = Math.max(pageStart, 0);
		pageStart = Math.min(pageStart, idList.size() );
		int pageEnd   = pageStart + pageSize;
		pageEnd = Math.min(pageEnd, idList.size() );
		List<String> pageIdList = idList.subList(pageStart, pageEnd);
		List<Map<String, String> > result = get(dataTable, pageIdList, columnNameList);
		//
		return convertToObjectList(dataTable, result);
	}
	
	@Override
	public int count(String tableName, DataColumnCriteria criteria) throws DataException{
		if(criteria==null){
			criteria = new DataColumnCriteria();
		}
		DataTable dataTable = getDataTable(tableName);
		return (int)queryCount(dataTable, criteria.getCriterionList() );
	}
	
	@Override
	public int count(String tableName, String sql) throws DataException{
		DataTable dataTable = getDataTable(tableName);
		DataSqlNode dataSqlNode = DataSql.parseSql(sql);
		return (int)queryCount(dataTable, dataSqlNode);
	}
	
	private DataTable getDataTable(String tableName) {
		DataTable dataTable = dataTableCache.get(tableName);
		if(dataTable == null){
			throw new DataException("Can not find table '"+tableName+"'. Please run set() first");
		}
		return dataTable;
	}
	
	private String getSortName(List<DataColumnCriterion> criterionList, DataColumnSort sort){
		StringBuilder sb = new StringBuilder();
		sb.append(getQueryName(criterionList) );
		sb.append(sort.toString() );
		sb.append(":");
		return sb.toString();
	}
	
	private String getSortName(DataSqlNode dataSqlNode, DataColumnSort sort){
		StringBuilder sb = new StringBuilder();
		sb.append((dataSqlNode == null) ? "" : dataSqlNode.toString() );
		sb.append(sort.toString() );
		sb.append(":");
		return sb.toString();
	}
	
	private String getQueryName(List<DataColumnCriterion> criteriaList){
		StringBuilder sb = new StringBuilder();
		for(DataColumnCriterion crit: criteriaList){
			sb.append(crit.toString() );
			sb.append(":");
		}
		return sb.toString();
	}
	
	
	/**
	 * @param columnNameList: null to get all columns
	 */
	private List<Map<String, String> > get(DataTable dataTable, List<String> idList, List<String> columnNameList){
		List<Map<String, String> > result = new ArrayList<Map<String, String> >();
		//
		Jedis jedis = getJedis();
		if(columnNameList == null){ // Get all columns
			List<Response<Map<String, String>> > respList = new ArrayList<Response<Map<String, String>> >(idList.size() );
			Pipeline p = jedis.pipelined();
			for(String id: idList){
				String key = dataTable.getKey(id);
				Response<Map<String, String> > r = null;
				r = p.hgetAll(key);
				respList.add(r);
			}
			p.sync();
			for(Response<Map<String, String> > r: respList){
				result.add(r.get() );
			}
		}else{
			List<Response<List<String> > > respList = new ArrayList<Response<List<String> > >(idList.size() );
			Pipeline p = jedis.pipelined();
			for(String id: idList){
				String key = dataTable.getKey(id);
				Response<List<String> > r = p.hmget(key, DataUtil.toArray(columnNameList) );
				respList.add(r);
			}
			p.sync();
			for(Response<List<String> > r: respList){
				List<String> valueList = r.get();
				result.add(DataUtil.toMap(columnNameList, valueList) );
			}
		}
		//
		return result;
	}
	
	/**
	 * @param idList: null to get all, otherwise get result only for id in idList
	 * @return map of: id => column value
	 */
	private Map<String, String> getHash(String hashKey, List<String> idList){
		Map<String, String> columnValueMap = new HashMap<String, String>();
		//
		Jedis jedis = getJedis();
		if(idList==null){
			columnValueMap = jedis.hgetAll(hashKey);
		}else if(idList.size()>0){
			// Use idList to minimize the search scope
			String[] idArray = DataUtil.toArray(idList);
			List<String> valueList = jedis.hmget(hashKey, idArray);
			columnValueMap = new HashMap<String, String>(idList.size() );
			for(int i=0; i<idList.size(); i++){
				columnValueMap.put(idList.get(i), valueList.get(i));
			}
		}
		return columnValueMap;
	}
	
	private Set<String> findNumber(DataTable dataTable, DataColumnCriterion criterion, Set<String> andIdList){
		Set<String> result = null;
		//
		DataColumnOperator op = criterion.operator;
		Double value = Double.valueOf(criterion.value);
		if(DataColumnOperator.EQ.equals(op) ){
			result = findNumberByRange(dataTable, criterion.columnName, value, value, true);
		}else if(DataColumnOperator.GE.equals(op) ){
			result = findNumberByRange(dataTable, criterion.columnName, value, Double.MAX_VALUE, true);
		}else if(DataColumnOperator.LE.equals(op) ){
			result = findNumberByRange(dataTable, criterion.columnName, - Double.MAX_VALUE, value, true);
		}else if(DataColumnOperator.GT.equals(op) ){
			result = findNumberByRange(dataTable, criterion.columnName, value, Double.MAX_VALUE, false);
		}else if(DataColumnOperator.LT.equals(op) ){
			result = findNumberByRange(dataTable, criterion.columnName, - Double.MAX_VALUE, value, false);
		}else if(DataColumnOperator.NOT_EQ.equals(op) ){
			Set<String> resultGT = findNumberByRange(dataTable, criterion.columnName, value, Double.MAX_VALUE, false);
			Set<String> resultLT = findNumberByRange(dataTable, criterion.columnName, - Double.MAX_VALUE, value, false );
			result = resultGT;
			result.addAll(resultLT);
		}else{
			throw new DataException("Invalid Number operator '"+op+"' for column '"+criterion.columnName+"'");
		}
		if(andIdList !=null){
			result.retainAll(andIdList);
		}
		//
		return result;
	}
	
	/**
	 * find Number by range in redis server
	 */
	private Set<String> findNumberByRange(DataTable dataTable, String columnName, Double rangeStart, Double rangeEnd, boolean isInclusive){
		String indexKey = dataTable.getIndexKey(columnName);
		Set<String> result = null;
		Jedis jedis = getJedis();
		if(isInclusive){
			result = jedis.zrangeByScore(indexKey, rangeStart, rangeEnd);
		}else{
			// Exclusive by using "(" char in string start. @see http://redis.io/commands/zrangebyscore
			String strStart = rangeStart.toString();
			if(rangeStart != (- Double.MAX_VALUE) ){
				strStart = "(" + strStart;
			}
			String strEnd = rangeEnd.toString();
			if(rangeEnd != Double.MAX_VALUE){
				strEnd = "(" + strEnd;
			}
			result = jedis.zrangeByScore(indexKey, strStart, strEnd);
		}
		return result;
	}
	
	/**
	 * Find String text in redis client. Case insensitive
	 * @param MyOp: EQ or LIKE or NOT_LIKE
	 * @param idSet: null to get all, otherwise get result only for id in idSet
	 */
	private Set<String> findString(DataTable dataTable, DataColumnCriterion criterion, Set<String> idSet){
		String hashKey = dataTable.getHashKey(criterion.columnName); 
		List<String> idList = null;
		if(idSet!=null){
			idList = new ArrayList<String>(idSet);
		}
		Map<String, String> columnValueMap = getHash(hashKey, idList);
		//
		Set<String> ss = new HashSet<String>();
		for(String id: columnValueMap.keySet() ){
			String text = columnValueMap.get(id);
			// Case insensitive
			text = text.trim().toLowerCase();
			String value = criterion.value.trim().toLowerCase();
			if(DataColumnOperator.EQ.equals(criterion.operator) ){
				if(text.equals(value) ){
					ss.add(id);
				}
			}else if(DataColumnOperator.NOT_EQ.equals(criterion.operator) ){
				if(!text.equals(value) ){
					ss.add(id);
				}
			}else if(DataColumnOperator.NOT_LIKE.equals(criterion.operator) ){
				if(!text.contains(value) ){
					ss.add(id);
				}
			}else if(DataColumnOperator.LIKE.equals(criterion.operator) ){
				if(text.contains(value) ){
					ss.add(id);
				}
			}else{
				throw new DataException("Invalid operation '"+criterion.operator.name()+"' for column '"+criterion.columnName+"' on table '"+dataTable.tableName+"'");
			}
		}
		//
		return ss;
	}
	
	/**
	 * @see query()
	 */
	private long queryCount(DataTable dataTable, List<DataColumnCriterion> criterionList){
		Jedis jedis = getJedis();
		// Return all if no criteria
		if(criterionList.size() ==0){
			return jedis.llen(dataTable.getIdListKey() );
		}
		// Use cache
		String queryName = getQueryName(criterionList);
		String queryNameKey = dataTable.getQueryNameKey();
		String queryListKey = dataTable.getQueryListKey();
		String oldQueryName = jedis.get(queryNameKey);
		if(queryName.equals(oldQueryName) ){
			return jedis.llen(queryListKey);
		}
		// Run Query
		List<String> list = query(dataTable, criterionList);
		return list.size();
	}
	
	/**
	 * Query with cache
	 * @see querySize()
	 */
	private List<String> query(DataTable dataTable, List<DataColumnCriterion> criterionList){
		Jedis jedis = getJedis();
		// Return all if no criteria
		if(criterionList== null || criterionList.size() ==0){
			List<String> list = jedis.lrange(dataTable.getIdListKey(), 0, -1);
			return list;
		}
		// Use query cache
		String queryName = getQueryName(criterionList);
		String queryNameKey = dataTable.getQueryNameKey();
		String queryListKey = dataTable.getQueryListKey();
		String oldQueryName = jedis.get(queryNameKey);
		if(queryName.equals(oldQueryName) ){
			List<String> list = jedis.lrange(queryListKey, 0, -1);
			return list;
		}
		// Separate into Number columns and String columns
		List<DataColumnCriterion> numList = new ArrayList<DataColumnCriterion>();
		List<DataColumnCriterion> strList = new ArrayList<DataColumnCriterion>();
		
		for(DataColumnCriterion crit: criterionList){
			String colName = crit.columnName;
			if(!dataTable.isColumn(colName) ){
				throw new DataException("Invalid column '"+colName+"'");
			}
			if(dataTable.isStringColumn(colName) ){
				strList.add(crit);
			}else{
				numList.add(crit);
			}
		}
		
		Set<String> allResult = null;
		// Query Number columns first
		List<Set<String> > resultList = new ArrayList<Set<String> >();
		for(DataColumnCriterion crit: numList){
			Set<String> result = findNumber(dataTable, crit, null);
			resultList.add(result);
		}
		if(resultList.size() >0){
			allResult = resultList.get(0);
			for(int i=1; i<resultList.size(); i++){
				Set<String> result = resultList.get(i);
				allResult.retainAll(result);
			}
		}
		
		// Query String columns within Number columns result
		for(DataColumnCriterion crit: strList){
			allResult = findString(dataTable, crit, allResult);
		}
		List<String> allResultList = new ArrayList<String>(allResult);
		// Save query cache
		jedis.set(queryNameKey, queryName);
		jedis.del(queryListKey);
		if(allResultList.size()>0){
			String[] idArray = DataUtil.toArray(allResultList);
			jedis.rpush(queryListKey, idArray);
		}
		//
		return allResultList;
	}
	
	/**
	 * @see query(DataTable, DataSqlNode)
	 */
	private long queryCount(DataTable dataTable, DataSqlNode dataSqlNode){
		Jedis jedis = getJedis();
		// Return all if no criteria
		if(dataSqlNode == null){
			return jedis.llen(dataTable.getIdListKey() );
		}
		// Use cache
		String queryName = dataSqlNode.toString();
		String queryNameKey = dataTable.getQueryNameKey();
		String queryListKey = dataTable.getQueryListKey();
		String oldQueryName = jedis.get(queryNameKey);
		if(queryName.equals(oldQueryName) ){
			return jedis.llen(queryListKey);
		}
		// Run Query
		List<String> list = query(dataTable, dataSqlNode);
		return list.size();
	}
	
	/**
	 * Query with cache
	 */
	private List<String> query(DataTable dataTable, DataSqlNode dataSqlNode){
		Jedis jedis = getJedis();
		// Return all if no criteria
		if(dataSqlNode== null){
			List<String> list = jedis.lrange(dataTable.getIdListKey(), 0, -1);
			return list;
		}
		// Use query cache
		String queryName = dataSqlNode.toString();
		String queryNameKey = dataTable.getQueryNameKey();
		String queryListKey = dataTable.getQueryListKey();
		String oldQueryName = jedis.get(queryNameKey);
		if(queryName.equals(oldQueryName) ){
			List<String> list = jedis.lrange(queryListKey, 0, -1);
			return list;
		}
		// Run
		Set<String> allResult = querySqlNode(dataTable, dataSqlNode, null);
		List<String> allResultList = new ArrayList<String>(allResult);
		// Save query cache
		jedis.set(queryNameKey, queryName);
		jedis.del(queryListKey);
		if(allResultList.size()>0){
			String[] idArray = DataUtil.toArray(allResultList);
			jedis.rpush(queryListKey, idArray);
		}
		//
		return allResultList;
	}
	
	private Set<String> querySqlNode(DataTable dataTable, DataSqlNode dataSqlNode, Set<String> andIdList){
		if(dataSqlNode.criterion !=null){
			return queryCriterion(dataTable, dataSqlNode.criterion, andIdList);
		}else if(DataSqlOperator.AND.equals(dataSqlNode.operator) ){
			Set<String> leftSet = querySqlNode(dataTable, dataSqlNode.leftNode, andIdList);
			Set<String> rightSet = querySqlNode(dataTable, dataSqlNode.rightNode, leftSet);// And by leftSet
			return rightSet;
		}else if(DataSqlOperator.OR.equals(dataSqlNode.operator) ){
			Set<String> leftSet = querySqlNode(dataTable, dataSqlNode.leftNode, andIdList);
			Set<String> rightSet =  querySqlNode(dataTable, dataSqlNode.rightNode, andIdList);
			rightSet.addAll(leftSet);
			return rightSet;
		}else{
			throw new DataException("Invalid DataSqlNode: "+dataSqlNode);
		}
	}
	
	private Set<String> queryCriterion(DataTable dataTable, DataColumnCriterion criterion, Set<String> andIdList){
		Set<String> result = null;
		//
		String colName = criterion.columnName;
		if(!dataTable.isColumn(colName) ){
			throw new DataException("Invalid column '"+colName+"'");
		}
		if(dataTable.isStringColumn(colName) ){
			result = findString(dataTable, criterion, andIdList);
		}else{
			result = findNumber(dataTable, criterion, andIdList);
		}
		return result;
	}
	
	private static class DataComparator implements Comparator<String>{
		private Map<String, String> columnValueMap;
		private DataColumnType columnType;
		private Boolean columnAsc;
		
		private Map<String, String> secondColumnValueMap;
		private DataColumnType secondColumnType;
		private Boolean secondColumnAsc;
		@Override
		public int compare(String id0, String id1) {
			String value0 = columnValueMap.get(id0);
			String value1 = columnValueMap.get(id1);
			int cv = compare(value0, value1, columnType);
			if(Boolean.FALSE.equals(columnAsc) ){
				cv = - cv;
			}
			if(cv==0 && secondColumnValueMap!=null){
				String secondValue0 = secondColumnValueMap.get(id0);
				String secondValue1 = secondColumnValueMap.get(id1);
				cv = compare(secondValue0, secondValue1, secondColumnType);
				if(Boolean.FALSE.equals(secondColumnAsc) ){
					cv = - cv;
				}
			}
			return cv;
		}
		
		private int compare(String value0, String value1, DataColumnType type){
			if(columnType.equals(DataColumnType.STRING) ){
				return value0.compareTo(value1);
			}else{
				Double d0 = Double.valueOf(value0);
				Double d1 = Double.valueOf(value1);
				return d0.compareTo(d1);
			}
		}
		
		public DataComparator(Map<String, String> columnValueMap, DataColumnType columnType, boolean columnAsc, Map<String, String> secondColumnValueMap, DataColumnType secondColumnType, boolean secondColumnAsc){
			this.columnValueMap = columnValueMap;
			this.columnType = columnType;
			this.columnAsc = columnAsc;
			this.secondColumnValueMap = secondColumnValueMap;
			this.secondColumnType = secondColumnType;
			this.secondColumnAsc = secondColumnAsc;
		}
	}
}
