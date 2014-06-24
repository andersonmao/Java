package data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import data.DataColumn.DataColumnCriterion;
import data.DataColumn.DataColumnOperator;

/**
 * @author Anderson Mao, 2014-06-17
 */
public class DataSql {
	private static void debug(String message){
		//System.out.println((new Date()).toString()+": "+DataSql.class.getSimpleName()+": "+message);
	}
	
	private static final char CHAR_QUOTE       = '\'';
	private static final String TOKEN_QUOTE    = String.valueOf(CHAR_QUOTE);
	private static final String TOKEN_NOT      = "NOT";
	private static final String TOKEN_LIKE     = "LIKE";
	private static final String TOKEN_NOT_LIKE = "NOTLIKE";
	
	private static final Map<String, DataColumnOperator> COLUMN_STRING_OPERATOR_MAP = new HashMap<String, DataColumnOperator>();
	static{
		COLUMN_STRING_OPERATOR_MAP.put(TOKEN_LIKE, DataColumnOperator.LIKE);
		COLUMN_STRING_OPERATOR_MAP.put(TOKEN_NOT_LIKE, DataColumnOperator.NOT_LIKE);
	}
	
	private static final Map<String, DataColumnOperator> COLUMN_SEPARATOR_OPERATOR_ONE_MAP = new HashMap<String, DataColumnOperator>();
	static{
		COLUMN_SEPARATOR_OPERATOR_ONE_MAP.put("=", DataColumnOperator.EQ);
		COLUMN_SEPARATOR_OPERATOR_ONE_MAP.put(">", DataColumnOperator.GT);
		COLUMN_SEPARATOR_OPERATOR_ONE_MAP.put("<", DataColumnOperator.LT);
	}
	
	private static final Map<String, DataColumnOperator> COLUMN_SEPARATOR_OPERATOR_TWO_MAP = new HashMap<String, DataColumnOperator>();
	static{
		COLUMN_SEPARATOR_OPERATOR_TWO_MAP.put("<>", DataColumnOperator.NOT_EQ);
		COLUMN_SEPARATOR_OPERATOR_TWO_MAP.put("!=", DataColumnOperator.NOT_EQ);
		COLUMN_SEPARATOR_OPERATOR_TWO_MAP.put(">=", DataColumnOperator.GE);
		COLUMN_SEPARATOR_OPERATOR_TWO_MAP.put("<=", DataColumnOperator.LE);
	}
	
	/**
	 * Used to merge two tokens into one
	 * @see COLUMN_SEPARATOR_OPERATOR_TWO_MAP and COLUMN_STRING_OPERATOR_MAP
	 */
	private static final Map<String, String> COLUMN_OPERATOR_TWO_MERGE_MAP = new HashMap<String, String>();
	static{
		COLUMN_OPERATOR_TWO_MERGE_MAP.put("<", ">");
		COLUMN_OPERATOR_TWO_MERGE_MAP.put("!", "=");
		COLUMN_OPERATOR_TWO_MERGE_MAP.put(">", "=");
		COLUMN_OPERATOR_TWO_MERGE_MAP.put("<", "=");
		COLUMN_OPERATOR_TWO_MERGE_MAP.put(TOKEN_NOT, TOKEN_LIKE);
	}
	
	private static final Map<String, DataColumnOperator> COLUMN_SEPARATOR_OPERATOR_MAP = new HashMap<String, DataColumnOperator>();
	static{
		COLUMN_SEPARATOR_OPERATOR_MAP.putAll(COLUMN_SEPARATOR_OPERATOR_ONE_MAP);
		COLUMN_SEPARATOR_OPERATOR_MAP.putAll(COLUMN_SEPARATOR_OPERATOR_TWO_MAP);
	}
	
	private static final List<String> SEPARATOR_LIST = new ArrayList<String>();
	static{
		// Two operators have high priority than one operators
		SEPARATOR_LIST.addAll(COLUMN_SEPARATOR_OPERATOR_TWO_MAP.keySet() );
		SEPARATOR_LIST.addAll(COLUMN_SEPARATOR_OPERATOR_ONE_MAP.keySet() );
		// Brackets
		SEPARATOR_LIST.add("(");
		SEPARATOR_LIST.add(")");
	}
	
	public static enum DataSqlOperator{
		AND,
		OR
	}
	
	/**
	 * DataSqlNode can be either a DataColumnCriterion or two DataSqlNode with operator
	 */
	public static class DataSqlNode{
		protected DataSqlOperator operator      = null;
		protected DataSqlNode leftNode          = null;
		protected DataSqlNode rightNode         = null;
		
		protected DataColumnCriterion criterion = null;
		
		DataSqlNode(DataColumnCriterion criterion){
			this.criterion = criterion;
		}
		
		DataSqlNode(DataSqlOperator operator, DataSqlNode leftNode, DataSqlNode rightNode){
			this.operator = operator;
			this.leftNode = leftNode;
			this.rightNode = rightNode;
		}
		
		@Override
		public String toString(){
			if(criterion!=null){
				return criterion.toString();
			}else{
				return "("+leftNode.toString()+") "+operator.name()+" ("+rightNode.toString()+")";
			}
		}
	}
	
	public static DataSqlNode parseSql(String sql){
		DataSqlNode root = null;
		if(sql==null || sql.equals("") ){
			return root;
		}
		debug("parse: separate");
		// Quote has high priority
		List<String> quoteList = parseQuoteList(sql);
		debug("quote: "+quoteList.toString() );
		List<String> tokenList = new ArrayList<String>();
		for(String qs: quoteList){
			if(qs.startsWith(TOKEN_QUOTE) ){
				tokenList.add(qs);
			}else{
				String[] sArray = qs.split("\\s+");
				for(String s: sArray){
					if(s.equals("") ){
						continue;
					}
					List<String> sList = separate(s);
					tokenList.addAll(sList);
				}
			}
		}
		debug("parse: tokens: "+tokenList.toString() );
		// Convert all tokens to upper case except string enclosed by ''
		List<String> upperTokenList = new ArrayList<String>();
		for(String s: tokenList){
			if(s.indexOf(TOKEN_QUOTE)>=0){
				upperTokenList.add(s);
			}else{
				upperTokenList.add(s.toUpperCase() );
			}
		}
		// Merge two tokens into one token. Example: merge NOT LIKE into NOTLIKE
		List<String> mergeTokenList = merge(upperTokenList);
		debug("parse: run");
		root = parseNode(mergeTokenList);
		debug("parse: done");
		//
		return root;
	}
	
	/**
	 * Create DataSqlNode from list of DataColumnCriterion with same DataSqlOperator
	 */
	public static DataSqlNode createNode(List<DataColumnCriterion> criterionList, DataSqlOperator operator){
		DataSqlNode dataSqlNode = null;
		if(criterionList==null || criterionList.size()==0){
			return dataSqlNode;
		}
		for(int i=0; i<criterionList.size(); i++){
			DataColumnCriterion crit = criterionList.get(i);
			if(dataSqlNode == null){
				dataSqlNode = new DataSqlNode(crit);
			}else{
				DataSqlNode rightNode = new DataSqlNode(crit);
				dataSqlNode = new DataSqlNode(operator, dataSqlNode, rightNode);
			}
		}
		//
		return dataSqlNode;
	}
	
	/**
	 * Separate into quote string '' or string without quote
	 */
	private static List<String> parseQuoteList(String value){
		List<String> tokenList = new ArrayList<String>();
		int strPos = 0;
		int quotePos = -1;
		int len = value.length();
		for(int i=0; i<len; i++){
			char c = value.charAt(i);
			if(c == CHAR_QUOTE){
				if(quotePos >=0){
					String s = value.substring(quotePos, i+1);
					tokenList.add(s);
					quotePos = -1;
					strPos = i+1;
				}else{
					if(i > strPos){
						String s = value.substring(strPos, i);
						tokenList.add(s);
					}
					quotePos = i;
				}
			}
		}
		// Last
		if(quotePos >=0){
			throw new DataException("Invalid quote '' in string");
		}else{
			if(len > strPos){
				String s = value.substring(strPos, len);
				tokenList.add(s);
			}
		}
		//
		return tokenList;
	}
	
	/**
	 * Main logic to parse tokens into DataSqlNode
	 */
	private static DataSqlNode parseNode(List<String> tokenList) throws DataException{
		DataSqlNode node = null;
		//
		int tokenSize = tokenList.size();
		int pos = 0;
		String columnToken         = null;
		String columnOperatorToken = null;
		
		DataSqlOperator operator   = null;
		DataSqlNode leftNode       = null;
		while(true){
			if(pos >= tokenSize ){
				break;
			}
			String token = tokenList.get(pos);
			DataSqlNode newNode = null;
			if(token.equals("(") ){
				int nextPos = pos+1;
				if(nextPos >= tokenSize){
					throw new DataException("Can not find end bracket ) for (");
				}
				int level = 0;
				int endBracketPos = -1;
				for(int i=nextPos; i<tokenSize; i++){
					String nextToken = tokenList.get(i);
					if(nextToken.equals("(") ){
						level++;
					}else if(nextToken.equals(")") ){
						if(level==0){
							endBracketPos = i;
							break;
						}else{
							level--;
						}
					}
				}
				if(endBracketPos == -1){
					throw new DataException("Can not find end bracket ) for ( before "+tokenList.get(nextPos) );
				}
				List<String> subList = tokenList.subList(nextPos, endBracketPos);
				newNode = parseNode(subList);
				// Skip to end of bracket
				pos = endBracketPos;
			}else{
				if(leftNode!=null && operator==null){
					operator = getDataSqlOperator(token);
				}else if(columnToken == null){
					columnToken = token;
				}else if(columnOperatorToken == null){
					columnOperatorToken = token;
				}else{
					String valueToken = token;
					DataColumnCriterion crit = new DataColumnCriterion(columnToken, getDataColumnOperator(columnOperatorToken), getTokenValue(valueToken));
					newNode = new DataSqlNode(crit);
					// Reset after create new node
					columnToken         = null;
					columnOperatorToken = null;
				}
			}
			if(newNode !=null){
				if(leftNode != null && operator !=null){
					DataSqlNode newLeftNode = new DataSqlNode(operator, leftNode, newNode);
					leftNode = newLeftNode;
					// Reset after create new node
					operator = null;
				}else if(leftNode == null){
					leftNode = newNode;
				}else{
					throw new DataException("Invalid operator");
				}
			}
			// Next
			pos++;
		}
		// Last
		if(operator !=null){
			throw new DataException("Invalid operator "+operator);
		}else if(columnOperatorToken !=null){
			throw new DataException("Invalid column operator "+columnOperatorToken);
		}else if(columnToken != null){
			throw new DataException("Invalid column "+columnToken);
		}else if(leftNode != null){
			node = leftNode;
		}
		//
		return node;
	}
	
	private static DataColumnOperator getDataColumnOperator(String token) throws DataException{
		if(COLUMN_SEPARATOR_OPERATOR_MAP.containsKey(token) ){
			return COLUMN_SEPARATOR_OPERATOR_MAP.get(token);
		}else if(COLUMN_STRING_OPERATOR_MAP.containsKey(token) ){
			return COLUMN_STRING_OPERATOR_MAP.get(token);
		}else{
			throw new DataException("Can not find valid column operator from token "+token);
		}
	}
	
	private static DataSqlOperator getDataSqlOperator(String token) throws DataException{
		DataSqlOperator op = null;
		try{
			op = DataSqlOperator.valueOf(token);
			return op;
		}catch(IllegalArgumentException ex){
			throw new DataException("Can not find AND/OR from token "+token);
		}
	}
	
	private static String getTokenValue(String token){
		String value = null;
		if(token.indexOf(TOKEN_QUOTE)>=0){
			if(token.indexOf(TOKEN_QUOTE)==0 && token.length()>=2 && token.lastIndexOf(TOKEN_QUOTE) == (token.length()-1) ){
				value = token.substring(1, token.length()-1 );
				return value;
			}
			throw new DataException("Invalid string value for token "+token);
		}else{
			return token;
		}
	}
	
	/**
	 * Lexical analysis into atomic tokens
	 */
	private static List<String> separate(String value){
		List<String> tokenList = new ArrayList<String>();
		tokenList.add(value);
		while(true){
			List<String> newList = new ArrayList<String>();
			for(String token: tokenList){
				boolean hasSep = false;
				for(String sep: SEPARATOR_LIST){
					if(sep.equals(token) ){// Skip separator self
						break;
					}
					int pos = token.indexOf(sep);
					if(pos>=0){
						hasSep = true;
						if(pos>0){
							String left = token.substring(0, pos);
							newList.add(left);
						}
						newList.add(sep);
						int rightPos = pos + sep.length();
						if(rightPos < token.length() ){
							String right = token.substring(rightPos );
							newList.add(right);
						}
						// Only separate max once for one string in one round.
						break;
					}
				}
				if(!hasSep){
					newList.add(token);
				}
			}
			if(newList.size() == tokenList.size() ){
				break;
			}
			// Next
			tokenList = newList;
		}
		return tokenList;
	}
	
	/**
	 * Merge two operator tokens into one. 
	 * Example: 
	 *   "NOT LIKE" into "NOTLIKE"
	 *   "> =" into ">="
	 */
	private static List<String> merge(List<String> tokenList) throws DataException{
		List<String> newList = new ArrayList<String>();
		//
		int tokenSize = tokenList.size();
		int pos = 0;
		while(true){
			if(pos >= tokenSize){
				break;
			}
			String token = tokenList.get(pos);
			boolean hasMerge = false;
			for(String op: COLUMN_OPERATOR_TWO_MERGE_MAP.keySet() ){
				String nextOp = COLUMN_OPERATOR_TWO_MERGE_MAP.get(op);
				if(token.equals(op) ){
					int nextPos = pos+1;
					if((nextPos) < tokenSize){
						String nextToken = tokenList.get(nextPos);
						if(nextToken.equals(nextOp) ){
							hasMerge = true;
							newList.add(op+nextOp);
							// Skip next
							pos++;
							break;
						}
					}
				}
			}
			if(!hasMerge){
				if(token.equals(TOKEN_NOT) ){
					throw new DataException("Can not find LIKE after NOT token");
				}else if(token.equals("!")){
					throw new DataException("Can not find = after ! token");
				}else{
					newList.add(token);
				}
			}
			// Next
			pos++;
		}
		//
		return newList;
	}
}
