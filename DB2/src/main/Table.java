package main;

import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;


public class Table {
int max; // max number of rows in each page
int pages; // number of pages in table
String name; // table name
String clustering; // primary/clustering key name
Hashtable<String,String> columnsType; // data types of rows
Hashtable<String,String> columnsMax; // max value in rows
Hashtable<String,String> columnsMin; // minimum value in rows
Vector<Hashtable<String, Object>> current; // vector to load data, actually change it to array list
ArrayList<String> fileNames;
ArrayList<String> keySets; // set of keys ordered same as metadata file
ArrayList<String> indexCol;

  public Table (String strTableName,String strClusteringKeyColumn, Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax,int MaxR ) {
	  name= strTableName;
	  clustering= strClusteringKeyColumn;
	  current = new Vector<Hashtable<String, Object>>();
	  
	  Hashtable<String,String> tmp = new Hashtable<String,String>();
	  Iterator<String> I = htblColNameType.keySet().iterator();
	  while(I.hasNext()) {
			String curr = I.next();
			tmp.put(curr, htblColNameType.get(curr));
		}
	  columnsType=tmp;
	  
	  Hashtable<String,String> tmp2 = new Hashtable<String,String>();
	  Iterator<String> I2 = htblColNameMin.keySet().iterator();
	  while(I2.hasNext()) {
			String curr = I2.next();
			tmp2.put(curr, htblColNameMin.get(curr));
		}
	  columnsMin=tmp2;
	  
	  Hashtable<String,String> tmp3 = new Hashtable<String,String>();
	  Iterator<String> I3 = htblColNameMax.keySet().iterator();
	  while(I3.hasNext()) {
			String curr = I3.next();
			tmp3.put(curr, htblColNameMax.get(curr));
		}
	  columnsMax=tmp3;
	  
	  max=MaxR;	  
	  fileNames= new ArrayList<String>();
	  keySets= new ArrayList<String>();
	  indexCol= new ArrayList<String>();
  }
  
  
  public void setFiles(ArrayList<String> f) {
	  for(int i = 0;i<f.size();i++) {
			 fileNames.add(f.get(i));
		 }
  }
  
  
  public void setIndex(ArrayList<String> n) {
	  for(int i = 0;i<n.size();i++) {
		  indexCol.add(n.get(i));
		 }
  }
  
  
 public void setKeys(ArrayList<String> k) {
	 for(int i = 0;i<k.size();i++) {
		 keySets.add(k.get(i));
	 }
 }
  
  // writes what's in the vector unto a csv file (csv file is the page i use it interchangeably )
  public void writeCSV(String FileName) {
	  try {
	  File Folder = new File("data");
	     if (!Folder.exists()) {
	         Folder.mkdir();
	     }
	  FileWriter fileWriter = new FileWriter(Folder.getAbsolutePath() + "/" + FileName + ".csv",false); 
	  
	  for (Hashtable<String,Object> R : current) { 
		  String str = "";
		  for(int i = 0;i<keySets.size();i++) { // loops on all list of keys in the input row and compare with each hashtable
			  String Column = keySets.get(i);
			  if(R.get(Column) instanceof Date) {
				  Date dr = ((Date)R.get(Column));
				  DateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd");
				  String dt = dateFormat.format(dr);
				  str=str + dt + "," ;
				  
			  }
			  else
				  str = str + R.get(Column) + ",";
		  }
		  str = str.substring(0, str.length() - 1);
		  
	      fileWriter.write(str + System.lineSeparator()); 
	  } 
	  fileWriter.close();
	  }
	  catch(IOException e) {
		  e.printStackTrace();
	  }
  }
  
  // reads a csv file and puts it in a vector
  public void readCSV(String FileName) {
	  try {
	  current = new Vector<Hashtable<String, Object>>(); 
	  File Folder = new File("data");
	     if (!Folder.exists()) {
	         Folder.mkdir();
	     }
	  FileReader fileReader = new 
	  FileReader( Folder.getAbsolutePath() + "/" + FileName + ".csv" ); // change this to the path of the file you want
	  BufferedReader reader = new BufferedReader( fileReader ); 
	  String strCurrentLine = ""; 
	  while( (strCurrentLine = reader.readLine() ) != null ) {
		  String[] c = strCurrentLine.split(","); // the size of the array is the same as size of hashtable columnsType
		  Hashtable<String,Object> readRow = new Hashtable<String,Object>();
		  for(int i=0;i<c.length;i++) {
			  String key = keySets.get(i);
			  
			  if(!c[i].toLowerCase().equals("null")) {
				  if(columnsType.get(key).toLowerCase().equals("java.lang.integer") ) {
					  readRow.put(key, new Integer(c[i])); //ask about this
				  }
				  else if (columnsType.get(key).toLowerCase().equals("java.lang.double") ) {
					  readRow.put(key, new Double(c[i]));
				  }
				  else if (columnsType.get(key).toLowerCase().equals("java.lang.string") ) {
					  readRow.put(key, c[i]);
				  }
				  else {
					  String[] date = c[i].split("-");
					  Date dInsert = new Date();
					  dInsert.setYear(Integer.parseInt(date[0]) - 1900);
					  dInsert.setMonth(Integer.parseInt(date[1]) - 1);
					  dInsert.setDate(Integer.parseInt(date[2]));
					  readRow.put(key, dInsert);
				  }
			  }
		  }
		  
		  current.add(readRow);
	  }
	 }
	  catch (IOException e) {
	         e.printStackTrace();
	      }
  }
  
  
  public boolean insert(Hashtable<String,Object> htblColNameValue) throws DBAppException {
	  boolean flag = false;
	  if(this.check(htblColNameValue) == true) {
		  if(pages == 0) {
				current.add(htblColNameValue);
				fileNames.add(name + pages);
				this.writeCSV(name + pages);
				pages++;
				readCSV(name + 0);
				flag = true;
			}
		// if less than maximum in table i insert in the current then serialize in the file
		  // if more i keep the same value and go into the loop until i find a page with size less than 200
		  // if no available pages at the end make one.
		  else {
			  if(this.check(htblColNameValue) == true) {
				  ArrayList<String> TheFiles = new ArrayList<String>();
				  if(indexCol.contains(clustering)) {
					  TheFiles = this.searchIndex(clustering, htblColNameValue.get(clustering) , "=");
				  }
				  else
					  TheFiles = fileNames;
				  if(TheFiles.isEmpty()) {
					  TheFiles = fileNames;
				  }
			  Hashtable<String,Object> CarryForwardRow; // if i want to insert in a page but find that count is more than 200 then call insert method with this
			  Hashtable<String,Object> InsertRow = htblColNameValue ;
			  for(String file: TheFiles) {
				  this.readCSV(file);
				  if(compare(InsertRow,current.get(current.size()-1), clustering) > 0 ) {
					  if(compare(InsertRow,current.get(current.size()-1), clustering) == 2 ) {
						  throw new DBAppException("clustering key value already exists"); 
					  }
					  if(current.size()>= max) {
						  CarryForwardRow = current.remove(current.size()-1);
						  current.add(InsertRow);
						  current = this.sort(current, clustering);
						  this.CheckUnique();
						  this.writeCSV(file);
						  current = new Vector<Hashtable<String,Object>>();
						  InsertRow = CarryForwardRow;
					  }
					  else {
						  current.add(InsertRow);
						  current = this.sort(current,clustering);
						  this.CheckUnique();
						  this.writeCSV(file);
						  current = new Vector<Hashtable<String, Object>>();
						  InsertRow = null;
					  }
				  }
				  else if(compare(InsertRow,current.get(current.size()-1), clustering) < 0) {
						  if(current.size() >= max) {
							  this.writeCSV(file);
							  current = new Vector<Hashtable<String,Object>>();
						  }
						  else {
							  current.add(InsertRow);
							  current = this.sort(current, clustering);
							  this.CheckUnique();
							  this.writeCSV(file);
							  current = new Vector<Hashtable<String, Object>>();
							  InsertRow = null;
						  }
					  }
				  if(InsertRow == null)
					  break;
			  }
			  if(InsertRow != null) { // if all pages full and no place to insert
				  current.add(InsertRow);
				  fileNames.add(name + pages);
				  this.writeCSV(name + pages); 
				  pages++;
				  flag = true;
			  }
		  }
			  int init = indexCol.size();
			  for(int i = 0 ;i< init; i++) {
				  String Coloumn = indexCol.get(i);
				  if(clustering.equals(Coloumn))
					  this.createIndexClustering(Coloumn);
				  else
					  this.createIndexNon(Coloumn);
			  }
	  }
	  }
		  else {
			throw new DBAppException("Incorrect row format inserted");
		}
	  return flag;
  }
  
  // the above for loop is repeated the amount of times the row has column names (they are the keys)
  // change the clustering with the key
  // so the deletion will be as follows, loop over each page and compare each row with the input row
  // if the row satisfies all conditions then add it or its index to an arraylist or vector 
  // when end of page is reached, you then remove it (this still not handled)
  public boolean delete(Hashtable<String,Object> row) throws DBAppException {
	  boolean fd = false;
	  ArrayList<String> IndexColomn = new ArrayList<String>();
	  Iterator<String> colTypes= row.keySet().iterator();
		while(colTypes.hasNext()) {
			String key = colTypes.next();
			if(deleteCheck(key,row.get(key)) == false)
				return fd;
			if(indexCol.contains(key))
				IndexColomn.add(key);
			
		}
		 ArrayList<String> pages = new ArrayList<String>();
		 for(String index: IndexColomn) {
			 if(IndexColomn.indexOf(index) == 0)
				 pages = this.searchIndex(index, row.get(index), "=");
			 else
				 pages = this.arrayOperator(pages, this.searchIndex(index, row.get(index), "="), "AND");
		 }
		  if(pages.isEmpty()) {
			  pages = fileNames;
		  }
	  for(String file: pages) {
		  this.readCSV(file);
		  Vector<Hashtable<String,Object>> temp = new Vector<Hashtable<String,Object>>();
		  for(int j=0;j<current.size();j++) {
			  boolean flag = true;
			  Iterator<String> it = row.keySet().iterator(); // list of keys in input row
			  while(it.hasNext()) { // loops on all list of keys in the input row and compare with each hashtable
				  String Coloumn = it.next();
				  if(compare(current.get(j),row,Coloumn) != 2) {
					  flag = false;
					  break; //if false then stops checking rest of conditions in hashtable
				  }
			  }
			  if(flag) {
				  temp.add(current.get(j)); 
			  }
		  }
		  if(current.size() == temp.size())
			  fd = true;
		  current.removeAll(temp);
		  this.writeCSV(file);
	  }
	  
	  int init = indexCol.size();
	  
	  for(int i = 0 ;i< init; i++) {
		  String Coloumn = indexCol.get(i);
		  if(clustering.equals(Coloumn))
			  this.createIndexClustering(Coloumn);
		  else
			  this.createIndexNon(Coloumn);
	  }
	  
	  if(fd)
		  this.RemoveEmpty();
	  return fd;
  }
  
  public boolean deleteCheck(String key, Object c) throws DBAppException {
	boolean flag = false;
	if(!keySets.contains(key)) {
		throw new DBAppException("the column name does not exist in the table");
	}
	if(columnsType.get(key).toLowerCase().equals("java.lang.integer") && c instanceof Integer && (int)c >= Integer.parseInt(columnsMin.get(key)) && (int)c <= Integer.parseInt(columnsMax.get(key))) {
		flag = true;
    }
    if(columnsType.get(key).toLowerCase().equals("java.lang.string") && c instanceof String && ((String)c).compareTo(columnsMin.get(key)) >= 0 && ((String)c).compareTo(columnsMax.get(key)) <= 0 ) {
		flag = true;
    }
    if(columnsType.get(key).toLowerCase().equals("java.lang.double") && c instanceof Double && (Double)c >= Double.parseDouble(columnsMin.get(key)) && (Double)c <= Double.parseDouble(columnsMax.get(key))) {
		flag = true;
    }
    if(columnsType.get(key).toLowerCase().equals("java.lang.date") && c instanceof Date ) {
    	Date dateMin = new Date(columnsMin.get(key)); // check other methods to parse stinger to date
    	Date dateMax = new Date(columnsMax.get(key));
    	Date currentD = (Date) c;
    	if(currentD.compareTo(dateMin) >= 0 && currentD.compareTo(dateMax) <= 0) {
    		flag = true;
    	}
    }
	
	return flag;
} 


public void update(String clusteringValue, Hashtable<String,Object> row) throws DBAppException {
	  if(row.get(clustering) != null) {
		  throw new DBAppException("you can not update a clustering key");
	  }
	  Object v = new Object();
	  if(columnsType.get(clustering).toLowerCase().equals("java.lang.integer") ) { // add clusteringValue to the row input
		  row.put(clustering, new Integer(clusteringValue));
		  v = new Integer(clusteringValue);
	  }
	  if(columnsType.get(clustering).toLowerCase().equals("java.lang.string") ) {
		  row.put(clustering, new String(clusteringValue));
		  v = new String(clusteringValue);
	  }
	  if(columnsType.get(clustering).toLowerCase().equals("java.lang.double") ) {
		  row.put(clustering, new Double(clusteringValue));
		  v = new Double(clusteringValue);
	  }
	  if(columnsType.get(clustering).toLowerCase().equals("java.lang.date") ) {
		  row.put(clustering, new Date(clusteringValue));
		  v = new Date(clusteringValue);
	  }
	  if(this.check(row) == true) {
		  ArrayList<String> pages = new ArrayList<String>();
		  if(indexCol.contains(clustering)) {
			  pages = this.searchIndex(clusteringValue, v , "=");
		  }
		  else {
			  pages = fileNames;
		  }
		  if(pages.isEmpty()) {
			  pages = fileNames;
		  }
	  for(String file: pages) {
		  this.readCSV(file);
		  int UpdateIndex= -1;  // row to be updated
		  if(compare(row,current.get(current.size()-1), clustering) > 0) {
			  //int UpdateIndex= -1;  // row to be updated			  
			  for(int j = 0;j<current.size();j++) { // loop on page to get row to be updated
				  if(compare(row,current.get(j), clustering) == 2){
					  UpdateIndex = j;
				  }
			  }
			  if(UpdateIndex == -1) {
				  throw new DBAppException("the clustering key does not exist");
			  }
			  
			  Iterator<String> it = row.keySet().iterator(); // list of keys in input row
			  while(it.hasNext()) { // loops on all list of keys in the input row and replace the old row values
				  String Coloumn = it.next();
				  if(current.get(UpdateIndex).get(Coloumn) != null) {
					  (current.get(UpdateIndex)).replace(Coloumn, row.get(Coloumn));
				  }
				  else {
					  current.get(UpdateIndex).put(Coloumn, row.get(Coloumn));
				  }
			  }
			  this.writeCSV(file);
			  break;
		  }
		  else { // if the row we are looking for isn't in this page
			  this.writeCSV(file); // maybe just empty the vector since the page is not touched?
			  current = new Vector<Hashtable<String,Object>>();
		  }
		  if( file.equals(fileNames.get(fileNames.size()-1)) && UpdateIndex == -1) {
			  throw new DBAppException("the clustering key does not exist");
		  }
	    }
	  Iterator<String> it = row.keySet().iterator(); // list of keys in input row
	  while(it.hasNext()) {
		  String Coloumn = it.next();
		  if(indexCol.contains(Coloumn) && clustering.equals(Coloumn))
			  this.createIndexClustering(Coloumn);
		  else if (indexCol.contains(Coloumn))
			  this.createIndexNon(Coloumn);
	  }
	  
	  }
	  else {
		  throw new DBAppException("the values inputted are invalid");
	  }
  }
  
  //returns -1 if r1 > r2. returns 1 if r1 < r2.returns 2 if equal. returns 0 if not compatible types or null (compareTo method)
  public int compare(Hashtable<String,Object> r1,Hashtable<String,Object> r2, String col) {
	  int x=0;
	  if(r1.get(col) == null || r2.get(col) == null)
		  x=0;
	  if(r1.get(col) instanceof Integer && r2.get(col) instanceof Integer) {
		  if( (int)(r1.get(col) ) > (int) (r2.get(col)))
			  x=-1;
		  else if( (int)(r1.get(col) ) < (int) (r2.get(col)))
			  x=1;
		  else if( (int)(r1.get(col) ) == (int) (r2.get(col)))
			  x=2;
	  }
	  
	  if(r1.get(col) instanceof Double && r2.get(col) instanceof Double) {
		  if( (double)(r1.get(col) ) > (double) (r2.get(col)))
			  x=-1;
		  else if( (double)(r1.get(col) ) < (double) (r2.get(col)))
			  x=1;
		  else if( (double)(r1.get(col) ) == (double) (r2.get(col)))
			  x=2;
	  }
	  
	  if(r1.get(col) instanceof String && r2.get(col) instanceof String) {
		 if(  ((String)(r1.get(col))).compareTo(((String)(r2.get(col)))) > 0)
			 x=-1;
		 else if( ((String)(r1.get(col))).compareTo(((String)(r2.get(col)))) == 0 )
			  x=2;
		 else
			 x=1;
	  }
	  
	  if(r1.get(col) instanceof Date && r2.get(col) instanceof Date) {
		  Date d1 = (Date) r1.get(col);
		  Date d2 = (Date) r2.get(col);
		 if( compareDate(d1,d2) > 0)
			 x=-1;
		 else if( compareDate(d1,d2) == 0 )
			  x=2;
		 else
			 x=1;
	  }
	  return x;
  }
  
	  public int compareDate(Date d1, Date d2) {
	    if (d1.getYear() != d2.getYear()) 
	        return d1.getYear() - d2.getYear();
	    if (d1.getMonth() != d2.getMonth()) 
	        return d1.getMonth() - d2.getMonth();
	    return d1.getDate() - d2.getDate();
	  }
  
  // bubble sort of a page
  public Vector<Hashtable<String,Object>> sort(Vector<Hashtable<String, Object>> page, String clustering){
	  for (int i = 0; i < page.size() - 1; i++) {
	        for (int j = 0; j < page.size() - i - 1; j++) {
	            if (this.compare(page.get(j), page.get(j+1), clustering) < 0) {
	            	page.add(j,page.remove(j+1));
	            }
	                
	        }
	    }
	  return page;
  }
  
  
  public void CheckUnique()throws DBAppException{
	  for(int i = 0; i < current.size()-1;i++){
		if(compare(current.get(i),current.get(i+1),clustering) == 2){
			throw new DBAppException("clustering key value already exists"); 
		}
	  }
  }
  
  
  
  
  
  
  
  
  
  
  // checks if the row to be inserted is correct regarding type,max,minimum and clustering key is present etc
  private boolean check(Hashtable<String, Object> col) {
//	  System.out.println("check");
	  boolean flag = true;
	  boolean cFlag = false; // to check if the row has a clustering column
	  Iterator<String> colTypes= col.keySet().iterator();
		while(colTypes.hasNext()) {
			String curr = colTypes.next();
			if(!keySets.contains(curr))
				return false;
			String T = columnsType.get(curr);
			String Min = columnsMin.get(curr);
			String Max = columnsMax.get(curr);
			Object c = col.get(curr);
			if(c != null) {
				if(checkHelper(c,T,Min,Max) == false) { //break if false
					flag = false;
					break;
				}
			}
			if(c == null && curr.compareTo(clustering) == 0) {
				cFlag = false;
				break;
			}
			if(curr.compareTo(clustering) == 0 && c!= null)
				cFlag = true;
		}
		return flag && cFlag;
}
  
  // might be an issue with the date part 
public boolean checkHelper(Object c, String colType, String min, String maxV) {
	boolean flag=false;
	
	if(colType.toLowerCase().equals("java.lang.integer") && c instanceof Integer && (int)c >= Integer.parseInt(min) && (int)c <= Integer.parseInt(maxV)) {
		flag = true;
    }
    if(colType.toLowerCase().equals("java.lang.string") && c instanceof String && ((String)c).toLowerCase().compareTo(min.toLowerCase()) >= 0 && ((String)c).toLowerCase().compareTo(maxV.toLowerCase()) <= 0 ) {
		flag = true;
    }
    if(colType.toLowerCase().equals("java.lang.double") && c instanceof Double && (Double)c >= Double.parseDouble(min) && (Double)c <= Double.parseDouble(maxV)) {
		flag = true;
    }
    if(colType.toLowerCase().equals("java.lang.date") && c instanceof Date ) {
    	Date dateMin = new Date(min); // check other methods to parse stinger to date
    	Date dateMax = new Date(maxV);
    	Date currentD = (Date) c;
    	if(currentD.compareTo(dateMin) >= 0 && currentD.compareTo(dateMax) <= 0) {
    		flag = true;
    	}
    }
	return flag;
    }


public void RemoveEmpty(){
	for(String f: fileNames) {
			  File Folder = new File("data");
			     if (!Folder.exists()) {
			         Folder.mkdir();
			     }
			     File check = new File( Folder.getAbsolutePath() + "/" + f + ".csv" );
			  if(check.length() == 0) {
				  fileNames.remove(f);
				  check.deleteOnExit();
	          }
	}
	System.gc();
}

public Iterator selectFromTable(SQLTerm[] SQLTerms,String[] Operators) {
	Vector<Hashtable<String,Object>> result = new Vector<Hashtable<String,Object>>();
	ArrayList<String> pages = new ArrayList<String>();
		for(SQLTerm term: SQLTerms) {
			int i = 0;
			if( indexCol.contains(term._strColumnName) ) {
				 if(term.equals(SQLTerms[0])) {
					 //System.out.println("1");
					 pages = this.searchIndex(term._strColumnName, term._objValue, term._strOperator);
					 //System.out.println(pages);
				 }
				 else {
					 //System.out.println("2");
					 pages = this.arrayOperator(pages, this.searchIndex(term._strColumnName, term._objValue, term._strOperator), Operators[i]);
				 }
			   }
				 else {
						if(term.equals(SQLTerms[0])) {
							//System.out.println("3");
							 pages = fileNames;
						}
						 else {
							 //System.out.println("4");
							 pages = this.arrayOperator(pages, fileNames , Operators[i]);
						 }
					}
			i++;
			}
			
			
		 
	  if(pages.isEmpty()) {
		  pages = fileNames;
	  }
	  //System.out.println(fileNames);
	  //System.out.println(pages);
	for(String f:pages) {
		readCSV(f);
		
		for(int i = 0;i< current.size();i++) {
			// code to compare a single column
			boolean flag = false;
			// if only one condition in select
			if(SQLTerms.length == 1) {
				flag = checkSQL(SQLTerms[0],current.get(i));
			}
			else { //if multiple conditions with operators between
				for(int j = 0 ;j<Operators.length;j++) {
					if(current.get(i).get(SQLTerms[j]._strColumnName) != null && current.get(i).get(SQLTerms[j+1]._strColumnName) != null) {
						if(Operators[j].equals("OR")) {
							flag = checkSQL(SQLTerms[j],current.get(i)) || checkSQL(SQLTerms[j+1],current.get(i));
							//System.out.println(i + "_________" + current.get(i).get("name") + "___________" + flag);
						}
						if(Operators[j].equals("AND")) {
							flag = checkSQL(SQLTerms[j],current.get(i)) && checkSQL(SQLTerms[j+1],current.get(i));
							//System.out.println(i + "_________" + current.get(i).get("name") + "___________" + flag);
						}
						if(Operators[j].equals("XOR")) {
							flag = checkSQL(SQLTerms[j],current.get(i)) ^ checkSQL(SQLTerms[j+1],current.get(i));
							//System.out.println(i + "_________" + current.get(i).get("name") + "___________" + flag);
						}
					}
					//else {
						/*if(Operators[j].equals("OR")) {
							flag = flag || checkSQL(SQLTerms[j],current.get(i)) || checkSQL(SQLTerms[j+1],current.get(i));
						}
						if(Operators[j].equals("AND")) {
							flag = flag && checkSQL(SQLTerms[j],current.get(i)) && checkSQL(SQLTerms[j+1],current.get(i));
						}
						if(Operators[j].equals("XOR")) {
							flag = flag ^ checkSQL(SQLTerms[j],current.get(i)) ^ checkSQL(SQLTerms[j+1],current.get(i));
						}*/
					//}    
				}
			}
			if(flag) {
				result.add(current.get(i));
			}
		}
		
		writeCSV(f);
	}
	return result.iterator();
}

// check this shit again
public boolean checkSQL(SQLTerm s , Hashtable<String,Object> row) {
	boolean flag = false;
	String col = s._strColumnName;
	if(columnsType.get(col).toLowerCase().equals("java.lang.integer") && s._objValue instanceof Integer) {
		if(s._strOperator.equals(">") && (int)row.get(col) > (int)s._objValue) {
	    	flag = true;
	    }
	    else if(s._strOperator.equals(">=") && (int)row.get(col) >= (int)s._objValue) {
	    	flag = true;
	    }
	    else if(s._strOperator.equals("<") && (int)row.get(col) < (int)s._objValue) {
	    	flag = true;
	    }
	    else if(s._strOperator.equals("<=") && (int)row.get(col) <= (int)s._objValue) {
	    	flag = true;
	    }
	    else if(s._strOperator.equals("=") && (int)row.get(col) == (int)s._objValue) {
	    	flag = true;
	    }
	    else if(s._strOperator.equals("!=") && (int)row.get(col) != (int)s._objValue) {
	    	flag = true;
	    }
    }
	else if(columnsType.get(col).toLowerCase().equals("java.lang.string") && s._objValue instanceof String) {
    	if(s._strOperator.equals(">") && ((String)row.get(col)).compareTo( (String)s._objValue) > 0 ) {
        	flag = true;
        }
        else if(s._strOperator.equals(">=") && ((String)row.get(col)).compareTo( (String)s._objValue) >= 0) {
        	flag = true;
        }
        else if(s._strOperator.equals("<") && ((String)row.get(col)).compareTo( (String)s._objValue) < 0) {
        	flag = true;
        }
        else if(s._strOperator.equals("<=") && ((String)row.get(col)).compareTo( (String)s._objValue) <= 0) {
        	flag = true;
        }
        else if(s._strOperator.equals("=") && ((String)row.get(col)).compareTo( (String)s._objValue) == 0) {
        	flag = true;
        }
        else if(s._strOperator.equals("!=") && ((String)row.get(col)).compareTo( (String)s._objValue) != 0) {
        	flag = true;
        }
    }
	else if(columnsType.get(col).toLowerCase().equals("java.lang.double") ) {
		
    	if(s._strOperator.equals(">") && (Double)row.get(col) > (Double)s._objValue ) {
        	flag = true;
        }
        else if(s._strOperator.equals(">=") && (Double)row.get(col) >= (Double)s._objValue) {
        	flag = true;
        }
        else if(s._strOperator.equals("<") && (Double)row.get(col) < (Double)s._objValue) {
        	flag = true;
        }
        else if(s._strOperator.equals("<=") && (Double)row.get(col) <= (Double)s._objValue) {
        	flag = true;
        }
        else if(s._strOperator.equals("=") && ((Double)row.get(col)).equals((Double)s._objValue)) {
        	flag = true;
        }
        else if(s._strOperator.equals("!=") && !((Double)row.get(col)).equals((Double)s._objValue)) {
        	flag = true;
        }
    }
	else if(columnsType.get(col).toLowerCase().equals("java.lang.date") ) {
		Date d2 = (Date) s._objValue;
		Date d1 = (Date) row.get(col);
    	if(s._strOperator.equals(">") && compareDate(d1,d2) > 0) {
        	flag = true;
        }
        else if(s._strOperator.equals(">=") && compareDate(d1,d2) >= 0) {
        	flag = true;
        }
        else if(s._strOperator.equals("<") && compareDate(d1,d2) < 0) {
        	flag = true;
        }
        else if(s._strOperator.equals("<=") && compareDate(d1,d2) <= 0) {
        	flag = true;
        }
        else if(s._strOperator.equals("=") && compareDate(d1,d2) == 0) {
        	flag = true;
        }
        else if(s._strOperator.equals("!=") && compareDate(d1,d2) != 0) {
        	flag = true;
        }
    }
	return flag;
}

// create an index on clustering column method
public void createIndexClustering(String col) {
	indexCol.add(col);
	ArrayList<String> temp = new ArrayList<String>();
	for(String file: fileNames) {
		  this.readCSV(file);
		  if(current.size() != 0) {
			  //System.out.println(current.size() + file);
			  if(current.get(0).get(col) instanceof Date) {
				  Date dr = ((Date)current.get(0).get(col));
				  DateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd");
				  String dt = dateFormat.format(dr);
				  temp.add(dt + "," + file);
			  }
			  else {
				  temp.add(current.get(0).get(col) + "," + file );
			  }
		  }
		  this.writeCSV(file);
	  }
	// write info in temp in a file and name it 
	try {
		  File Folder = new File("data");
		     if (!Folder.exists()) {
		         Folder.mkdir();
		     }
		  FileWriter fileWriter = new FileWriter(Folder.getAbsolutePath() + "/" + name + "_" + col + "_sparse" + ".csv",false); 
		  
		  for (String str : temp) { 
		      fileWriter.write(str + System.lineSeparator()); 
		  } 
		  fileWriter.close();
		  }
		  catch(IOException e) {
			  e.printStackTrace();
		  }
}

// create an index on non-clustering column method
public void createIndexNon(String col) {
	indexCol.add(col);
	// create first level
	ArrayList<String> tempSecondary = new ArrayList<String>();
	Vector<Hashtable<String,Object>> temp = new Vector<Hashtable<String,Object>>();
	for(String file: fileNames) {
		  this.readCSV(file);
		  if(current.size() != 0) {
			  for(int i =0;i<current.size();i++) {
				  if(current.get(i).get(col) != null) {
				  Hashtable<String,Object> value = new Hashtable<String,Object>();
				  value.put(col, current.get(i).get(col));
				  value.put("file", file);
				  temp.add(value);
				  }
			  }
		  }
		  this.writeCSV(file);
	  }
	this.sort(temp, col); // check if this sorts correctly
	// write the temp in a page
	try {
	    File Folder = new File("data");
	       if (!Folder.exists()) {
	           Folder.mkdir();
	       }
	    FileWriter fileWriter = new FileWriter(Folder.getAbsolutePath() + "/" + name + "_" + col + "_dense" + ".csv",false); 
	    
	    for (Hashtable<String,Object> R : temp) { 
	      String str = "";
	        if(R.get(col) instanceof Date) {
	          Date dr = ((Date)R.get(col));
	          DateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd");
	          String dt = dateFormat.format(dr);
	          str = str + dt + "," + R.get("file");
	        }
	        else
	          str = R.get(col) + "," + R.get("file");
	        fileWriter.write(str + System.lineSeparator()); 
	    } 
	    fileWriter.close();
	    }
	    catch(IOException e) {
	      e.printStackTrace();
	    }
	
	// create second level
	for(int j = 0;j<temp.size();j++) {
		if(j % max == 0) {
			if(temp.get(j).get(col) instanceof Date) {
				  Date dr = ((Date)temp.get(j).get(col));
				  DateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd");
				  String dt = dateFormat.format(dr);
				  tempSecondary.add(dt +","+ j + "," + name + "_" + col + "_dense");
			  }
			  else {
				  tempSecondary.add(temp.get(j).get(col) +","+ j + "," + name + "_" + col + "_dense");
			  }
		}
	}
    // iterate on the sorted vector and take every Max number of rows and write a single line	
	// write the array list in a page
	try {
		  File Folder = new File("data");
		     if (!Folder.exists()) {
		         Folder.mkdir();
		     }
		  FileWriter fileWriter = new FileWriter(Folder.getAbsolutePath() + "/" + name + "_" + col + "_sparse" + ".csv",false); 
		  
		  for (String str : tempSecondary) { 
		      fileWriter.write(str + System.lineSeparator()); 
		  } 
		  fileWriter.close();
		  }
		  catch(IOException e) {
			  e.printStackTrace();
		  }
}

// check the = cases in sparse
public ArrayList<String> searchIndex(String col , Object value , String operator){
	ArrayList<String> result = new ArrayList<String>();
	ArrayList<Integer> rows = new ArrayList<Integer>();
	if (col.equals(clustering)) {
		try {
			File Folder = new File("data");
		     if (!Folder.exists()) {
		         Folder.mkdir();
		     }
		  FileReader fileReader = new 
		  FileReader( Folder.getAbsolutePath() + "/" + name + "_" + col + "_sparse" + ".csv" ); // change this to the path of the file you want
		  BufferedReader reader = new BufferedReader( fileReader ); 
		  String strCurrentLine = "";
		  while( (strCurrentLine = reader.readLine() ) != null ) {
			  ArrayList<Object> v = new ArrayList<Object>();
			  String[] c = strCurrentLine.split(",");
			  if(columnsType.get(col).toLowerCase().equals("java.lang.integer") && value instanceof Integer) {
				  Integer n = new Integer(c[0]);
				  v.add(n);
				  if(operator.equals(">") && n > (Integer) value) {
					result.add(c[1]);
			         }
			      else if(operator.equals(">=") && n >= (Integer) value) {
						result.add(c[1]);

			         }
			      else if(operator.equals("<") && n < (Integer) value) {
						result.add(c[1]);

			         }
			      else if(operator.equals("<=") && n <= (Integer) value) {
						result.add(c[1]);

			         }
			      else if(operator.equals("=") && n == (Integer) value || (v.size() > 1 && n > (Integer)value && (Integer)v.get(v.size()-1) < (Integer)value) ) {
						result.add(c[1]);
			         }
			      else if(operator.equals("!=") && n != (Integer) value) {
						result.add(c[1]);
			         }
			  }
			  else if (columnsType.get(col).toLowerCase().equals("java.lang.double") && value instanceof Double) {
				  Double n = new Double(c[0]);
				  v.add(n);
				  if(operator.equals(">") && n > (Double) value) {
						result.add(c[1]);
				         }
				      else if(operator.equals(">=") && n >= (Double) value) {
							result.add(c[1]);

				         }
				      else if(operator.equals("<") && n < (Double) value) {
							result.add(c[1]);

				         }
				      else if(operator.equals("<=") && n <= (Double) value) {
							result.add(c[1]);

				         }
				      else if(operator.equals("=") && n.equals( (Double) value ) || (v.size() > 1 && n > (Double)value && (Double)v.get(v.size()-1) < (Double)value)) {
							result.add(c[1]);
				         }
				      else if(operator.equals("!=") && n != (Double) value) {
							result.add(c[1]);
				         }
			  }
			  else if (columnsType.get(col).toLowerCase().equals("java.lang.string") && value instanceof String) {
				  String n = c[0];
				  v.add(n);
				  if(operator.equals(">") && n.compareTo((String) value) > 0) {
						result.add(c[1]);

			         }
			      else if(operator.equals(">=") && n.compareTo((String) value) >= 0) {
						result.add(c[1]);

			         }
			      else if(operator.equals("<") && n.compareTo((String) value) < 0) {
						result.add(c[1]);

			         }
			      else if(operator.equals("<=") && n.compareTo((String) value) <= 0) {
						result.add(c[1]);

			         }
			      else if(operator.equals("=") && n.compareTo((String) value) == 0 || (v.size() > 1 && n.compareTo((String) value) > 0 && ((String)v.get(v.size()-1)).compareTo((String) value) < 0)) {
						result.add(c[1]);

			         }
			      else if(operator.equals("!=") && n.compareTo((String) value) != 0) {
						result.add(c[1]);

			         }
			  }
			  else {
				  String[] d = c[0].split("-");
				  Date n = new Date();
				  n.setYear(Integer.parseInt(d[0]) - 1900);
				  n.setMonth(Integer.parseInt(d[1]) - 1);
				  n.setDate(Integer.parseInt(d[2]));
				  v.add(n);
				  if(operator.equals(">") && compareDate(n,(Date) value) > 0) {
					  result.add(c[1]);
					  
				  }
				  else if(operator.equals(">=") && compareDate(n,(Date) value) >= 0) {
					  result.add(c[1]);
					  
				  }
				  else if(operator.equals("<") && compareDate(n,(Date) value) < 0) {
					  result.add(c[1]);
					  
				  }
				  else if(operator.equals("<=") && compareDate(n,(Date) value) <= 0) {
					  result.add(c[1]);
					  
				  }
				  else if(operator.equals("=") && compareDate(n,(Date) value) == 0 || (v.size() > 1 && compareDate(n,(Date) value) > 0 && compareDate((Date)v.get(v.size()-1),(Date) value) < 0)) {
					  result.add(c[1]);
					  
				  }
				  else if(operator.equals("!=") && compareDate(n,(Date) value) != 0) {
					  result.add(c[1]);
					  
				  }
			  }
		  }
		}
		catch(IOException e) {
			
		}
	}
	else {
		// search in sparse index
		//System.out.println("ALL ELSE");
		try {
			File Folder = new File("data");
		     if (!Folder.exists()) {
		         Folder.mkdir();
		     }
		  FileReader fileReader = new 
		  FileReader( Folder.getAbsolutePath() + "/" + name + "_" + col + "_sparse" + ".csv" ); // change this to the path of the file you want
		  BufferedReader reader = new BufferedReader( fileReader ); 
		  String strCurrentLine = "";
		  while( (strCurrentLine = reader.readLine() ) != null ) {
			  ArrayList<Object> v = new ArrayList<Object>();
			  String[] c = strCurrentLine.split(",");
			  if(columnsType.get(col).toLowerCase().equals("java.lang.integer") && value instanceof Integer) {
				  Integer n = new Integer(c[0]);
				  v.add(n);
				  if(operator.equals(">") && n > (Integer) value) {
					  rows.add(new Integer (c[1]));
			         }
			      else if(operator.equals(">=") && n >= (Integer) value) {
						rows.add(new Integer (c[1]));
			         }
			      else if(operator.equals("<") && n < (Integer) value) {
			    	  rows.add(new Integer (c[1]));

			         }
			      else if(operator.equals("<=") && n <= (Integer) value) {
			    	  rows.add(new Integer (c[1]));

			         }
			      else if(operator.equals("=") && n == (Integer) value || (v.size() > 1 && n > (Integer)value && (Integer)v.get(v.size()-1) < (Integer)value) ) {
			    	  rows.add(new Integer (c[1]));
			         }
			      else if(operator.equals("!=") && n != (Integer) value) {
			    	  rows.add(new Integer (c[1]));
			         }
			  }
			  else if (columnsType.get(col).toLowerCase().equals("java.lang.double") && value instanceof Double) {
				  Double n = new Double(c[0]);
				  v.add(n);
				  if(operator.equals(">") && n > (Double) value) {
					  rows.add(new Integer (c[1]));
				         }
				      else if(operator.equals(">=") && n >= (Double) value) {
				    	  rows.add(new Integer (c[1]));

				         }
				      else if(operator.equals("<") && n < (Double) value) {
				    	  rows.add(new Integer (c[1]));

				         }
				      else if(operator.equals("<=") && n <= (Double) value) {
				    	  rows.add(new Integer (c[1]));

				         }
				      else if(operator.equals("=") && n.equals((Double) value) || (v.size() > 1 && n > (Double)value && (Double)v.get(v.size()-1) < (Double)value)) {
				    	  rows.add(new Integer (c[1]));
				         }
				      else if(operator.equals("!=") && n != (Double) value) {
				    	  rows.add(new Integer (c[1]));
				         }
			  }
			  else if (columnsType.get(col).toLowerCase().equals("java.lang.string") && value instanceof String) {
				  String n = c[0];
				  v.add(n);
				  if(operator.equals(">") && n.compareTo((String) value) > 0) {
					  rows.add(new Integer (c[1]));

			         }
			      else if(operator.equals(">=") && n.compareTo((String) value) >= 0) {
			    	  rows.add(new Integer (c[1]));

			         }
			      else if(operator.equals("<") && n.compareTo((String) value) < 0) {
			    	  rows.add(new Integer (c[1]));

			         }
			      else if(operator.equals("<=") && n.compareTo((String) value) <= 0) {
			    	  rows.add(new Integer (c[1]));

			         }
			      else if(operator.equals("=") && n.compareTo((String) value) == 0 || (v.size() > 1 && n.compareTo((String) value) > 0 && ((String)v.get(v.size()-1)).compareTo((String) value) < 0)) {
			    	  rows.add(new Integer (c[1])); 
			         }
			      else if(operator.equals("!=") && n.compareTo((String) value) != 0) {
			    	  rows.add(new Integer (c[1]));

			         }
			  }
			  else {
				  String[] d = c[0].split("-");
				  Date n = new Date();
				  //System.out.println(c[0] + "_______________" + c[1]);
				  n.setYear(Integer.parseInt(d[0]) - 1900);
				  n.setMonth(Integer.parseInt(d[1]) - 1);
				  n.setDate(Integer.parseInt(d[2]));
				  v.add(n);
				  if(operator.equals(">") && compareDate(n,(Date) value) > 0) {
					  rows.add(new Integer (c[1]));
					  
				  }
				  else if(operator.equals(">=") && compareDate(n,(Date) value) >= 0) {
					  rows.add(new Integer (c[1]));
					  
				  }
				  else if(operator.equals("<") && compareDate(n,(Date) value) < 0) {
					  rows.add(new Integer (c[1]));
					  
				  }
				  else if(operator.equals("<=") && compareDate(n,(Date) value) <= 0) {
					  rows.add(new Integer (c[1]));
					  
				  }
				  else if(operator.equals("=") && compareDate(n,(Date) value) == 0 || (v.size() > 1 && compareDate(n,(Date) value) > 0 && compareDate((Date)v.get(v.size()-1),(Date) value) < 0)) {
					  rows.add(new Integer (c[1]));
					  
				  }
				  else if(operator.equals("!=") && compareDate(n,(Date) value) != 0) {
					  rows.add(new Integer (c[1]));
					  
				  }
			  }
		  }
		    FileReader fileReader2 = new 
			FileReader( Folder.getAbsolutePath() + "/" + name + "_" + col + "_dense" + ".csv" ); 
			BufferedReader reader2 = new BufferedReader( fileReader2 ); 
			String strCurrent = "";
			//System.out.println("Result: " + result);
			int index = 0; // position in dense index
			int z = 0; // position in rows array list
			int p = 0;
			while( (strCurrent = reader2.readLine() ) != null ) {
				
				String[] c = strCurrent.split(",");
				
				if(p < max && z < rows.size() && rows.get(z) == index ) { // see this again 
					p++;
					z++;
					result.add(c[1]);
				}
				else if(p == max) {
					p = 0;
				}
			   index++;
			  }
		}
		catch(IOException e) {
			
		}
	}
	return result;
}


public ArrayList<String> arrayOperator(ArrayList<String> r1,ArrayList<String> r2, String op){
	ArrayList<String> result = new ArrayList<String>();
	if(op.equals("AND")) {
		for(int i = 0;i<r1.size();i++) {
			if(r2.contains(r1.get(i))) {
				result.add(r1.get(i));
				//System.out.println(r1.get(i) + "________ the name of the page to be added");
			}
		}
	}
	else if(op.equals("OR")) {
		for(int i = 0;i<r1.size();i++) {
			if(!r2.contains(r1.get(i))) {
				result.add(r1.get(i));
			}
		}
		for(int j = 0;j<r2.size();j++) {
			result.add(r2.get(j));
		}
	}
	else if(op.equals("XOR")) {
		for(int i = 0;i<r1.size();i++) {
			if(!r2.contains(r1.get(i))) {
				result.add(r1.get(i));
			}
		}
		for(int j = 0;j<r2.size();j++) {
			if(!r1.contains(r2.get(j))) {
				result.add(r2.get(j));
			}
		}
	}
	return result;
}

















public static void main(String[] args) throws Exception {
	  /* /the code below is testing snippets taken from the milestone description
	  String strTableName = "Student";
	  
	  Hashtable<String,String>  htblColNameType = new Hashtable<String,String> ( ); 
	  htblColNameType.put("id", "java.lang.Integer"); 
	  htblColNameType.put("name", "java.lang.String"); 
	  htblColNameType.put("gpa", "java.lang.Double");
	  htblColNameType.put("date", "java.lang.Date");
	  
	  Hashtable<String,String>  htblColNameMin = new Hashtable<String,String> ( ); 
	  htblColNameMin.put("id", "0"); 
	  htblColNameMin.put("name", "A"); 
	  htblColNameMin.put("gpa", "0.7"); 
	  htblColNameMin.put("date", "2000/01/01"); 
	  
	  Hashtable<String,String>  htblColNameMax = new Hashtable<String,String> ( ); 
	  htblColNameMax.put("id", "1000000000"); 
	  htblColNameMax.put("name", "zzzzzzzzzzzzzzzzzzzz"); 
	  htblColNameMax.put("gpa", "5");
	  htblColNameMax.put("date", "2025/12/31");


	  Table test = new Table(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax,2);
	  
	  System.out.println(test.name);
	  
	  
	  ArrayList<String> keys = new ArrayList<String>();
	  keys.add("date");
	  keys.add("id");
	  keys.add("gpa");
	  keys.add("name");
	  test.setKeys(keys);
	  
	  
	  Hashtable<String,Object> i1 = new Hashtable<String,Object>();
	  i1.put("id", new Integer( 111 )); 
	  i1.put("name", new String("John 1" ) ); 
	  i1.put("gpa", new Double( 1.5 ) );
	  
	  Hashtable<String,Object> i2 = new Hashtable<String,Object>();
	  i2.put("id", new Integer( 222 )); 
	  i2.put("name", new String("John 2" ) ); 
	  i2.put("gpa", new Double( 1.6 ) );
	  i2.put("date", new Date("2003/9/6"));
	  
	  
	  Hashtable<String,Object> i3 = new Hashtable<String,Object>();
	  i3.put("id", new Integer( 333 )); 
	  i3.put("name", new String("John 3" ) ); 
	  i3.put("gpa", new Double( 1.7 ) );
	  i3.put("date", new Date("2004/9/6"));
	  
	  Hashtable<String,Object> i4 = new Hashtable<String,Object>();
	  i4.put("id", new Integer( 444 )); 
	  i4.put("name", new String("John 3" ) ); 
	  i4.put("gpa", new Double( 1.8 ) );
	  i4.put("date", new Date("2005/9/6"));
	  
	  Hashtable<String,Object> i5 = new Hashtable<String,Object>();
	  i5.put("id", new Integer( 555 )); 
	  i5.put("name", new String("John 4" ) ); 
	  i5.put("gpa", new Double( 1.7 ) );
	  i5.put("date", new Date("2006/9/6"));
	  
	  Hashtable<String,Object> i6 = new Hashtable<String,Object>();
	  i6.put("id", new Integer( 666 )); 
	  i6.put("name", new String("John 6" ) ); 
	  i6.put("gpa", new Double( 1.9 ) );

	  
	  test.insert(i1);
	  test.insert(i2);
	  test.insert(i3);
	  test.insert(i4);
	  test.insert(i5);
	  test.insert(i6);
	  
	  
	  Hashtable<String,Object> t = new Hashtable<String,Object>();
	  //t.put("date", new Date("2006/11/11"));
	  t.put("name", "John 3");
	  //test.update("666", t);
	  test.delete(t);
	  
	  
	  System.out.println("test: " + test.current);*/
  }
  
}
