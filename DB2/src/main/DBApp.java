package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

public class DBApp {
	
	int MaxRows;
	Hashtable<String,Table> allTables = new Hashtable<String,Table>();
	Vector<String> filesPages;
	Vector<String> metaPages;
	Hashtable<String,Boolean> Indexes = new Hashtable<String,Boolean>();
	
	public DBApp() {
		filesPages = new Vector<String>();
		metaPages = new Vector<String>();
		File workingFolder = new File("data");
        if (!workingFolder.exists()) {
            workingFolder.mkdir(); 
        }
        //File pfile = new File(workingFolder.getAbsolutePath() + "/files.csv");
		File file = new File(workingFolder.getAbsolutePath() + "/metadata.csv"); // if metadata = 0 so files is 0 as well
		if(file.length() == 0) {
			allTables = new Hashtable<String,Table>();
		}
		else {
			Loading();
		}
		init();
	}
	
	// loads previous tables by reading both metadata and files csv files
	// so the hashtable within hashtable is done so as i save the values from reading in it.
	// string is table name, hashtable next to it will be the hashtables used to create the table
	// tablesColoumns is tables types
	private void Loading() {
		try {
			Hashtable<String,Hashtable<String, String>> tablesColoumns = new Hashtable<String,Hashtable<String, String>>();
			Hashtable<String,Hashtable<String, String>> tablesMaxs = new Hashtable<String,Hashtable<String, String>>();
			Hashtable<String,Hashtable<String, String>> tablesMins = new Hashtable<String,Hashtable<String, String>>();
			Hashtable<String,String> clusteringKeysOfTables = new Hashtable<String, String>();
			ArrayList<String> tableNames = new ArrayList<String>();
			Hashtable<String,ArrayList<String>> IndexName = new Hashtable<String,ArrayList<String>>();
			String Metarow = "";
			File Folder = new File("data");
		       if (!Folder.exists()) {
		           Folder.mkdir();
		       }
		    FileReader fileReader = new 
		    FileReader( Folder.getAbsolutePath() + "/metadata.csv" ); 
		    BufferedReader MetaReader = new BufferedReader( fileReader ); 
			ArrayList<String[]> metaFile = new ArrayList<String[]>(); // has all of metadata file, each String[] is a line
			while ((Metarow = MetaReader.readLine()) != null) {
				String[] data = Metarow.split(",");
				metaFile.add(data);
			}
			MetaReader.close();  // so in each arrayList cell there is a String[] and each one represent a row in the metadata file
			for(int i=0;i<metaFile.size();i++) {
				// the above code is repeated for each row and for each table
				String[] t = metaFile.get(i);
				if(tableNames.isEmpty()) {
					tableNames.add(t[0]);
					Hashtable<String,String> type =  new Hashtable<String,String>();
					Hashtable<String,String> MinV =  new Hashtable<String,String>();
					Hashtable<String,String> MaxV =  new Hashtable<String,String>();
					ArrayList<String> index =  new ArrayList<String>();
					tablesColoumns.put(t[0],type);
					tablesMaxs.put(t[0], MaxV);
					tablesMins.put(t[0], MinV);
					IndexName.put(t[0], index);
				}
				else if(t[0] != tableNames.get(tableNames.size()-1)) {
					tableNames.add(t[0]);
					Hashtable<String,String> type =  new Hashtable<String,String>();
					Hashtable<String,String> MinV =  new Hashtable<String,String>();
					Hashtable<String,String> MaxV =  new Hashtable<String,String>();
					ArrayList<String> index =  new ArrayList<String>();
					tablesColoumns.put(t[0],type);
					tablesMaxs.put(t[0], MaxV);
					tablesMins.put(t[0], MinV);
					IndexName.put(t[0], index);
				}
				// above code adds a hash table for each table in tablesCol,tablesMaxs,tablesMins
				tablesColoumns.get(t[0]).put(t[1], t[2]);
				tablesMaxs.get(t[0]).put(t[1], t[6]);
				tablesMins.get(t[0]).put(t[1], t[7]);
				if(t[3].equals("true")) {
					clusteringKeysOfTables.put(t[0], t[1]);
				}
				if(t[5].equals("SparseIndex") || t[5].equals("DenseIndex")) {
					Indexes.put(t[0], true);
					IndexName.get(t[0]).add(t[1]);
				}
			}
			for(int i = 0;i < tableNames.size();i++) {
				allTables.put(tableNames.get(i).toLowerCase(), new Table(tableNames.get(i),clusteringKeysOfTables.get(tableNames.get(i)),
										tablesColoumns.get(tableNames.get(i)),tablesMaxs.get(tableNames.get(i)),tablesMins.get(tableNames.get(i)),this.MaxRows));
			}
			for(int i = 0;i < tableNames.size();i++) {
				allTables.get(tableNames.get(i).toLowerCase()).setIndex(IndexName.get(tableNames.get(i)));
			}
			// to lower case as table names are case insensitive, it is saved with its case in metadata file
			// search by table name, get table object, add file names to its array
			Hashtable<String,ArrayList<String>> FileNames = new Hashtable<String,ArrayList<String>>();
			String FilesRow = "";
			File FilesFolder = new File("data");
		       if (!FilesFolder.exists()) {
		           FilesFolder.mkdir();
		       }
		    FileReader fReader = new 
		    FileReader( FilesFolder.getAbsolutePath() + "/files.csv" ); 
		    BufferedReader FileReader = new BufferedReader( fReader ); 
			ArrayList<String[]> PagesFiles = new ArrayList<String[]>(); // has all of files file, each String[] is a line
			while ((FilesRow = FileReader.readLine()) != null) {
				String[] data = FilesRow.split(",");
				PagesFiles.add(data);
			}
			FileReader.close();
			for(int z = 0;z<tableNames.size();z++) {
				FileNames.put(tableNames.get(z).toLowerCase(), new ArrayList<String>());
			}
			for(int i=0;i<PagesFiles.size();i++) {
				String[] f = PagesFiles.get(i);
				FileNames.get(f[0]).add(f[2]);
			}
			for(int i = 0;i < tableNames.size();i++) {
				allTables.get(tableNames.get(i).toLowerCase()).setFiles(FileNames.get(tableNames.get(i).toLowerCase()));
			}
		}
		catch(IOException e) {
			
		}
	}

	public void init() {
		try
		{
		 File workingFolder = new File("config");
	       if (!workingFolder.exists()) {
	           workingFolder.mkdir();
	       }
		FileReader reader = new FileReader(workingFolder.getAbsolutePath() + "/DBApp");
		Properties properties = new Properties();
		properties.load(reader);
		MaxRows = Integer.parseInt(properties.getProperty("MaximumRowsCountinPage"));
	}
		catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public void createTable(String strTableName, String strClusteringKeyColumn,Hashtable<String,String> htblColNameType,Hashtable<String,String> htblColNameMin, 
			Hashtable<String,String> htblColNameMax ) throws DBAppException {
		if(allTables.get(strTableName.toLowerCase()) != null) {
			throw new DBAppException("There is already a table with that name");
		}
		else if(htblColNameType == null || htblColNameMin == null || htblColNameMax == null){
			throw new DBAppException("Not all the types, minimum values and maximum values were provided");
		}
		else if(!htblColNameType.keySet().equals(htblColNameMax.keySet()) || !htblColNameType.keySet().equals(htblColNameMin.keySet()) || !htblColNameMax.keySet().equals(htblColNameMin.keySet())){
			throw new DBAppException("The columns in the types,minimum and maximum are not the same");
		}
		else {
			allTables.put(strTableName.toLowerCase(), new Table(strTableName,strClusteringKeyColumn,htblColNameType,htblColNameMin,htblColNameMax,MaxRows));
			metadataCSV(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax, "null", "null");
		}
	}
	
	public void updateTable(String strTableName,String strClusteringKeyValue,Hashtable<String,Object> htblColNameValue )throws DBAppException {
		if(allTables.get(strTableName.toLowerCase()) != null)
			allTables.get(strTableName.toLowerCase()).update(strClusteringKeyValue, htblColNameValue);
		else
			throw new DBAppException("Table not found");
	}
	
	public void insertIntoTable(String strTableName,Hashtable<String,Object> htblColNameValue)throws DBAppException {
		boolean f = false;
		if(allTables.get(strTableName.toLowerCase()) != null)
			f=allTables.get(strTableName.toLowerCase()).insert(htblColNameValue);
		else
			throw new DBAppException("Table not found");
		if(f)
			this.WriteFilesCSV();
	}
	
	public void deleteFromTable(String strTableName,Hashtable<String,Object> htblColNameValue)throws DBAppException {
		boolean f = false;
		if(allTables.get(strTableName.toLowerCase()) != null)
			f=allTables.get(strTableName.toLowerCase()).delete(htblColNameValue);
		else
			throw new DBAppException("Table not found");
		if(f)
			this.WriteFilesCSV();
	}
	
	// metadata updating method
	public void metadataCSV(String strTableName, String strClusteringKeyColumn,Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax, String indexName, String indexType) {
	  try {
		  File Folder = new File("data");
		     if (!Folder.exists()) {
		         Folder.mkdir();
		     }
		FileWriter fileWriter = new FileWriter(Folder.getAbsolutePath() + "/metadata.csv",true);
		ArrayList<String> keys = new ArrayList<String>();
		Iterator<String> it = htblColNameType.keySet().iterator();
		while(it.hasNext()) {
			String row = "";
			String col = it.next();
			row = row + strTableName + "," ;
			row = row + col + "," ;
			row = row + htblColNameType.get(col) + "," ;
			if(col == strClusteringKeyColumn) {
				row = row + "true" + "," ;
			}
			else {
				row = row + "false" + "," ;
			}
			row = row + indexName + "," ;
			row = row + indexType + "," ;
			row = row + htblColNameMin.get(col) + "," ;
			row = row + htblColNameMax.get(col) + "," ;
			keys.add(col);
			fileWriter.write(row + System.lineSeparator()); 
		}
		fileWriter.close();
		allTables.get(strTableName.toLowerCase()).setKeys(keys);
	   } 
	  catch (IOException e) {
		e.printStackTrace();
	   }
	}
	
	
	// files.csv remove page method
	public void WriteFilesCSV() {
		try {
			  File Folder = new File("data");
			     if (!Folder.exists()) {
			         Folder.mkdir();
			     }
			  FileWriter fileWriter = new FileWriter(Folder.getAbsolutePath() + "/files.csv",false); 
			  Iterator<String> it = allTables.keySet().iterator();
			  while(it.hasNext()) { 
				  String table = it.next();
				  ArrayList<String> FileN = allTables.get(table).fileNames ;
				  ArrayList<String> IndexN = allTables.get(table).indexCol ;
				  String clusterK = allTables.get(table).clustering ;
				  for(int i = 0;i<FileN.size();i++) { 
					  String str = "";
					  // File file = new File(Folder.getAbsolutePath() + "/" + FileN.get(i) + ".csv");
					  str = str + table + "," + "Table" + "," + FileN.get(i) + ".csv" + "," + Folder.getAbsolutePath();
					  fileWriter.write(str + System.lineSeparator());
				  } 
				  for(int i = 0;i<IndexN.size();i++) { 
					  String str = "";
					  if(IndexN.get(i).equals(clusterK)) {
						  str = str + table + "_" + IndexN.get(i) + "_sparse" + "," + "SparseIndex" + "," + table + "_" + IndexN.get(i) + "_sparse.csv" + "," + Folder.getAbsolutePath();
					  }
					  else { // writing two level
						  str = str + table + "_" + IndexN.get(i) + "_dense" + "," + "DenseIndex" + "," + table + "_" + IndexN.get(i) + "_dense.csv" + "," + Folder.getAbsolutePath();
						  fileWriter.write(str + System.lineSeparator());
						  str = "";
						  str = str + table + "_" + IndexN.get(i) + "_sparse" + "," + "SparseIndex" + "," + table + "_" + IndexN.get(i) + "_sparse.csv" + "," + Folder.getAbsolutePath();
					  }
					  fileWriter.write(str + System.lineSeparator());
				  } 
			  } 
			  fileWriter.close();
			  }
			  catch(IOException e) {
				  e.printStackTrace();
			  }
	}
	
	
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,String[] strarrOperators) throws DBAppException{ 
		if(allTables.get(arrSQLTerms[0]._strTableName.toLowerCase()) != null)
		   return allTables.get(arrSQLTerms[0]._strTableName.toLowerCase()).selectFromTable(arrSQLTerms, strarrOperators);
		else
			throw new DBAppException("Table not found");
	}
	
	
	public void createIndex(String strTableName, String strColName ) throws DBAppException{
		if(allTables.get(strTableName.toLowerCase()) != null) {
			if(allTables.get(strTableName.toLowerCase()).clustering.equals(strColName)) {
				allTables.get(strTableName.toLowerCase()).createIndexClustering(strColName);
			}
			else if(allTables.get(strTableName.toLowerCase()).keySets.contains(strColName)) {
				allTables.get(strTableName.toLowerCase()).createIndexNon(strColName);
			}
			else {
				throw new DBAppException("column does not exist");
			}
		}
		else
			throw new DBAppException("Table not found");
		this.WriteFilesCSV();
		this.updateMetaIndex(strTableName, strColName);
	}
	
	
	public void updateMetaIndex(String table,String col) {
		try { // read the meta file
		String Metarow = "";
		File Folder = new File("data");
	       if (!Folder.exists()) {
	           Folder.mkdir();
	       }
	    FileReader fileReader = new 
	    FileReader( Folder.getAbsolutePath() + "/metadata.csv" ); 
	    BufferedReader MetaReader = new BufferedReader( fileReader ); 
		ArrayList<String[]> metaFile = new ArrayList<String[]>(); // has all of metadata file, each String[] is a line
		while ((Metarow = MetaReader.readLine()) != null) {
			String[] data = Metarow.split(",");
			metaFile.add(data);
		}
		MetaReader.close();
		
		// write the meta file with the updates (when comparing table and column names do lower string)
		FileWriter fileWriter = new FileWriter(Folder.getAbsolutePath() + "/metadata.csv",false);
		for(int i = 0;i<metaFile.size();i++) {
			String row = "";
			row = row + metaFile.get(i)[0] + "," + metaFile.get(i)[1] + "," + metaFile.get(i)[2] + "," + metaFile.get(i)[3] + ",";
			if(metaFile.get(i)[0].toLowerCase().equals(table.toLowerCase()) && metaFile.get(i)[1].toLowerCase().equals(col.toLowerCase())) {
				row = row + table + "_" + col + "_sparse" + "," + "SparseIndex" + ",";
			}
			else {
				row = row + metaFile.get(i)[4] + "," + metaFile.get(i)[5] + ",";
			}
			row = row + metaFile.get(i)[6] + "," + metaFile.get(i)[7] ;
			fileWriter.write(row + System.lineSeparator()); 
		}
		fileWriter.close();
		}
		catch(IOException e) {
			
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static void main(String[] args) throws DBAppException {
		/*System.out.println(System.nanoTime());
		 DBApp test = new DBApp();
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

		test.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
		//test.createIndex("student", "id");
		//test.createIndex("student", "date");
		
		Hashtable<String,Object> i1 = new Hashtable<String,Object>();
		  i1.put("id", new Integer( 111 )); 
		  i1.put("name", new String("John 1" ) ); 
		  i1.put("gpa", new Double( 1.5 ) );
		  i1.put("date", new Date("2003/9/6"));
		  
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
		
		test.insertIntoTable("student", i1);
		test.insertIntoTable("student", i2);
		test.insertIntoTable("student", i3);
		test.insertIntoTable("student", i4);
		test.insertIntoTable("student", i5);
		test.insertIntoTable("student", i6);
		/*Hashtable<String,Object> i7 = new Hashtable<String,Object>();
		  i7.put("name", new String( "John 3" )); 
		test.deleteFromTable("student", i7);*/
		
		//test.createIndex("student", "id");
		//test.createIndex("student", "date");
		
		/*Hashtable<String,Object> i7 = new Hashtable<String,Object>();
		i7.put("name", new String( "John 3" )); 
		test.deleteFromTable("student", i7);
		//test.updateTable("student", "444", i7);
		
		Hashtable<String, String> club_type = new Hashtable<String, String>();
		  club_type.put("id", "java.lang.Integer");
		  club_type.put("name", "java.lang.String");
		  club_type.put("year_of_est", "java.lang.Integer");
		  Hashtable<String, String> club_min = new Hashtable<String, String>();
		  club_min.put("id", "0");
		  club_min.put("name", "A");
		  club_min.put("year_of_est", "1800");
		  Hashtable<String, String> club_max = new Hashtable<String, String>();
		  club_max.put("id", "1000000000");
		  club_max.put("name", "zzzzzzzzzzzzzz");
		  club_max.put("year_of_est", "2050");
		  test.createTable("club", "id", club_type, club_min, club_max);
		  Hashtable<String, Object> club_insert1 = new Hashtable<String, Object>();
		  club_insert1.put("id", 1);
		  club_insert1.put("name", "ahly");
		  club_insert1.put("year_of_est", 1907);;
		  test.insertIntoTable("club", club_insert1);
		  
		  SQLTerm t1 = new SQLTerm();
		  t1._objValue = new String("John 3");
		  t1._strColumnName = "name";
		  t1._strOperator = "!=";
		  t1._strTableName = "student";
		  
		 SQLTerm t2 = new SQLTerm();
		  t2._objValue = new Double( 1.7 ); 
		  t2._strColumnName = "gpa";
		  t2._strOperator = "!=";
		  t2._strTableName = "student";
		  
		  /*SQLTerm t2 = new SQLTerm();
		  t2._objValue = new Date( "2003/9/6" ); 
		  t2._strColumnName = "date";
		  t2._strOperator = "!=";
		  t2._strTableName = "student";*/
		  
		  /*SQLTerm[] terms = new SQLTerm[2];
		  terms[0] = t1;
		  terms[1] = t2;
		  String[] op = new String[1];
		  op[0] = "AND";
		  Iterator v = test.selectFromTable(terms, op);
		  System.out.println(System.nanoTime());
		  System.out.println(v.toString() + "___________" );
		  while(v.hasNext()) {
			  System.out.println(v.next());
		  } */
		  
	}
}
