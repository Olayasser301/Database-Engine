package main;

import java.io.IOException;

public class DBAppException extends IOException{
public DBAppException() {
	
}
public DBAppException(String s) {
	super(s);
}
}
