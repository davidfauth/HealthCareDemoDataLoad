package com.dfauth.imports;


import com.dfauth.schema.Labels;
import com.dfauth.schema.RelationshipTypes;
import com.uber.h3core.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class H3DemoRunnable implements Runnable {

    private static final int TRANSACTION_LIMIT = 10_000;
    private GraphDatabaseService db;
    private Log log;
    private String ringSize;
    private String h3Address;

    public H3DemoRunnable(String h3Address, String ringSize, GraphDatabaseService db, Log log ) {
        this.db = db;
        this.log = log;
        this.ringSize = ringSize;
        this.h3Address = h3Address;
    }

    @Override
    public void run() {
        int counter=0;
 //       Transaction tx1 = db.beginTx();
        try {
            H3Core h3 = H3Core.newInstance();
            
            List<String> ringList = null;
        
            ringList = h3.kRing(h3Address, Integer.parseInt(ringSize));
            Iterator<String> ringListIterator = ringList.iterator();
            Result result = db.execute( "UNWIND $hexs AS hexaddress MATCH (l:Location {hexAddr:hexaddress}) RETURN l;", Collections.singletonMap( "hexs", ringList ) );
    	
		    
  //          tx1.close();
            
        }    catch (Exception e) {
            System.out.println(e);
        }  finally {
 //           tx1.close();
        }
    }
    
}
