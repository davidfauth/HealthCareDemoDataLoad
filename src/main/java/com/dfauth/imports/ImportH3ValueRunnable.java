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
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;

public class ImportH3ValueRunnable implements Runnable {

    private static final int TRANSACTION_LIMIT = 10_000;
    private String resolution;
    private GraphDatabaseService db;
    private Log log;

    public ImportH3ValueRunnable(String resolution, GraphDatabaseService db, Log log) {
        this.resolution = resolution;
        this.db = db;
        this.log = log;
    }

    @Override
    public void run() {
        int counter=1;
        int h3Resolution = 9;
        Transaction tx1 = db.beginTx();
        try {
            H3Core h3 = H3Core.newInstance();
            double lat = 37.775938728915946;
            double lng = -122.41795063018799;
            if (Integer.parseInt(resolution) < 0 || Integer.parseInt(resolution) > 16){
                h3Resolution = 9;
            } else {
                h3Resolution = Integer.parseInt(resolution);
            }
            String hexAddr = null;
        
            ResourceIterator<Node> iter = db.findNodes(Labels.Location);
            while(iter.hasNext()) {
                Node locationNode = iter.next();
                
                
                if (locationNode.hasProperty("latitude"))
                {
                    if (!locationNode.hasProperty("hexAddr"))
                    {
                        counter++;
                        Double latVal = Double.parseDouble(locationNode.getProperty("latitude").toString());
                        Double longVal = Double.parseDouble(locationNode.getProperty("longitude").toString());
                        hexAddr = h3.geoToH3Address(latVal, longVal, h3Resolution);
                        locationNode.setProperty("hexAddr",hexAddr);
                    }
                }
                if (counter % TRANSACTION_LIMIT == 0) {
                    tx1.success();
                    tx1.close();
                    tx1 = db.beginTx();
                }
                
            }
            System.out.println(counter);
            tx1.success();
        }    catch (Exception e) {
            System.out.println(e);
        }  finally {
            tx1.close();
        }
    }
    
}
