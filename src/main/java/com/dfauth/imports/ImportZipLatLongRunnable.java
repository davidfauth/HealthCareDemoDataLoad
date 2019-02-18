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
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84_3D;
import static org.neo4j.values.storable.Values.pointValue;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;

public class ImportZipLatLongRunnable implements Runnable {

    private static final int TRANSACTION_LIMIT = 1000;
    private String file;
    private GraphDatabaseService db;
    private Log log;

    public ImportZipLatLongRunnable(String file, GraphDatabaseService db, Log log) {
        this.file = file;
        this.db = db;
        this.log = log;
    }

    @Override
    public void run() {
        Reader in;
        Iterable<CSVRecord> records = null;
        int h3Resolution = 9;
        String hexAddr = null;

        try {
            in = new FileReader("/" + file);
            records = CSVFormat.EXCEL.withHeader().parse(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            log.error("ImportZipLatLongRunnable Import - File not found: " + file);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("ImportZipLatLongRunnable Import - IO Exception: " + file);
        }

        

        Transaction tx = db.beginTx();
        try {
            int count = 0;
            H3Core h3 = H3Core.newInstance();
            
            assert records != null;
            for (CSVRecord record : records) {
                count++;
                hexAddr = null;
                    Node practiceLocation = null;
                    if(!record.get(0).isEmpty()){
                        double latValue = Double.parseDouble(record.get(5));
                        double longValue = Double.parseDouble(record.get(6));
                        practiceLocation = db.findNode(Labels.PostalCode, "PostalCode", record.get(0));
                        if (practiceLocation !=null){
                            practiceLocation.setProperty("location",pointValue( WGS84, latValue, longValue ));
                            practiceLocation.setProperty("latitude",latValue);
                            practiceLocation.setProperty("longitude",longValue);
                            hexAddr = h3.geoToH3Address(latValue, longValue, h3Resolution);
                            practiceLocation.setProperty("hexAddr",hexAddr);
                        }
                    }
                
                

                if (count % TRANSACTION_LIMIT == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }

            tx.success();
        }   catch (Exception e) {
            System.out.println(e);
        }  finally {
            tx.close();
        }

    }
    
}
