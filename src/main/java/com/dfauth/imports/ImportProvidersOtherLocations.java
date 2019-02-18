package com.dfauth.imports;


import com.dfauth.schema.Labels;
import com.dfauth.schema.RelationshipTypes;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;

public class ImportProvidersOtherLocations implements Runnable {

    private static final int TRANSACTION_LIMIT = 1000;
    private String file;
    private GraphDatabaseService db;
    private Log log;

    public ImportProvidersOtherLocations(String file, GraphDatabaseService db, Log log) {
        this.file = file;
        this.db = db;
        this.log = log;
    }

    @Override
    public void run() {
        Reader in;
        Iterable<CSVRecord> records = null;
        try {
            in = new FileReader("/" + file);
            records = CSVFormat.EXCEL.withHeader().parse(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            log.error("ImportProvidersRunnable Import - File not found: " + file);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("ImportProvidersRunnable Import - IO Exception: " + file);
        }

        HashMap<String, Node> continents = new HashMap<>();
        HashMap<String, Node> countries = new HashMap<>();
        HashMap<String, Node> metros = new HashMap<>();
        HashMap<String, Node> states = new HashMap<>();
        HashMap<String, Node> timezones = new HashMap<>();

        HashSet<String> relationships = new HashSet<>();

        Transaction tx = db.beginTx();
        try {
            int count = 0;

            assert records != null;
            for (CSVRecord record : records) {
                count++;
                if (count>1){
                    Node provider = null;
                    if(!record.get(0).isEmpty()){
                        provider = db.findNode(Labels.Provider, "NPI", record.get(0));
                        if (provider ==null){
                            provider = db.createNode(Labels.Provider);
                            provider.setProperty("NPI",record.get(0));
                        }

                    
                        Node practiceLocation = null;
                        if(!record.get(1).isEmpty()){
                            String locationKey = record.get(1) + "_" + record.get(2) + "_" + record.get(3) + "_" + record.get(4) + "_" + record.get(5);
                            practiceLocation = db.findNode(Labels.Location, "LocationKey", locationKey);
                            if (practiceLocation ==null){
                                practiceLocation = db.createNode(Labels.Location);
                                practiceLocation.setProperty("LocationKey",locationKey);
                                practiceLocation.setProperty("Address1",record.get(1));
                                practiceLocation.setProperty("Address2",record.get(2));
                                practiceLocation.setProperty("CityName",record.get(3));
                                practiceLocation.setProperty("StateName",record.get(4));
                                practiceLocation.setProperty("PostalCode",record.get(5));
                                try {
                                    practiceLocation.setProperty("addressZip5",record.get(5).substring(0,5));
                                }finally{
                                }
                            }
                        }

                        if (provider !=null && practiceLocation !=null){
    			            int found=0;
					        String locationKey = practiceLocation.getProperty("LocationKey").toString();
					        for (Relationship r : provider.getRelationships(RelationshipTypes.HAS_SECONDARY_PRACTICE_LOCATION, Direction.OUTGOING)) {
                                String relLocKey = r.getEndNode().getProperty("LocationKey").toString();
						        if (relLocKey.equalsIgnoreCase(locationKey)){
							        found=1;
						        }
					        }
					        if (found==0){
						        provider.createRelationshipTo(practiceLocation, RelationshipTypes.HAS_SECONDARY_PRACTICE_LOCATION);
					        }
                        }
                    }
                

                    if (count % TRANSACTION_LIMIT == 0) {
                        tx.success();
                        tx.close();
                        tx = db.beginTx();
                    }
                }
            }
            tx.success();
        } finally {
            tx.close();
        }

    }

    private Node getMetro(GraphDatabaseService db, HashMap<String, Node> metros, CSVRecord record) {
        Node metro = metros.get(record.get("metro_code"));
        if (metro == null) {
            metro = db.createNode(Labels.Metro);
            metro.setProperty("code", record.get("metro_code"));
        }
        return metro;
    }

    private Node getTimezone(GraphDatabaseService db, HashMap<String, Node> timezones, CSVRecord record) {
        Node timezone = timezones.get(record.get("time_zone"));
        if (timezone == null) {
            timezone = db.createNode(Labels.Timezone);
            timezone.setProperty("name", record.get("time_zone"));
            timezones.put(record.get("time_zone"), timezone);
        }
        return timezone;
    }

    private Node getState(GraphDatabaseService db, HashMap<String, Node> states, CSVRecord record) {
        Node state = states.get(record.get("country_iso_code") + "-" + record.get("subdivision_1_iso_code"));
        if (state == null) {
            state = db.createNode(Labels.State);
            state.setProperty("code", record.get("subdivision_1_iso_code"));
            state.setProperty("name", record.get("subdivision_1_name"));
            states.put(record.get("country_iso_code") + "-" + record.get("subdivision_1_iso_code"), state);
        }
        return state;
    }

    
}
