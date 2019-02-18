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

public class ImportProvidersRunnable implements Runnable {

    private static final int TRANSACTION_LIMIT = 1000;
    private String file;
    private GraphDatabaseService db;
    private Log log;

    public ImportProvidersRunnable(String file, GraphDatabaseService db, Log log) {
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
                            provider.setProperty("EIN",record.get(3));
                            if (record.get(4).length() > 0){
                                provider.setProperty("BusinessName",record.get(4)); 
                            }
                            if (record.get(4).length() < 2){
                                provider.setProperty("LastName",record.get(5)); 
                                provider.setProperty("FirstName",record.get(6));
                                provider.setProperty("MiddleName",record.get(7));
                                provider.setProperty("NameSuffix",record.get(8));
                            }
                        }
                    }

                    Node taxonomyCode = null;
                    if(!record.get(47).isEmpty()){
                        taxonomyCode = db.findNode(Labels.TaxonomyCode, "code", record.get(47));
                        if (taxonomyCode ==null){
                            taxonomyCode = db.createNode(Labels.TaxonomyCode);
		    				taxonomyCode.setProperty("code",record.get(47));
                        }
                    }

                    Node practiceLocation = null;
                    if(!record.get(28).isEmpty()){
                        String locationKey = record.get(28) + "_" + record.get(29) + "_" + record.get(30) + "_" + record.get(31) + "_" + record.get(32);
                        practiceLocation = db.findNode(Labels.Location, "LocationKey", locationKey);
                        if (practiceLocation ==null){
                           practiceLocation = db.createNode(Labels.Location);
                            practiceLocation.setProperty("LocationKey",locationKey);
                            practiceLocation.setProperty("Address1",record.get(28));
                            practiceLocation.setProperty("Address2",record.get(29));
                            practiceLocation.setProperty("CityName",record.get(30));
                            practiceLocation.setProperty("StateName",record.get(31));
                            practiceLocation.setProperty("PostalCode",record.get(32));
                       }
                    }

                    Node billingLocation = null;
                    if(!record.get(20).isEmpty()){
                        String locationKey = record.get(20) + "_" + record.get(21) + "_" + record.get(22) + "_" + record.get(23) + "_" + record.get(24);
                        billingLocation = db.findNode(Labels.Location, "LocationKey", locationKey);
                        if (billingLocation ==null){
                            billingLocation = db.createNode(Labels.Location);
                            billingLocation.setProperty("LocationKey",locationKey);
                           billingLocation.setProperty("Address1",record.get(20));
                            billingLocation.setProperty("Address2",record.get(21));
                            billingLocation.setProperty("CityName",record.get(22));
                            billingLocation.setProperty("StateName",record.get(23));
                            billingLocation.setProperty("PostalCode",record.get(24));
                        }
                    }

                    if (provider !=null && taxonomyCode !=null){
    					int found=0;
                        String taxCode = taxonomyCode.getProperty("code").toString();
    					for (Relationship r : provider.getRelationships(RelationshipTypes.HAS_SPECIALTY, Direction.OUTGOING)) {
                           String taxEndCode = r.getEndNode().getProperty("code").toString();
    						 if (taxEndCode.equalsIgnoreCase(taxCode)){
    							found=1;
    						}
    					}
    					if (found==0){
    						provider.createRelationshipTo(taxonomyCode, RelationshipTypes.HAS_SPECIALTY);
    					}
                   }
                
                   if (provider !=null && practiceLocation !=null){
    			        int found=0;
					    String locationKey = practiceLocation.getProperty("LocationKey").toString();
					    for (Relationship r : provider.getRelationships(RelationshipTypes.HAS_PRACTICE_AT, Direction.OUTGOING)) {
                            String relLocKey = r.getEndNode().getProperty("LocationKey").toString();
						    if (relLocKey.equalsIgnoreCase(locationKey)){
							    found=1;
						    }
					    }
					    if (found==0){
						    provider.createRelationshipTo(practiceLocation, RelationshipTypes.HAS_PRACTICE_AT);
					    }
                    }

                    if (provider !=null && billingLocation !=null){
					    int found=0;
					    String locationKey = billingLocation.getProperty("LocationKey").toString();
					    for (Relationship r : provider.getRelationships(RelationshipTypes.HAS_BILLING_ADDRESS_AT, Direction.OUTGOING)) {
                            String relLocKey = r.getEndNode().getProperty("LocationKey").toString();
						    if (relLocKey.equalsIgnoreCase(locationKey)){
							    found=1;
						    }
					    }
					    if (found==0){
						    provider.createRelationshipTo(billingLocation, RelationshipTypes.HAS_BILLING_ADDRESS_AT);
					    }
                    }
                }
                

                if (count % TRANSACTION_LIMIT == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
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
