package com.dfauth.imports;

import com.dfauth.results.NodeResult;
import com.dfauth.results.NodeListResult;
import com.dfauth.results.StringResult;
import com.uber.h3core.*;
import com.uber.h3core.util.*;
import com.uber.h3core.util.GeoCoord;

import org.apache.commons.*;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;

import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.*;
import java.util.Map.Entry;

public class Imports {
    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(name = "com.dfauth.import.locations", mode = Mode.WRITE)
    @Description("CALL com.dfauth.import.locations(file)")
    public Stream<StringResult> importLocations(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportLocationsRunnable(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Locations imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.import.providers", mode = Mode.WRITE)
    @Description("CALL com.dfauth.import.providers(file)")
    public Stream<StringResult> importProviders(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportProvidersRunnable(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Providers imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.import.counties", mode = Mode.WRITE)
    @Description("CALL com.dfauth.import.counties(file)")
    public Stream<StringResult> importCounties(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportCountiesRunnable(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Counties imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.import.zipcounties", mode = Mode.WRITE)
    @Description("CALL com.dfauth.import.zipcounties(file)")
    public Stream<StringResult> importZipCounties(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportZipCountiesRunnable(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Zip Counties imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.import.statecounties", mode = Mode.WRITE)
    @Description("CALL com.dfauth.import.statecounties(file)")
    public Stream<StringResult> importStateCounties(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportStateCountiesRunnable(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("State Counties imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.import.zipdistances", mode = Mode.WRITE)
    @Description("CALL com.dfauth.import.zipdistances(file)")
    public Stream<StringResult> importZipDistances(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportZipDistancesRunnable(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Zip Distances imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.import.ziplatlong", mode = Mode.WRITE)
    @Description("CALL com.dfauth.import.ziplatlong(file)")
    public Stream<StringResult> importZipLatLong(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportZipLatLongRunnable(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Zip Lat Longs imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.import.pharmacies", mode = Mode.WRITE)
    @Description("CALL com.dfauth.import.pharmacies(file)")
    public Stream<StringResult> importPharmacies(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportPharmaciesRunnable(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Pharmacies imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.import.geocoded", mode = Mode.WRITE)
    @Description("CALL com.dfauth.import.geocoded(file)")
    public Stream<StringResult> importGeoCoded(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportGeoCodedRunnable(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Geocoded Values imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.import.geocodedPostalCodes", mode = Mode.WRITE)
    @Description("CALL com.dfauth.import.geocodedPostalCodes(file)")
    public Stream<StringResult> importGeoCodedPostalCodes(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportGeoCodedPostalCodesRunnable(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Geocoded Values imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.import.geocodedAddresses", mode = Mode.WRITE)
    @Description("CALL com.dfauth.import.geocodedAddresses(file, addressType)")
    public Stream<StringResult> importGeocodedAddresses(@Name("file") String file, @Name("addressType") String addressType) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportProviderAddressesGeoCodedRunnable(file, addressType, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Providers imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.import.docgraph", mode = Mode.WRITE)
    @Description("CALL com.dfauth.import.docgraph(file)")
    public Stream<StringResult> importDocGraph(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportDocGraphRunnable(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Docgraph Referrals imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.import.importProviderOtherLocations", mode = Mode.WRITE)
    @Description("CALL com.dfauth.import.importProviderOtherLocations(file)")
    public Stream<StringResult> importProviderOtherLocations(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportProvidersOtherLocations(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Provider other locations imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.import.importProviderOtherName", mode = Mode.WRITE)
    @Description("CALL com.dfauth.import.importProviderOtherName(file)")
    public Stream<StringResult> importProviderOtherName(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportProvidersOtherName(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Provider other name imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.import.ip4", mode = Mode.WRITE)
    @Description("CALL com.dfauth.import.ip4(file)")
    public Stream<StringResult> importIP4(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportIP4Runnable(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Locations imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.import.h3.locations", mode = Mode.WRITE)
    @Description("CALL com.dfauth.locations.h3")
    public Stream<StringResult> addH3Value(@Name("resolution") String resolution) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportH3ValueRunnable(resolution, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Locations imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.h3.ringSize", mode = Mode.WRITE)
    @Description("CALL com.dfauth.h3.ringSize(hexAddress, ringSize)")
    public Stream<StringResult> h3RingSize(@Name("hexAddress") String hexAddress, @Name("ringSize") String ringSize) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new H3DemoRunnable(hexAddress, ringSize, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("H3 Key Ring calculated in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.dfauth.h3.hexArea", mode = Mode.WRITE)
    @Description("CALL com.dfauth.h3.hexArea(hexSize)")
    public Stream<StringResult> h3RingSize(@Name("hexSize") String hexSize) throws InterruptedException {
        Double hexArea = 0.0;
        try {
            H3Core h3 = H3Core.newInstance();
            hexArea = h3.hexArea(Integer.parseInt(hexSize),AreaUnit.km2);


        }    catch (Exception e) {
                System.out.println(e);
            }  finally {
     //           tx1.close();
            }

        return Stream.of(new StringResult("Hex size is " + Double.toString(hexArea) + " square meters"));
    }

    @Procedure(name = "com.dfauth.h3.locationsByRingSize", mode = Mode.WRITE)
    @Description("CALL com.dfauth.h3.locationsByRingSizes(hexAddress, ringSize)")
        public Stream<NodeListResult> findNearbyHex(@Name("hexAddress") String hexAddress, @Name("ringSize") String ringSize) throws InterruptedException {
            Map<Node, String> map = new HashMap<Node,String>();
            Map<Node, List<Node>> results = new HashMap<>();
            
            Node node = null;
            Relationship rel = null;

            try {
                H3Core h3 = H3Core.newInstance();
                
                List<String> ringList = null;
            
                ringList = h3.kRing(hexAddress, Integer.parseInt(ringSize));
                Iterator<String> ringListIterator = ringList.iterator();


                Result result = db.execute( "UNWIND $hexs AS hexaddress MATCH (l:Location {hexAddr:hexaddress}) RETURN l;", Collections.singletonMap( "hexs", ringList ) );
                Iterator<Node> n_column = result.columnAs( "l" );
                
                List<Node> reasons;
                for ( Node nodeIter : Iterators.asIterable( n_column ) )
                {
                    reasons = new ArrayList<>();
                    reasons.add(nodeIter);
                    map.put(nodeIter,nodeIter.getProperty( "hexAddr" ).toString());
                   results.put(nodeIter,reasons);
                 }
                 
      //          tx1.close();
                
            }    catch (Exception e) {
                System.out.println(e);
            }  finally {
     //           tx1.close();
            }

            // return a node and its linked nodes
            return results.entrySet().stream().map(entry -> {
                entry.getValue().add(entry.getKey());
                return new NodeListResult(entry.getValue());
            });
        }


        @Procedure(name = "com.dfauth.h3.locationsLatLongByRingSize", mode = Mode.WRITE)
        @Description("CALL com.dfauth.h3.locationsLatLongByRingSize(latitude, longitude, ringSize)")
            public Stream<NodeListResult> findNearbyLatLong(@Name("startLat") Double startLat,@Name("startLong") Double startLong, @Name("ringSize") String ringSize) throws InterruptedException {
                Map<Node, String> map = new HashMap<Node,String>();
                Map<Node, List<Node>> results = new HashMap<>();
                
                Node node = null;
                Relationship rel = null;
                String hexAddr = null;
    
                try {
                    H3Core h3 = H3Core.newInstance();
                    
                    List<String> ringList = null;
                    hexAddr = h3.geoToH3Address(startLat, startLong, 9);
                    System.out.println("Hex Address is: " + hexAddr);
                    ringList = h3.kRing(hexAddr, Integer.parseInt(ringSize));
                    Iterator<String> ringListIterator = ringList.iterator();

    
                    Result result = db.execute( "UNWIND $hexs AS hexaddress MATCH (l:Location {hexAddr:hexaddress}) RETURN l;", Collections.singletonMap( "hexs", ringList ) );
                    Iterator<Node> n_column = result.columnAs( "l" );
                    
                    List<Node> reasons;
                    for ( Node nodeIter : Iterators.asIterable( n_column ) )
                    {
                        reasons = new ArrayList<>();
                        reasons.add(nodeIter);
                        map.put(nodeIter,nodeIter.getProperty( "hexAddr" ).toString());
                       results.put(nodeIter,reasons);
                     }
                     
          //          tx1.close();
                    
                }    catch (Exception e) {
                    System.out.println(e);
                }  finally {
         //           tx1.close();
                }
    
                // return a node and its linked nodes
                return results.entrySet().stream().map(entry -> {
                    entry.getValue().add(entry.getKey());
                    return new NodeListResult(entry.getValue());
                });
            } 
            
            
            @Procedure(name = "com.dfauth.h3.polygonSearch", mode = Mode.WRITE)
            @Description("CALL com.dfauth.h3.polygonSearch(polyEdges)")
                public Stream<NodeListResult> findNodesPolygon(@Name("polyEdges") List<Map<String, Object>> polyEdges,@Name("polyEdgeHoles") List<Map<String, Object>> polyEdgeHoles) throws InterruptedException {
                    Map<Node, String> map = new HashMap<Node,String>();
                    Map<Node, List<Node>> results = new HashMap<>();
                    
                    List<GeoCoord> hexPoints = new ArrayList();
                    List<GeoCoord> hexHoles = new ArrayList();
                    List<List<GeoCoord>> holesList = new ArrayList<List<GeoCoord>>();

                    Node node = null;
                    Relationship rel = null;
                    String hexAddr = null;
                    Double thisLat = 0.0;
                    Double thisLong = 0.0;
                    
                    try {
                        
                        H3Core h3 = H3Core.newInstance();
                        for (Map<String, Object> mapEdges : polyEdges) {
                            for (Map.Entry<String, Object> entry : mapEdges.entrySet()) {
                                String key = entry.getKey();
                                Object value = entry.getValue();
                                Double myData = Double.parseDouble(value.toString());

                                if (key.equalsIgnoreCase("lat")){
                                    thisLat = myData;
                                }
                                if (key.equalsIgnoreCase("lon")){
                                    thisLong = myData;
                                }
                            }
                            GeoCoord tmpGeoCoord = new GeoCoord(thisLat, thisLong);
                            hexPoints.add(tmpGeoCoord);                           
                        }

                        for (Map<String, Object> hexHolesMap : polyEdgeHoles) {
                            for (Map.Entry<String, Object> entry : hexHolesMap.entrySet()) {
                                String key = entry.getKey();
                                Object value = entry.getValue();
                                Double myData = Double.parseDouble(value.toString());

                                if (key.equalsIgnoreCase("lat")){
                                    thisLat = myData;
                                }
                                if (key.equalsIgnoreCase("lon")){
                                    thisLong = myData;
                                }
                            }
                            GeoCoord tmpGeoCoord = new GeoCoord(thisLat, thisLong);
                            hexHoles.add(tmpGeoCoord);                           
                        }



                        List<String> hexList = null;
                        if (!hexHoles.isEmpty()){
                            holesList.add(hexHoles);
                            hexList = h3.polyfillAddress(hexPoints,holesList,9);
                        }else {
                            hexList = h3.polyfillAddress(hexPoints,null,9);
                        }

                        Result result = db.execute( "UNWIND $hexs AS hexaddress MATCH (l:Location {hexAddr:hexaddress}) RETURN l;", Collections.singletonMap( "hexs", hexList ) );
                        Iterator<Node> n_column = result.columnAs( "l" );
                        
                        List<Node> reasons;
                        for ( Node nodeIter : Iterators.asIterable( n_column ) )
                        {
                            reasons = new ArrayList<>();
                            reasons.add(nodeIter);
                            map.put(nodeIter,nodeIter.getProperty( "hexAddr" ).toString());
                           results.put(nodeIter,reasons);
                         }
                         
              //          tx1.close();
                        
                    }    catch (Exception e) {
                        System.out.println(e);
                    }  finally {
             //           tx1.close();
                    }
        
                    // return a node and its linked nodes
                    return results.entrySet().stream().map(entry -> {
                        entry.getValue().add(entry.getKey());
                        return new NodeListResult(entry.getValue());
                    });
                }  
            
}
