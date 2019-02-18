package com.healthcaredemo.locator;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.logging.Log;
import org.neo4j.string.UTF8;

import static java.lang.String.format;

@Path("locator")
public class LocatorService {
	
	
  private static int LIMIT = 100000;


  
  
  
  private static final ObjectMapper objectMapper = new ObjectMapper();




  @GET
  @Path("/helloworld")
  public String helloWorld() {
      return "Hello World!";
  }
  
  // should be able to uncomment out the cache stuff for 3.0
  @GET
  @Path("/warmup")
  public String warmUp(@Context GraphDatabaseService db) {
	  int counter=1;
	  Transaction tx = db.beginTx();
      try 
      {


//for Neo4j 3.0.0
    	  for (Node node : db.getAllNodes()) {
              node.getPropertyKeys();
              counter ++;
              if (counter % 10_000 == 0) {
                  tx.success();
                  tx.close();
                  tx = db.beginTx();
              }
          }
    	  
    	  for ( Relationship relationship : db.getAllRelationships()){
              relationship.getPropertyKeys();
              relationship.getNodes();
              counter ++;
              if (counter % 10_000 == 0) {
                  tx.success();
                  tx.close();
                  tx = db.beginTx();
              }
          } 
  
      }catch (Exception e){
      	System.out.println(e);
      }
      return "Warmed up and ready to go!";
  }
  
  

  @GET
  @Path("/stats/State/{stateCode}/{taxonomyCode}")
  public Response statsState( final @PathParam("stateCode") String stateCode, @PathParam("taxonomyCode") String taxonomyCode,  @Context GraphDatabaseService db)
  {
      final Map<String, Object> params = MapUtil.map( "stateCode", stateCode );
      params.put("taxonomyCode", taxonomyCode);

      StreamingOutput stream = new StreamingOutput()
      {
          @Override
          public void write( OutputStream os ) throws IOException, WebApplicationException
          {
              JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator( os, JsonEncoding.UTF8 );
              jg.writeStartObject();
              jg.writeArrayFieldStart("Providers");

              try ( Transaction tx = db.beginTx();
                    Result result = db.execute( statsState(), params ) )
             {
                  while ( result.hasNext() )
                  {
                      Map<String,Object> row = result.next();
                      jg.writeStartObject();
                  			jg.writeObjectField("TaxonomyCode",row.get( "taxonomyCode" ).toString());
                  			jg.writeObjectField("maxDistance",row.get( "maxCrow" ).toString());
                  			jg.writeObjectField("minDistance",row.get( "minCrow" ).toString());
                  			jg.writeObjectField("avgDistance",row.get( "avgCrow" ).toString());
                  			jg.writeObjectField("numPatients",row.get( "numProviders" ).toString());
                      jg.writeEndObject();
                      

                  }
                  jg.writeEndArray();
                  tx.success();
              }

              jg.writeEndObject();
              jg.flush();
              jg.close();
          }
      };

      return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
  }
  
  @GET
  @Path("/stats/StateByCounty/{stateCode}/{taxonomyCode}")
  public Response statsStateByCounty( final @PathParam("stateCode") String stateCode, @PathParam("taxonomyCode") String taxonomyCode,  @Context GraphDatabaseService db)
  {
      final Map<String, Object> params = MapUtil.map( "stateCode", stateCode );
      params.put("taxonomyCode", taxonomyCode);

      StreamingOutput stream = new StreamingOutput()
      {
          @Override
          public void write( OutputStream os ) throws IOException, WebApplicationException
          {
              JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator( os, JsonEncoding.UTF8 );
              jg.writeStartObject();
              jg.writeArrayFieldStart("Providers");

              try ( Transaction tx = db.beginTx();
                    Result result = db.execute( statsStateByCounty(), params ) )
             {
                  while ( result.hasNext() )
                  {
                      Map<String,Object> row = result.next();
                      jg.writeStartObject();
            			    jg.writeObjectField("PostalCode",row.get( "post5" ).toString());
                  			jg.writeObjectField("TaxonomyCode",row.get( "taxonomyCode" ).toString());
                  			jg.writeObjectField("maxDistance",row.get( "maxCrow" ).toString());
                  			jg.writeObjectField("minDistance",row.get( "minCrow" ).toString());
                  			jg.writeObjectField("avgDistance",row.get( "avgCrow" ).toString());
                  			jg.writeObjectField("numPatients",row.get( "numProviders" ).toString());
                      jg.writeEndObject();
                      

                  }
                  jg.writeEndArray();
                  tx.success();
              }

              jg.writeEndObject();
              jg.flush();
              jg.close();
          }
      };

      return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
  }
  
  @GET
  @Path("/stats/PostalCode/{postalCode}/{taxonomyCode}")
  public Response statsPostalCode( final @PathParam("postalCode") String postalCode,@PathParam("taxonomyCode") String taxonomyCode,   @Context GraphDatabaseService db)
  {
      final Map<String, Object> params = MapUtil.map( "postalCode", postalCode );
      params.put("taxonomyCode", taxonomyCode);
      
      StreamingOutput stream = new StreamingOutput()
      {
          @Override
          public void write( OutputStream os ) throws IOException, WebApplicationException
          {
              JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator( os, JsonEncoding.UTF8 );
              jg.writeStartObject();
              jg.writeArrayFieldStart("Providers");

              try ( Transaction tx = db.beginTx();
                    Result result = db.execute( statsPostalCode(), params ) )
              {
                  while ( result.hasNext() )
                  {
                      Map<String,Object> row = result.next();
                      jg.writeStartObject();
                  			jg.writeObjectField("TaxonomyCode",row.get( "taxonomyCode" ).toString());
                  			jg.writeObjectField("maxDistance",row.get( "maxCrow" ).toString());
                  			jg.writeObjectField("minDistance",row.get( "minCrow" ).toString());
                  			jg.writeObjectField("avgDistance",row.get( "avgCrow" ).toString());
                  			jg.writeObjectField("numPatients",row.get( "numProviders" ).toString());
                      jg.writeEndObject();
                      

                  }
                  jg.writeEndArray();
                  tx.success();
              }

              jg.writeEndObject();
              jg.flush();
              jg.close();
          }
      };

      return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
  }
  
  @GET
  @Path("/stats/County/{countyName}/{stateCode}/{taxonomyCode}")
  public Response statsCountyCode( final @PathParam("stateCode") String stateCode,   @PathParam("countyName") String countyName,  @PathParam("taxonomyCode") String taxonomyCode,    @Context GraphDatabaseService db)
  {
      final Map<String, Object> params = MapUtil.map( "stateCode", stateCode );
      params.put("countyName", countyName);
      params.put("taxonomyCode", taxonomyCode);
      
      StreamingOutput stream = new StreamingOutput()
      {
          @Override
          public void write( OutputStream os ) throws IOException, WebApplicationException
          {
              JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator( os, JsonEncoding.UTF8 );
              jg.writeStartObject();
              jg.writeArrayFieldStart("Providers");

              try ( Transaction tx = db.beginTx();
                    Result result = db.execute( statsCounty(), params ) )
              {
                  while ( result.hasNext() )
                  {
                      Map<String,Object> row = result.next();
                      jg.writeStartObject();
                  			jg.writeObjectField("TaxonomyCode",row.get( "taxonomyCode" ).toString());
                  			jg.writeObjectField("PostalCode",row.get( "post5" ).toString());
                  			jg.writeObjectField("maxDistance",row.get( "maxCrow" ).toString());
                  			jg.writeObjectField("minDistance",row.get( "minCrow" ).toString());
                  			jg.writeObjectField("avgDistance",row.get( "avgCrow" ).toString());
                  			jg.writeObjectField("numPatients",row.get( "numProviders" ).toString());
                      jg.writeEndObject();
                      

                  }
                  jg.writeEndArray();
                  tx.success();
              }
              jg.writeEndObject();
              jg.flush();
              jg.close();
          }
      };

      return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
  }
  
  
  @GET
  @Path("/provider/{postalCode}/{taxonomyCode}")
  public Response providerPostalCode( final @PathParam("postalCode") String postalCode,@PathParam("taxonomyCode") String taxonomyCode,   @Context GraphDatabaseService db)
  {
      final Map<String, Object> params = MapUtil.map( "postalCode", postalCode );
      params.put("taxonomyCode", taxonomyCode);
      
      StreamingOutput stream = new StreamingOutput()
      {
          @Override
          public void write( OutputStream os ) throws IOException, WebApplicationException
          {
              JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator( os, JsonEncoding.UTF8 );
              jg.writeStartObject();
              jg.writeArrayFieldStart("Providers");

              try ( Transaction tx = db.beginTx();
                    Result result = db.execute( providerPostalCodeQry(), params ) )
              {
                  while ( result.hasNext() )
                  {
                      Map<String,Object> row = result.next();
                      jg.writeStartObject();
            			    jg.writeObjectField("NPI",row.get( "NPI" ).toString());
                            jg.writeObjectField("Name",row.get( "lastName" ).toString());
                  			jg.writeObjectField("Address1",row.get( "address1" ).toString());
                  			jg.writeObjectField("City",row.get( "cityName" ).toString());
                  			jg.writeObjectField("State",row.get( "stateName" ).toString());
                  			jg.writeObjectField("FullAddress",row.get( "fullAddress" ).toString());
                  			jg.writeEndObject();
                      

                  }
                  jg.writeEndArray();
                  tx.success();
              }

              jg.writeEndObject();
              jg.flush();
              jg.close();
          }
      };

      return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
  }

  @GET
  @Path("/providerSummary/{stateCode}/{taxonomyCode}")
  public Response taxonomyStateCode( final @PathParam("stateCode") String stateCode,@PathParam("taxonomyCode") String taxonomyCode,   @Context GraphDatabaseService db)
  {
      final Map<String, Object> params = MapUtil.map( "stateCode", stateCode );
      params.put("taxonomyCode", taxonomyCode);
      
      StreamingOutput stream = new StreamingOutput()
      {
          @Override
          public void write( OutputStream os ) throws IOException, WebApplicationException
          {
              JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator( os, JsonEncoding.UTF8 );
              jg.writeStartObject();
              jg.writeArrayFieldStart("Providers");

              try ( Transaction tx = db.beginTx();
                    Result result = db.execute( taxonomyByStateCodeQry(), params ) )
              {
                  while ( result.hasNext() )
                  {
                      Map<String,Object> row = result.next();
                      jg.writeStartObject();
            			    jg.writeObjectField("PostalCode",row.get( "post5" ).toString());
                  			jg.writeObjectField("ProviderCount",row.get( "providerCount" ).toString());
                  			jg.writeEndObject();
                      

                  }
                  jg.writeEndArray();
                  tx.success();
              }

              jg.writeEndObject();
              jg.flush();
              jg.close();
          }
      };

      return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
  }

  @GET
  @Path("/provider/boundingBox/{startLatitude}/{startLongitude}/{endLatitude}/{endLongitude}/{taxonomyCode}")
  public Response providerBoundingBox( final @PathParam("startLatitude") String startLatitude,@PathParam("startLongitude") String startLongitude,@PathParam("endLatitude") String endLatitude,@PathParam("endLongitude") String endLongitude,@PathParam("taxonomyCode") String taxonomyCode,   @Context GraphDatabaseService db)
  {
      final Map<String, Object> params = MapUtil.map( "startLatitude", Float.valueOf(startLatitude));
      params.put("startLongitude", Float.valueOf(startLongitude));
      params.put("endLatitude", Float.valueOf(endLatitude));
      params.put("endLongitude", Float.valueOf(endLongitude));
      params.put("taxonomyCode", taxonomyCode);
      
      StreamingOutput stream = new StreamingOutput()
      {
          @Override
          public void write( OutputStream os ) throws IOException, WebApplicationException
          {
              JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator( os, JsonEncoding.UTF8 );
              jg.writeStartObject();
              jg.writeArrayFieldStart("Providers");

              try ( Transaction tx = db.beginTx();
                    Result result = db.execute( providerBoundingBoxQuery(), params ) )
              {
                  while ( result.hasNext() )
                  {
                      Map<String,Object> row = result.next();
                      jg.writeStartObject();
                      jg.writeObjectField("NPI",row.get( "NPI" ).toString());
                      jg.writeObjectField("Name",row.get( "lastName" ).toString());
                      jg.writeObjectField("Address1",row.get( "address1" ).toString());
                      jg.writeObjectField("City",row.get( "cityName" ).toString());
                      jg.writeObjectField("State",row.get( "stateName" ).toString());
                      jg.writeObjectField("FullAddress",row.get( "fullAddress" ).toString());
                      jg.writeEndObject();
                      

                  }
                  jg.writeEndArray();
                  tx.success();
              }

              jg.writeEndObject();
              jg.flush();
              jg.close();
          }
      };

      return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
  }
  
  @GET
  @Path("/pharmacy/boundingBox/{startLatitude}/{startLongitude}/{endLatitude}/{endLongitude}")
  public Response pharmacyBoundingBoxQuery( final @PathParam("startLatitude") String startLatitude,@PathParam("startLongitude") String startLongitude,@PathParam("endLatitude") String endLatitude,@PathParam("endLongitude") String endLongitude,   @Context GraphDatabaseService db)
  {
      final Map<String, Object> params = MapUtil.map( "startLatitude", Float.valueOf(startLatitude));
      params.put("startLongitude", Float.valueOf(startLongitude));
      params.put("endLatitude", Float.valueOf(endLatitude));
      params.put("endLongitude", Float.valueOf(endLongitude));
      
      StreamingOutput stream = new StreamingOutput()
      {
          @Override
          public void write( OutputStream os ) throws IOException, WebApplicationException
          {
              JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator( os, JsonEncoding.UTF8 );
              jg.writeStartObject();
              jg.writeArrayFieldStart("Pharmacies");

              try ( Transaction tx = db.beginTx();
                    Result result = db.execute( pharmacyBoundingBoxQuery(), params ) )
              {
                  while ( result.hasNext() )
                  {
                      Map<String,Object> row = result.next();
                      jg.writeStartObject();
                      jg.writeObjectField("Name",row.get( "name" ).toString());
                      jg.writeObjectField("Medcare",row.get( "pMedCare" ).toString());
                      jg.writeObjectField("phone",row.get( "phone" ).toString());
                      jg.writeObjectField("Address1",row.get( "address1" ).toString());
                      jg.writeObjectField("City",row.get( "cityName" ).toString());
                      jg.writeObjectField("State",row.get( "stateName" ).toString());
                      jg.writeObjectField("PostalCode",row.get( "postalCode" ).toString());
                      jg.writeObjectField("FullAddress",row.get( "fullAddress" ).toString());
                      jg.writeEndObject();
                      

                  }
                  jg.writeEndArray();
                  tx.success();
              }

              jg.writeEndObject();
              jg.flush();
              jg.close();
          }
      };

      return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
  }
  
  private String nearestProvider()
  {
      return "match (p:PostalCode {PostalCode: {postalCode}})-[h:HAS_DISTANCE]->(p1) where h.distance < 20 with p, collect(p1.PostalCode) as closeZips, point({latitude: p.latitude, longitude: p.longitude}) AS stafford match (a:Location) where a.PostalCode IN closeZips and exists(a.latitude) WITH a, distance(point(a), stafford) AS distance WHERE distance < 32000 WITH a, distance MATCH (t:TaxononmyCode {code: {taxonomyCode}})<-[:HAS_SPECIALTY]-(p:Provider)-[:HAS_PRACTICE_AT]->(a) WHERE p.LastName is not null RETURN t.Code,  p.LastName, p.NPI, a.Address1, a.CityName, a.StateName, apoc.number.format(distance/1600,'#,##0.00;(#,##0.00)') as milesAway, a.latitude, a.longitude ORDER BY distance LIMIT 100";
  }
  
  private String statsState()
  {
      return "match (t:TaxononmyCode {Code:{taxonomyCode}})<-[:HAS_SPECIALTY]-(p:Provider)-[:HAS_PRACTICE_AT]->(a:Location) where a.StateName = {stateCode} with a.Post5 as post5, t.Code as taxonomyCode, p match (p)<-[IP:IS_PATIENT]-() return  taxonomyCode, max(IP.crowDistance) as maxCrow, min(IP.crowDistance) as minCrow, avg(IP.crowDistance) as avgCrow, count(IP) as numProviders;";
  }
  
  private String statsStateByCounty()
  {
      return "match (t:TaxononmyCode {Code:{taxonomyCode}})<-[:HAS_SPECIALTY]-(p:Provider)-[:HAS_PRACTICE_AT]->(a:Location) where a.StateName = {stateCode} with a.Post5 as post5, t.Code as taxonomyCode, p match (p)<-[IP:IS_PATIENT]-() return  post5, taxonomyCode, max(IP.crowDistance) as maxCrow, min(IP.crowDistance) as minCrow, avg(IP.crowDistance) as avgCrow, count(IP) as numProviders order by post5 asc;";
  }
  
  private String statsPostalCode()
  {
      return "match (t:TaxononmyCode {Code:{taxonomyCode}})<-[:HAS_SPECIALTY]-(p:Provider)-[:HAS_PRACTICE_AT]->(a:Location) where a.Post5 = {postalCode} with a.Post5 as post5, t.Code as taxonomyCode, p match (p)<-[IP:IS_PATIENT]-() return post5, taxonomyCode, max(IP.crowDistance) as maxCrow, min(IP.crowDistance) as minCrow, avg(IP.crowDistance) as avgCrow, count(IP) as numProviders;";
  }
  
  private String statsCounty()
  {
      return "match (s:State {code:{stateCode}})<-[:IS_COUNTY_IN]-(:County {Name:{countyName}})<-[:IS_IN_COUNTY]-(pc:PostalCode) with collect(pc.PostalCode) as candidatePostalCodes match (t:TaxononmyCode {Code:{taxonomyCode}})<-[:HAS_SPECIALTY]-(p:Provider)-[:HAS_PRACTICE_AT]->(a:Location) where  a.Post5 IN candidatePostalCodes with a.Post5 as post5, t.Code as taxonomyCode, p match (p)<-[IP:IS_PATIENT]-() return post5, taxonomyCode, max(IP.crowDistance) as maxCrow, min(IP.crowDistance) as minCrow, avg(IP.crowDistance) as avgCrow, count(IP) as numProviders;";
  }
  
  private String providerPostalCodeQry()
  {
      return "match (p:PostalCode {PostalCode: {postalCode}})-[h:HAS_DISTANCE]->(p1) where h.Distance < 20 with p, collect(p1.PostalCode) as closeZips,point({latitude: p.latitude, longitude: p.longitude}) AS zipLocation match (a:Location) where a.PostalCode IN closeZips and exists(a.latitude) WITH a, distance(point(a), zipLocation) AS distance WHERE distance < 32000 WITH a, distance MATCH (t:TaxonomyCode {code:{taxonomyCode}})<-[:HAS_SPECIALTY]-(p:Provider)-[:HAS_PRACTICE_AT]->(a) WHERE p.LastName is not null RETURN t.code as taxonomyCode,  p.LastName as lastName, p.NPI as NPI, a.Address1 as address1, a.CityName as cityName, a.StateName as stateName, a.PostalCode as postalCode, a.Address1 + ' ' + a.CityName + ', ' + a.StateName + ' ' + a.PostalCode as fullAddress, distance/1600 as milesAway, a.latitude, a.longitude ORDER BY distance LIMIT 50;";
  }

  private String taxonomyByStateCodeQry()
  {
      return "match (t:TaxonomyCode {code:{taxonomyCode}})<-[:HAS_SPECIALTY]-(p:Provider)-[:HAS_PRACTICE_AT]->(a:Location) where a.StateName = {stateCode} with a.PostalCode as post5, t.code as taxonomyCode return post5, count(taxonomyCode) as providerCount order by providerCount DESC;";
  }

  private String providerBoundingBoxQuery()
  {
    return "MATCH (a:Location) where a.location < point({latitude: {startLatitude}, longitude: {startLongitude}}) and a.location > point({latitude: {endLatitude}, longitude: {endLongitude}}) with a limit 1000 MATCH (t:TaxonomyCode {code:{taxonomyCode}})<-[:HAS_SPECIALTY]-(p:Provider)-[:HAS_PRACTICE_AT]->(a) WHERE p.LastName is not null RETURN t.code as taxonomyCode,  p.LastName as lastName, p.NPI as NPI, a.Address1 as address1, a.CityName as cityName, a.StateName as stateName, a.PostalCode as postalCode, a.Address1 + ' ' + a.CityName + ', ' + a.StateName + ' ' + a.PostalCode as fullAddress, a.latitude, a.longitude LIMIT 50;";
  }

  private String pharmacyBoundingBoxQuery()
  {
    return "MATCH (a:Location) where a.location < point({latitude: {startLatitude}, longitude: {startLongitude}}) and a.location > point({latitude: {endLatitude}, longitude: {endLongitude}}) with a limit 1000 MATCH (p:Pharmacy)-[:IS_LOCATED_AT]->(a) return p.name as name, p.pMedcare as pMedCare, p.phone as phone, a.Address1 as address1, a.CityName as cityName, a.StateName as stateName, a.PostalCode as postalCode, a.Address1 + ' ' + a.CityName + ', ' + a.StateName + ' ' + a.PostalCode as fullAddress, a.latitude, a.longitude;";
  }
}
