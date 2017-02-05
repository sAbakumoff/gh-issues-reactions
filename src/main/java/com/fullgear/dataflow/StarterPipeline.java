package com.fullgear.dataflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.jcabi.github.Github;
import com.jcabi.github.RtGithub;
import com.jcabi.github.wire.CarefulWire;
import com.jcabi.http.Request;
import com.jcabi.http.request.ApacheRequest;
import com.jcabi.http.response.JsonResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.language.v1beta1.model.Entity;
import com.google.api.services.language.v1beta1.model.Sentiment;
import com.google.api.services.language.v1beta1.model.Token;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.Charsets;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.json.JsonObject;
import javax.ws.rs.core.HttpHeaders;
import javax.xml.bind.DatatypeConverter;

public class StarterPipeline {
	
	static class ExtractIssueBody extends DoFn<TableRow, String> { 
		private static final Logger LOG = LoggerFactory.getLogger(ExtractIssueBody.class);
		private static final Request REQUEST =
		        new ApacheRequest("https://api.github.com")
		            .header(HttpHeaders.ACCEPT, "application/vnd.github.squirrel-girl-preview")
		            .header(
		                    HttpHeaders.AUTHORIZATION,
		                    String.format(
		                        "Basic %s",
		                        DatatypeConverter.printBase64Binary(
		                            String.format("%s:%s", null, null)
		                                .getBytes(Charsets.UTF_8)
		                        )
		                    )
		                )
		            .through(CarefulWire.class, 5);
		public static void GithubCall(String url) throws IOException{
			String reactionsUrl = url.replaceFirst("https://api.github.com", "").concat("/reactions");
			//LOG.info("reactions url: " + reactionsUrl);
			Request request = REQUEST.uri().path(reactionsUrl).back();
			JsonResponse resp = request.fetch().as(JsonResponse.class);
			final List<JsonObject> items = resp.json().readArray().getValuesAs(JsonObject.class);
			for (final JsonObject item : items) {
				LOG.info("Reaction " + item.getString("content"));
			}
		}
		@Override 
		  public void processElement(ProcessContext c) { 
    		TableRow issueRow = c.element();
    		String type = (String) issueRow.get("type");
    		
    		if(type.equalsIgnoreCase("IssuesEvent")){
    			JSONParser parser = new JSONParser();
    			String payload = (String) issueRow.get("payload");
				try {
					JSONObject payloadObject = (JSONObject) parser.parse(payload);
		    		String action = (String) payloadObject.get("action");		    		
		    		if(action.equalsIgnoreCase("opened")){
		    			//LOG.info("Found the opened issue!!");
		    			JSONObject issueObject = (JSONObject) payloadObject.get("issue");
		    			String url = (String) issueObject.get("url");
		    			GithubCall(url);
		    			/*String body = (String) issueObject.get("body");
		    			if(body!=null && !body.isEmpty())
		    				c.output(body);*/
		    		}								
				} catch (ParseException | IOException e) {
					LOG.info("Exception! " + e.toString());
				}
    		}
		}
	}
	
	static class BuildTableRow extends DoFn<String, TableRow>{
		@Override
		public void processElement(ProcessContext c){
			String body = c.element();
			TableRow row = new TableRow();
			row.set("body", body);
			row.set("action", "opened");
			c.output(row);
		}
	}
  
	public static TableReference getTableReference() {
	  TableReference tableRef = new TableReference();
	  tableRef.setProjectId("in-full-gear");
	  tableRef.setDatasetId("Dataset1");
	  tableRef.setTableId("issues_temp_table");
	  return tableRef;
	}

  private static TableSchema getSchema() {
	  List<TableFieldSchema> fields = new ArrayList<>();
	  fields.add(new TableFieldSchema().setName("body").setType("STRING"));
	  fields.add(new TableFieldSchema().setName("action").setType("STRING"));
	  /*fields.add(new TableFieldSchema().setName("polarity").setType("FLOAT"));
	  fields.add(new TableFieldSchema().setName("magnitude").setType("FLOAT"));
	  fields.add(new TableFieldSchema().setName("syntax").setType("STRING"));*/
	  TableSchema schema = new TableSchema().setFields(fields);
	  return schema;
	}

  public static void main(String[] args) throws IOException {

	DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
	options.setTempLocation("gs://in-full-gear-temp");
	options.setStreaming(true);
	options.setProject("in-full-gear");
	options.setStagingLocation("gs://in-full-gear-temp");
	//options.setRunner(BlockingDataflowPipelineRunner.class);
	options.setRunner(DirectPipelineRunner.class);
	Pipeline pipeline = Pipeline.create(options);

	pipeline.apply(BigQueryIO.Read
	         .named("GithubPostedIssues")
	         .from("in-full-gear:Dataset1.gh_events_3000"))
	.apply(ParDo.of(new ExtractIssueBody()))
	.apply(ParDo.of(new BuildTableRow()))
	.apply(BigQueryIO.Write.to(getTableReference()).withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED).
			withWriteDisposition(WriteDisposition.WRITE_APPEND).withSchema(getSchema()));
	pipeline.run();
  }
}