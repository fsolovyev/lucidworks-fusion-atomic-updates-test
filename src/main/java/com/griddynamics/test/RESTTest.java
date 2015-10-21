package com.griddynamics.test;

import org.apache.log4j.BasicConfigurator;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.TermsParams;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RESTTest {
    public static void main(String[] args) {


        BasicConfigurator.configure();
        getTerms();
       // testSearchRequest();

//        String url = "http://localhost:8983/solr/Collection";
//        SolrClient solr = new HttpSolrClient(url);
//
//        List<String> results = new ArrayList<>();
//        for (int i = 0; i < 1500; i++) {
//            results.add(getFirstNameById(i, solr));
//        }
//
//        for (int i = 0; i < results.size(); i++) {
//            System.out.println(i + " " + results.get(i));
//        }

    }
   public static void getTerms() {
//
//        Client client = ClientBuilder.newBuilder().newClient();
//        WebTarget target = client.target("http://localhost:8983/solr/Collection");
//        String test = target.path("terms").queryParam("terms.fl", "first_name").queryParam("").request(MediaType.TEXT_PLAIN)
//                .get(String.class);;
//
//        Invocation.Builder builder = target.request();
//        Response response = builder.get();
//        System.out.println(test);

//        WebClient client = WebClient.create("http://localhost:8983").header("","");
//        //AdvisorClient res =
//            Response res =    client.path("solr/Collection/terms").query("terms.fl","first_name").accept("text/xml").get();//.accept("text/xml").get(AdvisorClient.class);
//        System.out.println(res);

        //String url = "http://localhost:8983/solr";
  /*
    HttpSolrServer is thread-safe and if you are using the following constructor,
    you *MUST* re-use the same instance for all requests.  If instances are created on
    the fly, it can cause a connection leak. The recommended practice is to keep a
    static instance of HttpSolrServer per solr server url and share it for all requests.
    See https://issues.apache.org/jira/browse/SOLR-861 for more details
  */
        //SolrServer server = new HttpSolrServer( url );


        SolrParams q = new SolrQuery().setRequestHandler("/terms")
                .set(TermsParams.TERMS, true).set(TermsParams.TERMS_FIELD, "first_name")
                //.set(TermsParams.TERMS_LOWER_INCLUSIVE, false)
                .set(TermsParams.TERMS_LIMIT, -1);
        QueryResponse queryResponse = null;
       // try {

            String url = "http://localhost:8983/solr/Collection";
  /*
    HttpSolrServer is thread-safe and if you are using the following constructor,
    you *MUST* re-use the same instance for all requests.  If instances are created on
    the fly, it can cause a connection leak. The recommended practice is to keep a
    static instance of HttpSolrServer per solr server url and share it for all requests.
    See https://issues.apache.org/jira/browse/SOLR-861 for more details
  */

        SolrClient solr = new HttpSolrClient(url);

        try {
            queryResponse = solr.query(q);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //  } catch (SolrServerException e) {
            //Logger.error(e, "Error when querying server.");
            //throw new IOException(e);
          //  e.printStackTrace();
        //}

        NamedList tags = (NamedList) ((NamedList)queryResponse.getResponse().get("terms")).get("first_name");

        List<String> result = new ArrayList<String>();
        for (Iterator iterator = tags.iterator(); iterator.hasNext();) {
            Map.Entry tag = (Map.Entry) iterator.next();
            result.add(tag.getKey().toString());
            System.out.println(tag.getKey().toString());
        }



    }


    private static String getFirstNameById(int idNumber, SolrClient solr) {


        SolrQuery query = new SolrQuery();
        query.setQuery("id:\"/GenaratedSearchData.csv.csv#" + idNumber + "\"");
        //query.set("rows", 15);
        //query.addSort("identifier", SolrQuery.ORDER.asc);
        query.setFields("first_name");
        //query.setStart(0);

        QueryResponse response = null;
        try {
            response = solr.query(query);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
       // List<String> fieldList = new ArrayList<>();
        SolrDocumentList results = response.getResults();
        return (String) results.get(0).getFirstValue("first_name");
    }


        public static void testSearchRequest() {
            String url = "http://localhost:8983/solr/Collection";
            SolrClient solr = new HttpSolrClient(url);

            SolrQuery query = new SolrQuery();
            query.setQuery("*:*");
            query.set("rows", 15000);
            query.addSort("id", SolrQuery.ORDER.asc);
            query.setFields("first_name");
            query.setStart(0);

        QueryResponse response = null;
        try {
            response = solr.query(query);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<String> fieldList = new ArrayList<>();
        SolrDocumentList results = response.getResults();
            for (int i = 0; i < results.size(); i++) {
                //System.out.println(results.get(i).getFirstValue("first_name"));
                //System.out.println(results.get(i));
                fieldList.add((String) results.get(i).getFirstValue("first_name"));
            }
        }

}
