package com.griddynamics;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.TermsParams;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public enum FieldValuesSingleton
{
    INSTANCE;

    private final List<String> terms;

    FieldValuesSingleton()
    {
        terms = loadTerms();
    }

    private List<String> loadTerms() {

        SolrParams q = new SolrQuery().setRequestHandler("/terms")
                .set(TermsParams.TERMS, true).set(TermsParams.TERMS_FIELD, "first_name")
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
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        NamedList tags = (NamedList) ((NamedList)queryResponse.getResponse().get("terms")).get("first_name");

        List<String> result = new ArrayList<String>();
        for (Iterator iterator = tags.iterator(); iterator.hasNext();) {
            Map.Entry tag = (Map.Entry) iterator.next();
            result.add(tag.getKey().toString());
            //System.out.println(tag.getKey().toString());
        }

        return result;
    }

 /*   private List<String> getAllFieldValues() {
        String url = "http://localhost:8983/solr/Collection";
        SolrClient solr = new HttpSolrClient(url);

        SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.set("rows", SimpleQuerySampler.DOCUMENTS_NUMBER/2);
        query.addSort("id", SolrQuery.ORDER.asc);
        query.setFields("first_name");
        query.setStart(0);

        QueryResponse response = null;
        try {
            response = solr.query(query);
        } catch (SolrServerException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<String> fieldList = new ArrayList<>();
        SolrDocumentList results = response.getResults();
        for (int i = 0; i < results.size(); i++) {
            //System.out.println(results.get(i).getFirstValue("first_name"));
            //System.out.println(results.get(i));
            fieldList.add((String) results.get(i).getFirstValue("first_name"));
        }
        return fieldList;
    }*/

    /*private List<String> getAllFieldValues() {

        String url = "http://localhost:8983/solr/Collection";
        SolrClient solr = new HttpSolrClient(url);

        ExecutorService exService = Executors.newFixedThreadPool(5);

        List<String> results = new ArrayList<>();
        for (int i = 0; i < SimpleQuerySampler.DOCUMENTS_NUMBER/2; i++) {

            Future<String> solrQueryFuture = exService.submit(new SolrQueryCallable(i, solr));
            try {
                results.add(solrQueryFuture.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        exService.shutdown();
        try {
            exService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return results;
    }*/

    public List<String> getTerms()
    {
        return terms;
    }

    public String getRandomTerm() {
        int randomTermIndex = ThreadLocalRandom.current().nextInt(0, terms.size());
        return terms.get(randomTermIndex);
    }
}