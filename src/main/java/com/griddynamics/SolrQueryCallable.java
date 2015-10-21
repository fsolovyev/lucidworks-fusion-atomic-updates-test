package com.griddynamics;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;
import java.util.concurrent.Callable;

public class SolrQueryCallable implements Callable<String> {

    private final int idNumber;
    private final SolrClient solr;

    public SolrQueryCallable(int idNumber, SolrClient solr) {
        this.idNumber = idNumber;
        this.solr = solr;
    }

    @Override
    public String call() throws Exception {
        return getFirstNameById(idNumber, solr);
    }

    private String getFirstNameById(int idNumber, SolrClient solr) {

        SolrQuery query = new SolrQuery();
        query.setQuery("id:\"/GenaratedSearchData.csv.csv#" + idNumber + "\"");
        query.setFields("first_name");

        QueryResponse response = null;
        try {
            response = solr.query(query);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        SolrDocumentList results = response.getResults();
        return (String) results.get(0).getFirstValue("first_name");
    }

}



