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

/*
    SimpleQuerySampler.solrClient and SimpleQuerySampler.fieldToSearchBy
    must be already initialised before FieldValuesSingleton can be used.
 */
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
                .set(TermsParams.TERMS, true).set(TermsParams.TERMS_FIELD, SimpleQuerySampler.getFieldToSearchBy())
                .set(TermsParams.TERMS_LIMIT, -1);
        QueryResponse queryResponse = null;

        try {
            queryResponse = SimpleQuerySampler.getSolrClient().query(q);
        } catch (SolrServerException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        NamedList tags = (NamedList) ((NamedList)queryResponse.getResponse().get("terms")).get(SimpleQuerySampler.getFieldToSearchBy());

        List<String> result = new ArrayList<String>();
        for (Iterator iterator = tags.iterator(); iterator.hasNext();) {
            Map.Entry tag = (Map.Entry) iterator.next();
            result.add(tag.getKey().toString());
        }

        return result;
    }

    public List<String> getTerms()
    {
        return terms;
    }

    public String getRandomTerm() {
        int randomTermIndex = ThreadLocalRandom.current().nextInt(0, terms.size());
        return terms.get(randomTermIndex);
    }
}