package com.griddynamics;

import org.apache.solr.client.solrj.SolrClient;

public class SolrAtomicUpdaterBuilder {
    private SolrClient solrClient;
    private String documentIdPrefix;
    private int documentsNumber;
    private String fieldToSearchBy;

    public SolrAtomicUpdaterBuilder setSolrClient(SolrClient solrClient) {
        this.solrClient = solrClient;
        return this;
    }

    public SolrAtomicUpdaterBuilder setDocumentIdPrefix(String documentIdPrefix) {
        this.documentIdPrefix = documentIdPrefix;
        return this;
    }

    public SolrAtomicUpdaterBuilder setDocumentsNumber(int documentsNumber) {
        this.documentsNumber = documentsNumber;
        return this;
    }

    public SolrAtomicUpdaterBuilder setFieldToSearchBy(String fieldToSearchBy) {
        this.fieldToSearchBy = fieldToSearchBy;
        return this;
    }

    public SolrAtomicUpdater createSolrAtomicUpdater() {
        return new SolrAtomicUpdater(solrClient, documentIdPrefix, documentsNumber, fieldToSearchBy);
    }
}