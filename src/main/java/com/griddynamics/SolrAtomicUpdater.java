package com.griddynamics;


import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

public class SolrAtomicUpdater implements Callable<Boolean> {

    private final SolrClient client;
    private final String documentIdPrefix;
    private final int documentsNumber;
    private final String fieldToSearchBy;
    private final com.codahale.metrics.Counter updatesCounter;

    SolrAtomicUpdater(SolrClient solrClient, String documentIdPrefix, int documentsNumber, String fieldToSearchBy, com.codahale.metrics.Counter updatesCounter ) {
        this.client = solrClient;
        this.documentIdPrefix = documentIdPrefix;
        this.documentsNumber = documentsNumber;
        this.fieldToSearchBy = fieldToSearchBy;
        this.updatesCounter = updatesCounter;
    }

    @Override
    public Boolean call() {

        while (!Thread.currentThread().isInterrupted()) {

            int randomEntryId = ThreadLocalRandom.current().nextInt(0, documentsNumber);

            SolrInputDocument sdoc = createDocumentUpdate(randomEntryId, FieldValuesSingleton.INSTANCE.getRandomTerm());

            try {
                client.add(sdoc); // send it to the solr server
                softCommit(client, sdoc);
                updatesCounter.inc();
            } catch (SolrServerException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return true;
    }

    private SolrInputDocument createDocumentUpdate(int entryId, String term) {
        SolrInputDocument sdoc = new SolrInputDocument();
        sdoc.addField("id", documentIdPrefix + entryId);
        Map<String, Object> fieldModifier = new HashMap<String, Object>(1);
        fieldModifier.put("set", term);
        sdoc.addField(fieldToSearchBy, fieldModifier); // add the map as the field value
        return sdoc;
    }

    private void softCommit(SolrClient client,
                            SolrInputDocument sdoc) throws SolrServerException, IOException {
        UpdateRequest req = new UpdateRequest();
        req.setAction(UpdateRequest.ACTION.COMMIT, false, false, true);
        req.add(sdoc);
        UpdateResponse rsp = req.process(client);
    }
}
