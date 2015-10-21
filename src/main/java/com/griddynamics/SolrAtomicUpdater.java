package com.griddynamics;


        import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
        import org.apache.solr.client.solrj.request.UpdateRequest;
        import org.apache.solr.client.solrj.response.UpdateResponse;
        import org.apache.solr.common.SolrInputDocument;

        import java.io.IOException;
import java.util.*;
        import java.util.concurrent.Callable;
        import java.util.concurrent.ThreadLocalRandom;

public class SolrAtomicUpdater implements Callable<Boolean> {

    //private static final int START_DOCUMENT_ID_NUMBER = 1500000;
    //private static final int END_DOCUMENT_ID_NUMBER = 3000000;
    //private List<String> documentIds;
    //private List<String> terms;

   // public SolrAtomicUpdater() {
        //this.documentIds = getDocumentIds();
        //terms = FieldValuesSingleton.INSTANCE.getTerms();
   // }

    @Override
    public Boolean call() {
        // log4j easy configuration
        //BasicConfigurator.configure();
        // create the SolrJ client


        HttpSolrClient client = new HttpSolrClient(
                "http://localhost:8983/solr/Collection");

        while (!Thread.currentThread().isInterrupted()) {
            //long millis = System.currentTimeMillis();



          //  for (int i = START_DOCUMENT_ID_NUMBER; i < END_DOCUMENT_ID_NUMBER; i++) {
                // create the document


                int randomEntryId = ThreadLocalRandom.current().nextInt(0, SimpleQuerySampler.DOCUMENTS_NUMBER);

                SolrInputDocument sdoc = createDocumentUpdate(randomEntryId, FieldValuesSingleton.INSTANCE.getRandomTerm());

                try {
                    client.add(sdoc); // send it to the solr server
                    softCommit(client, sdoc);
                } catch (SolrServerException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }




            //}
        }

        // client.commit();

        //client.close(); // shutdown client before we exit

        return true;
    }



/*
    private List<String> getDocumentIds() {
        String url = "http://localhost:8983/solr/Collection";
        SolrClient solr = new HttpSolrClient(url);

        SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.set("rows", SimpleQuerySampler.DOCUMENTS_NUMBER/2);
        query.addSort("id", SolrQuery.ORDER.desc);
        query.setFields("id");
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
            fieldList.add((String) results.get(i).getFirstValue("id"));
        }
        return fieldList;
    }*/

    private SolrInputDocument createDocumentUpdate(int entryId, String term) {
        SolrInputDocument sdoc = new SolrInputDocument();
        sdoc.addField("id",
                "GenaratedSearchData.csv#"
                        + entryId);
        Map<String, Object> fieldModifier = new HashMap<String, Object>(1);
        fieldModifier.put("set", term);
        sdoc.addField("first_name", fieldModifier); // add the map as the field value
        return sdoc;
    }

    private void softCommit(HttpSolrClient client,
                                       SolrInputDocument sdoc) throws SolrServerException, IOException {
        UpdateRequest req = new UpdateRequest();
        req.setAction(UpdateRequest.ACTION.COMMIT, false, false, true, 1);
        req.add(sdoc);
        UpdateResponse rsp = req.process(client);
    }
}
