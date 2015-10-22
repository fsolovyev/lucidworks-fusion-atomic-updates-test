package com.griddynamics;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * To start Solr in Standalone mode ( type=solr )
 * ./bin/start -e techproducts -noprompt
 * <p>
 * To start Solr in SolrCloud mode ( type=solrcloud )
 * ./bin/start -e cloud -noprompt
 * <p>
 * To start Fusion ( type=fusion)
 * ./bin/fusion start
 * <p>
 * {@link #runTest(JavaSamplerContext)} is called n times ( where n is the number of test runs you ask
 * jmeter to run and is specified in LoopController.loops in query.jmx )
 * <p>
 * Use {@link #setupTest(JavaSamplerContext)} to create the set of queries which you then
 * want {@link #runTest(JavaSamplerContext)} to utilize in it's test run.
 * <p>
 * This sample code always fires match all queries.
 */
public class SimpleQuerySampler extends AbstractJavaSamplerClient implements Serializable {
    private static final long serialVersionUID = 1L;

    // keeps track of how many tests are running this sampler and when there are
    // none, a final hard commit is sent.
    private static AtomicInteger refCounter = new AtomicInteger(0);
    private static final MetricRegistry metrics = new MetricRegistry();
    private static final com.codahale.metrics.Timer queryTimer = metrics.timer("query");
    private static final com.codahale.metrics.Counter noResultsCounter = metrics.counter("noresults");
    private static final com.codahale.metrics.Counter excCounter = metrics.counter("exceptions");

    private static ConsoleReporter reporter = null;

    /*  Using 2 fields (fusionClient and solrClient) instead of only 1,
        because I assume that a SolrClient instance created to connect to Fusion
        perhaps could do more work and be less performant than SolrClient configured to work with Solr.
        But fusionClient allows to use a query pipeline. So for the search queries I use fusionClient.
        For all the other queries that don't need any Fusion functionality is used solrClient.*/
    private static volatile SolrClient fusionClient = null;

    /*
      HttpSolrServer is thread-safe and if you are using the following constructor,
      you *MUST* re-use the same instance for all requests.  If instances are created on
      the fly, it can cause a connection leak. The recommended practice is to keep a
      static instance of HttpSolrServer per solr server url and share it for all requests.
      See https://issues.apache.org/jira/browse/SOLR-861 for more details
    */
    private static volatile SolrClient solrClient = null;

    private static volatile String fieldToSearchBy;

    //private static Random random;
    private CloseableHttpClient httpClient;
    private String[] queries = new String[100000];
    private int queryCounter = 0;
    private static CountDownLatch latch = new CountDownLatch(3);
    private static FutureTask<Boolean> updaterTask;
    private static Thread updaterThread;

    public static SolrClient getSolrClient() {
        return solrClient;
    }

    public static String getFieldToSearchBy() {
        return fieldToSearchBy;
    }

    @Override
    public SampleResult runTest(JavaSamplerContext context) {
        SampleResult result = new SampleResult();
        result.sampleStart();

        SolrQuery query = new SolrQuery();
        query.setQuery(queries[queryCounter++]);

        final com.codahale.metrics.Timer.Context queryTimerCtxt = queryTimer.time();
        try {
            QueryResponse qr = fusionClient.query(query);
            if (qr.getResults().getNumFound() == 0)
                noResultsCounter.inc();

            result.setResponseOK();
        } catch (Exception solrExc) {
            excCounter.inc();
        } finally {
            queryTimerCtxt.stop();
        }

        result.sampleEnd();

        return result;
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument("RANDOM_SEED", "5150");
        defaultParameters.addArgument("mode", "solr");

        defaultParameters.addArgument("COLLECTION", "techproducts");
        defaultParameters.addArgument("SOLR_URL", "http://localhost:8983/solr");

        defaultParameters.addArgument("ZK_HOST", "localhost:9983");

        defaultParameters.addArgument("QUERY_PIPELINE", "http://localhost:8764/api/apollo/query-pipelines/default/collections/system_metrics");
        defaultParameters.addArgument("username", "admin");
        defaultParameters.addArgument("password", "password123");
        return defaultParameters;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {
        super.setupTest(context);

        refCounter.incrementAndGet(); // keep track of threads using the statics in this class

        Map<String, String> params = loadParamsMap(context);

        setupFusionClient(params);

        setupFieldToSearchBy(params);

        setupSolrClient(params);
        /*
            setupSolrClient and setupFieldToSearchBy must be called before
            generateSearchQueries because they initialise FieldValuesSingleton.
            Initialising of a Singleton this way is very fragile.
            The alternative is to use Inversion of Control.
         */
        generateSearchQueries(params);

        setupReporter();
        startAtomicUpdatesThread(params);
        waitAllInitialisationFinished();

    }

    private void setupFieldToSearchBy(Map<String, String> params) {
        synchronized (SimpleQuerySampler.class) {
            if (fieldToSearchBy == null)
                fieldToSearchBy = params.get("fieldToSearchBy");
        }
    }

    private void startAtomicUpdatesThread(Map<String, String> params) {
        synchronized (SimpleQuerySampler.class) {
            if (updaterThread == null) {
                SolrAtomicUpdater updater = new SolrAtomicUpdaterBuilder()
                        .setSolrClient(solrClient)
                        .setDocumentIdPrefix(params.get("documentIdPrefix"))
                        .setDocumentsNumber(Integer.parseInt(params.get("documentsNumber")))
                        .setFieldToSearchBy(params.get("fieldToSearchBy"))
                        .createSolrAtomicUpdater();
                updaterTask = new FutureTask<Boolean>(updater);
                updaterThread = new Thread(updaterTask);
                updaterThread.start();
            }
        }
    }

    private void setupReporter() {
        synchronized (SimpleQuerySampler.class) {
            if (reporter == null) {
                reporter = ConsoleReporter.forRegistry(metrics)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS).build();
                reporter.start(1, TimeUnit.MINUTES);
            }
        }
    }

    private Map<String, String> loadParamsMap(JavaSamplerContext context) {
        Map<String, String> params = new HashMap<>();
        Iterator<String> paramNames = context.getParameterNamesIterator();
        while (paramNames.hasNext()) {
            String paramName = paramNames.next();
            String param = context.getParameter(paramName);
            if (param != null)
                params.put(paramName, param);
        }
        return params;
    }

    private void setupFusionClient(Map<String, String> params) {
        String mode = params.get("mode");
        synchronized (SimpleQuerySampler.class) {
            if (fusionClient == null) {
                if ("solrcloud".equals(mode)) {
                    String collection = params.get("COLLECTION");
                    String zkString = params.get("ZK_HOST");
                    fusionClient = new CloudSolrClient(zkString);
                    ((CloudSolrClient) fusionClient).setDefaultCollection(collection);
                    ((CloudSolrClient) fusionClient).connect();
                } else if ("fusion".equals(mode)) {
                    String username = params.get("username");
                    String password = params.get("password");
                    String url = params.get("QUERY_PIPELINE");

                    httpClient = HttpClientBuilder.create().useSystemProperties()
                            .addInterceptorLast(new PreEmptiveBasicAuthenticator(username, password))
                            .build();
                    fusionClient = new HttpSolrClient(url, httpClient, new XMLResponseParser());
                }
            }
        }

    }

    private void setupSolrClient(Map<String, String> params) {
        synchronized (SimpleQuerySampler.class) {
            if (solrClient == null) {
                String collection = params.get("COLLECTION");
                String url = params.get("SOLR_URL");
                if (!url.endsWith("/")) {
                    url = url + "/";
                }
                solrClient = new HttpSolrClient(url + collection);
            }
        }
    }

    private void waitAllInitialisationFinished() {
        latch.countDown();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void generateSearchQueries(Map<String, String> params) {
        for (int i = 0; i < queries.length; i++) {
            queries[i] = params.get("fieldToSearchBy") + ":" + FieldValuesSingleton.INSTANCE.getRandomTerm();
        }
    }

    public static class PreEmptiveBasicAuthenticator implements HttpRequestInterceptor {
        private final UsernamePasswordCredentials credentials;

        public PreEmptiveBasicAuthenticator(String user, String pass) {
            credentials = new UsernamePasswordCredentials(user, pass);
        }

        public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
            request.addHeader(new BasicScheme().authenticate(credentials, request, context));

        }
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        if (fusionClient != null) {
            int refs = refCounter.decrementAndGet();
            if (refs == 0) {
                getLogger().info("Shutting down solr client");
                if (reporter != null) {
                    reporter.report();
                    reporter.stop();
                }
                try {
                    fusionClient.close();
                    solrClient.close();
                } catch (IOException e) {
                    fusionClient = null;
                }
            }
        }
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                httpClient = null;
            }
        }
        if (updaterTask != null) {
            updaterTask.cancel(true);
        }
        super.teardownTest(context);
    }

}
