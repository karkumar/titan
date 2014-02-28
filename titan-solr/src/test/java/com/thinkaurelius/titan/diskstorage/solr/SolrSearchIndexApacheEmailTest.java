package com.thinkaurelius.titan.diskstorage.solr;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.Mapping;
import com.thinkaurelius.titan.core.Parameter;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProviderTest;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.diskstorage.indexing.StandardKeyInformation;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.query.condition.PredicateCondition;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.thinkaurelius.titan.diskstorage.solr.SolrConstants.*;
import static org.junit.Assert.assertEquals;

/**
 * @author Jared Holmberg (jholmberg@bericotechnologies.com)
 */
public class SolrSearchIndexApacheEmailTest extends SolrSearchIndexTest {

    public static final String TYPE = "type", NAME="name", ENTITY="entity", EMAIL_ADDRESS = "email_address";
    public static final String UUID = "message_uuid", SUBJECT="subject", FROM="message_from", TO="message_to";
    public static final String REPLY_TO = "in_reply_to", REFERENCES="references", MESSAGE_ID="message_id";
    public static final String LIST_ID ="list_id", DATE="date", BODY="body";

    @Before
    public void setUpd() throws Exception {
        //allKeys.clear();
        allKeys.put(TYPE, new StandardKeyInformation(String.class, new Parameter("mapping", Mapping.STRING)));
        allKeys.put(NAME, new StandardKeyInformation(String.class, new Parameter("mapping", Mapping.STRING)));
        allKeys.put(ENTITY,new StandardKeyInformation(String.class, new Parameter("mapping", Mapping.STRING)));
        allKeys.put(EMAIL_ADDRESS, new StandardKeyInformation(String.class, new Parameter("mapping", Mapping.STRING)));
        allKeys.put(UUID, new StandardKeyInformation(String.class, new Parameter("mapping", Mapping.STRING)));
        allKeys.put(SUBJECT, new StandardKeyInformation(String.class, new Parameter("mapping", Mapping.TEXT)));
        allKeys.put(FROM, new StandardKeyInformation(String.class, new Parameter("mapping", Mapping.STRING)));
        allKeys.put(TO, new StandardKeyInformation(String.class, new Parameter("mapping", Mapping.STRING)));
        allKeys.put(REPLY_TO,new StandardKeyInformation(String.class, new Parameter("mapping", Mapping.STRING)));
        allKeys.put(REFERENCES, new StandardKeyInformation(String.class, new Parameter("mapping", Mapping.STRING)));
        allKeys.put(MESSAGE_ID, new StandardKeyInformation(String.class, new Parameter("mapping", Mapping.STRING)));
        allKeys.put(LIST_ID, new StandardKeyInformation(String.class, new Parameter("mapping", Mapping.STRING)));
        allKeys.put(DATE, new StandardKeyInformation(String.class, new Parameter("mapping", Mapping.STRING)));
        allKeys.put(BODY, new StandardKeyInformation(String.class, new Parameter("mapping", Mapping.STRING)));
    }

    @Override
    public IndexProvider openIndex() throws StorageException {
        return new SolrIndex(getLocalSolrTestConfig());
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    public static final Configuration getLocalSolrTestConfig() {
        Configuration config = new BaseConfiguration();

        //SOLR_MODE_HTTP
        config.setProperty(SOLR_MODE, SOLR_MODE_HTTP);
        config.setProperty(SOLR_HTTP_URL, "http://localhost:8983/solr");
        config.setProperty(SOLR_HTTP_CONNECTION_TIMEOUT, 10000); //in milliseconds

        //SOLR_MODE_EMBEDDED
//        config.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, StorageSetup.getHomeDir("solr"));
//        String home = "titan-solr/target/test-classes/solr/";
//        config.setProperty(SOLR_HOME, home);

        //SOLR CLOUD
//        config.setProperty(SOLR_MODE, SOLR_MODE_CLOUD);
//        config.setProperty(SOLR_CLOUD_ZOOKEEPER_URL, "localhost:2181");
//        config.setProperty(SOLR_CLOUD_COLLECTION, "store");

        //Common settings needed in all modes
        config.setProperty(SOLR_CORE_NAMES, "edge,vertex");
        config.setProperty(SOLR_KEY_FIELD_NAMES, "edge=message_uuid,vertex=message_uuid");

        return config;
    }

    protected Map<String, Object> createDocument()
    {
        Map doc1 = new HashMap<String, Object>() {{
            put(TYPE, "email");
            put(NAME, "user");
            put(ENTITY, "email");
            put(EMAIL_ADDRESS, "user@localhost.com");
            //put(UUID, "1ce44e065e1c11e3a468fa163e1e9c80");
            put(SUBJECT, "re: Test Message");
            put(FROM, "user@localhost.com");
            put(TO, "list@localhost.com");
            put(REPLY_TO, "Test Message");
            put(REFERENCES, "20100505114747.42C0323889BF@localhost.com");
            put(MESSAGE_ID, "20100505114747.42C0323889BF@localhost.com");
            put(LIST_ID, "list@locahost.com");
            put(DATE, "Fri, 14 May 2010 07:05:27 -0000");
            put(BODY, "This is the body of the message ! ");
        }};

        return doc1;
    }
    @Test
    public void testApacheData() throws StorageException
    {
        this.openIndex().clearStorage();


        String[] stores = new String[] { "vertex" };

        Map<String,Object> doc1 = createDocument();

        for (String store : stores) {
            initialize(store);

            add(store,"1ce44e065e1c11e3a468fa163e1e9c80",doc1,true);
        }

        clopen();

        for (String store : stores) {

            List<String> result = tx.query(new IndexQuery(store, PredicateCondition.of("body", Text.CONTAINS, "body")));
            assertEquals(1,result.size());
            assertEquals(ImmutableSet.of("1ce44e065e1c11e3a468fa163e1e9c80"), ImmutableSet.copyOf(result));
        }
    }
}
