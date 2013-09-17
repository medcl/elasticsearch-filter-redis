package org.elasticsearch.index.utils;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.RedisFilter;
import org.testng.annotations.Test;

/**
 * User: Medcl
 * Date: 12-11-2
 * Time: 上午9:29
 */
public class RedisHanlderTest {
    @Test
    public void testAddNewItem() throws Exception {

       {RedisHanlder redisHanlder=RedisHanlder.getInstance("localhost",6379,true,false);
        System.out.println(redisHanlder.convert("key1", "北京"));
        System.out.println(redisHanlder.convert("key1","北京"));
        System.out.println(redisHanlder.convert("key1","北京2"));
        System.out.println(redisHanlder.convert("key1","北京3"));
        System.out.println(redisHanlder.convert("key1","北京4"));
    }
}

    /**
     */
    @Test
    public static class RedisFilterTests {

        @Test
        public void testTermFilter() throws Exception {
            String fieldName = "field1";
            Directory rd = new RAMDirectory();
            IndexWriter w = new IndexWriter(rd, new IndexWriterConfig(Lucene.VERSION, new KeywordAnalyzer()));
            for (int i = 0; i < 100; i++) {
                Document doc = new Document();
                int term = i * 10; //terms are units of 10;
                doc.add(new Field(fieldName, "" + term, StringField.TYPE_NOT_STORED));
                doc.add(new Field("all", "xxx", StringField.TYPE_NOT_STORED));
                doc.add(new Field("_uid", "id"+i, StringField.TYPE_NOT_STORED));
                w.addDocument(doc);
                if ((i % 40) == 0) {
                    w.commit();
                }
            }
            AtomicReader reader = new SlowCompositeReaderWrapper(DirectoryReader.open(w, true));
            w.close();

    //        TermFilter tf = new TermFilter(new Term(fieldName, "19"));
    //        FixedBitSet bits = (FixedBitSet) tf.getDocIdSet(reader.getContext(), reader.getLiveDocs());
    //        assertThat(bits, nullValue());
    //
    //        tf = new TermFilter(new Term(fieldName, "20"));
    //        bits = (FixedBitSet) tf.getDocIdSet(reader.getContext(), reader.getLiveDocs());
    //        assertThat(bits.cardinality(), equalTo(1));
    //
    //        tf = new TermFilter(new Term("all", "xxx"));
    //        bits = (FixedBitSet) tf.getDocIdSet(reader.getContext(), reader.getLiveDocs());
    //        assertThat(bits.cardinality(), equalTo(100));

            //test redis utils

            RedisFilter tf = new RedisFilter(new Term("all", "xxx"));
            FixedBitSet bits = (FixedBitSet) tf.getDocIdSet(reader.getContext(), reader.getLiveDocs());
            System.out.println(bits.cardinality());
    //        assertThat(bits.cardinality(), equalTo(1));

            reader.close();
            rd.close();
        }

    }
}
