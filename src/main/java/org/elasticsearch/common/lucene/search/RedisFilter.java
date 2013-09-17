/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.docset.AllDocIdSet;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;

/**
 * A simple utils for a specific term.
 */
public class RedisFilter extends Filter {

    private static final ESLogger logger = Loggers.getLogger(RedisFilter.class);

    private final Term term;

    public RedisFilter(Term term) {
        this.term = term;
    }

    public Term getTerm() {
        return term;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        logger.info("redis key:"+term.field()+",redis value:"+term.text());

        //TODO 判断redis里面是否存在该权限Key，如果不存在，说明查询的时候，参数有误，代码里直接返回和报错

        final AllDocIdSet result = new AllDocIdSet(context.reader().maxDoc());
        DocIdSetIterator docsEnum = result.iterator();
        int docId = docsEnum.nextDoc();
        if (docId == DocsEnum.NO_MORE_DOCS) {
            return null;
        }

        final FixedBitSet finalResult = new FixedBitSet(context.reader().maxDoc());
        for (; docId < DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
            Document doc=context.reader().document(docId);
            String uid= doc.getField("_uid").stringValue();
            Uid id = Uid.createUid(uid);
            logger.info("type:"+id.type()+",uid:"+id.id());

            boolean hitPermission=false;

            //Notice 此处判断文档是否满足权限
            if(id.id().equals("1")){
                hitPermission=false;
                logger.info("没有权限");
            }else{
                logger.info("权限正常");
                hitPermission=true;
            }


            if(hitPermission)
            {
                finalResult.set(docId);
            }
        }
        return finalResult;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RedisFilter that = (RedisFilter) o;

        if (term != null ? !term.equals(that.term) : that.term != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return term != null ? term.hashCode() : 0;
    }

    @Override
    public String toString() {
        return term.field() + ":" + term.text();
    }
}
