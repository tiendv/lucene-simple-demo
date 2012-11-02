/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.ConnectionPool;
import constant.IndexConst;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author Dungit86
 */
public class IndexAutocomplete {

    static String delims = "[ .,?!()-]+";
    String Path = "";
    
    public IndexAutocomplete(String path)
    {
        this.Path = path;
    }

    public static void main(String[] args) throws IOException {
        String path = "C:\\";
        int count = index(path);
        System.out.println(count);
    }
    public String _run() {
        String out = "";
        try {
            long start = new Date().getTime();
            int count = this.index(Path);
            long end = new Date().getTime();
            out = "Index : " + count + " files : Time index :" + (end - start) + " milisecond";
        } catch (Exception ex) {
            out = ex.getMessage();
        }
        return out;
    }
    /**
     *          
     * @param path: đường dẫn tới folder
     * @return số doc được index
     * @Summary: lấy tất cả các từ khóa các trường: authorName, conferenceName, journalName, orgName, keyword.
     * phân tích thành các token sau đó index vào theo thứ tự của các token đó
     */
    public static int index(String path) throws IOException {
        int count = 0;

        String AuthorDir = path + IndexConst.AUTHOR_INDEX_PATH;
        String KeywordDir = path + IndexConst.KEYWORD_INDEX_PATH;
        String ConfDir = path + IndexConst.CONFERENCE_INDEX_PATH;
        String JourDir = path + IndexConst.JOURNAL_INDEX_PATH;
        String OrgDir = path + IndexConst.ORG_INDEX_PATH;

        String Dictionary = path + IndexConst.AUTOCOMPLETE_DIRECTORY_PATH;


        String[] st = null;

        Directory Directory = new SimpleFSDirectory(new File(KeywordDir));
        IndexReader indexReader = IndexReader.open(Directory);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        BooleanQuery blQuery = new BooleanQuery();
        Query query = new MatchAllDocsQuery();
        blQuery.add(query, BooleanClause.Occur.SHOULD);

        //Index keyword
        File indexDir = new File(Dictionary);
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36,Collections.EMPTY_SET);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
        Directory directory = FSDirectory.open(indexDir);
        IndexWriter writer = new IndexWriter(directory, config);

        TopDocs hits = indexSearcher.search(blQuery, null, Integer.MAX_VALUE);
        for (int i = 0; i < hits.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = hits.scoreDocs[i];
            Document doc = indexSearcher.doc(scoreDoc.doc);
            count++;

            System.out.println(IndexConst.AUTOCOMPLETE_OBJECT_VAL_KEY + " " + count + " " + doc.get(IndexConst.KEYWORD_KEYWORD_FIELD));

            st = doc.get(IndexConst.KEYWORD_KEYWORD_FIELD).split(delims);

            Document d = new Document();
            d.add(new Field(IndexConst.AUTOCOMPLETE_KEYWORD_FIELD, doc.get(IndexConst.KEYWORD_KEYWORD_FIELD), Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.AUTOCOMPLETE_IDOBJECT_FIELD, doc.get(IndexConst.KEYWORD_IDKEYWORD_FIELD), Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.AUTOCOMPLETE_OBJECT_FIELD, IndexConst.AUTOCOMPLETE_OBJECT_VAL_KEY, Field.Store.YES, Field.Index.ANALYZED));
            int NumberTerm = 1;
            for (String term : st) {
                d.add(new Field(IndexConst.AUTOCOMPLETE_TERM_FIELD + Integer.toString(NumberTerm), term, Field.Store.YES, Field.Index.ANALYZED));
                NumberTerm++;
            }
            writer.addDocument(d);
            d = null;
        }

        //index Author
        Directory = new SimpleFSDirectory(new File(AuthorDir));
        indexReader = IndexReader.open(Directory);
        indexSearcher = new IndexSearcher(indexReader);

        blQuery = new BooleanQuery();
        query = new MatchAllDocsQuery();
        blQuery.add(query, BooleanClause.Occur.SHOULD);

        hits = indexSearcher.search(blQuery, null, Integer.MAX_VALUE);
        for (int i = 0; i < hits.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = hits.scoreDocs[i];
            Document doc = indexSearcher.doc(scoreDoc.doc);
            count++;

            System.out.println(IndexConst.AUTOCOMPLETE_OBJECT_VAL_AUTHOR + " " + count + " " + doc.get(IndexConst.AUTHOR_AUTHORNAME_FIELD));

            st = doc.get(IndexConst.AUTHOR_AUTHORNAME_FIELD).split(delims);

            Document d = new Document();
            d.add(new Field(IndexConst.AUTOCOMPLETE_KEYWORD_FIELD, doc.get(IndexConst.AUTHOR_AUTHORNAME_FIELD), Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.AUTOCOMPLETE_IDOBJECT_FIELD, doc.get(IndexConst.AUTHOR_IDAUTHOR_FIELD), Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.AUTOCOMPLETE_OBJECT_FIELD, IndexConst.AUTOCOMPLETE_OBJECT_VAL_AUTHOR, Field.Store.YES, Field.Index.ANALYZED));
            int NumberTerm = 1;
            for (String term : st) {
                d.add(new Field(IndexConst.AUTOCOMPLETE_TERM_FIELD + Integer.toString(NumberTerm), term, Field.Store.YES, Field.Index.ANALYZED));
                NumberTerm++;
            }
            writer.addDocument(d);
            d = null;
        }

        //index Conference
        Directory = new SimpleFSDirectory(new File(ConfDir));
        indexReader = IndexReader.open(Directory);
        indexSearcher = new IndexSearcher(indexReader);

        blQuery = new BooleanQuery();
        query = new MatchAllDocsQuery();
        blQuery.add(query, BooleanClause.Occur.SHOULD);

        hits = indexSearcher.search(blQuery, null, Integer.MAX_VALUE);
        for (int i = 0; i < hits.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = hits.scoreDocs[i];
            Document doc = indexSearcher.doc(scoreDoc.doc);
            count++;

            System.out.println(IndexConst.AUTOCOMPLETE_OBJECT_VAL_CONF + " " + count + " " + doc.get(IndexConst.CONFERENCE_CONFERENCENAME_FIELD));

            st = doc.get(IndexConst.CONFERENCE_CONFERENCENAME_FIELD).split(delims);

            Document d = new Document();
            d.add(new Field(IndexConst.AUTOCOMPLETE_KEYWORD_FIELD, doc.get(IndexConst.CONFERENCE_CONFERENCENAME_FIELD), Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.AUTOCOMPLETE_IDOBJECT_FIELD, doc.get(IndexConst.CONFERENCE_IDCONFERENCE_FIELD), Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.AUTOCOMPLETE_OBJECT_FIELD, IndexConst.AUTOCOMPLETE_OBJECT_VAL_CONF, Field.Store.YES, Field.Index.ANALYZED));
            int NumberTerm = 1;
            for (String term : st) {
                d.add(new Field(IndexConst.AUTOCOMPLETE_TERM_FIELD + Integer.toString(NumberTerm), term, Field.Store.YES, Field.Index.ANALYZED));
                NumberTerm++;
            }
            writer.addDocument(d);
            d = null;
        }

        //index Journal
        Directory = new SimpleFSDirectory(new File(JourDir));
        indexReader = IndexReader.open(Directory);
        indexSearcher = new IndexSearcher(indexReader);

        blQuery = new BooleanQuery();
        query = new MatchAllDocsQuery();
        blQuery.add(query, BooleanClause.Occur.SHOULD);

        hits = indexSearcher.search(blQuery, null, Integer.MAX_VALUE);
        for (int i = 0; i < hits.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = hits.scoreDocs[i];
            Document doc = indexSearcher.doc(scoreDoc.doc);
            count++;

            System.out.println(IndexConst.AUTOCOMPLETE_OBJECT_VAL_JOUR + " " + count + " " + doc.get(IndexConst.JOURNAL_JOURNALNAME_FIELD));

            st = doc.get(IndexConst.JOURNAL_JOURNALNAME_FIELD).split(delims);

            Document d = new Document();
            d.add(new Field(IndexConst.AUTOCOMPLETE_KEYWORD_FIELD, doc.get(IndexConst.JOURNAL_JOURNALNAME_FIELD), Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.AUTOCOMPLETE_IDOBJECT_FIELD, doc.get(IndexConst.JOURNAL_IDJOURNAL_FIELD), Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.AUTOCOMPLETE_OBJECT_FIELD, IndexConst.AUTOCOMPLETE_OBJECT_VAL_JOUR, Field.Store.YES, Field.Index.ANALYZED));
            int NumberTerm = 1;
            for (String term : st) {
                d.add(new Field(IndexConst.AUTOCOMPLETE_TERM_FIELD + Integer.toString(NumberTerm), term, Field.Store.YES, Field.Index.ANALYZED));
                NumberTerm++;
            }
            writer.addDocument(d);
            d = null;
        }

        //index Org
        Directory = new SimpleFSDirectory(new File(OrgDir));
        indexReader = IndexReader.open(Directory);
        indexSearcher = new IndexSearcher(indexReader);

        blQuery = new BooleanQuery();
        query = new MatchAllDocsQuery();
        blQuery.add(query, BooleanClause.Occur.SHOULD);

        hits = indexSearcher.search(blQuery, null, Integer.MAX_VALUE);
        for (int i = 0; i < hits.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = hits.scoreDocs[i];
            Document doc = indexSearcher.doc(scoreDoc.doc);
            count++;

            System.out.println(IndexConst.AUTOCOMPLETE_OBJECT_VAL_ORG + " " + count + " " + doc.get(IndexConst.ORG_ORGNAME_FIELD));

            st = doc.get(IndexConst.ORG_ORGNAME_FIELD).split(delims);

            Document d = new Document();
            d.add(new Field(IndexConst.AUTOCOMPLETE_KEYWORD_FIELD, doc.get(IndexConst.ORG_ORGNAME_FIELD), Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.AUTOCOMPLETE_IDOBJECT_FIELD, doc.get(IndexConst.ORG_IDORG_FIELD), Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.AUTOCOMPLETE_OBJECT_FIELD, IndexConst.AUTOCOMPLETE_OBJECT_VAL_ORG, Field.Store.YES, Field.Index.ANALYZED));
            int NumberTerm = 1;
            for (String term : st) {
                d.add(new Field(IndexConst.AUTOCOMPLETE_TERM_FIELD + Integer.toString(NumberTerm), term, Field.Store.YES, Field.Index.ANALYZED));
                NumberTerm++;
            }
            writer.addDocument(d);
            d = null;
        }

        writer.close();

        return count;
    }
}
