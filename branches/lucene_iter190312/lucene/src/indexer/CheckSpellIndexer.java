/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.IndexConst;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
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
public class CheckSpellIndexer {

    static String delims = "[ ,?!()-.]+";
    String Path = "";

    public CheckSpellIndexer(String path) {
        this.Path = path;
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

    public static class AnalysisResult {

        public String input;
        public List<String> gram3s = new ArrayList<String>();
        public List<String> gram4s = new ArrayList<String>();
        public String start3 = "";
        public String start4 = "";
        public String end3 = "";
        public String end4 = "";
    }

    public static class NGramAnalyzer extends Analyzer {

        private int minGram;
        private int maxGram;

        public NGramAnalyzer(int minGram, int maxGram) {
            this.minGram = minGram;
            this.maxGram = maxGram;
        }

        @Override
        public TokenStream tokenStream(String fieldName, Reader reader) {
            return new NGramTokenizer(reader, minGram, maxGram);
        }
    }

    public static int index(String path) throws IOException {
        int count = 0;

        String AuthorDir = path + IndexConst.AUTHOR_INDEX_PATH;
        String KeywordDir = path + IndexConst.KEYWORD_INDEX_PATH;

        String Dictionary = path + IndexConst.CHECKSPELL_DIRECTORY_PATH;


        String[] st = null;

        Directory Directory = new SimpleFSDirectory(new File(KeywordDir));
        IndexReader indexReader = IndexReader.open(Directory);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        BooleanQuery blQuery = new BooleanQuery();
        Query query = new MatchAllDocsQuery();
        blQuery.add(query, BooleanClause.Occur.SHOULD);

        //Index keyword
        File indexDir = new File(Dictionary);
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
        Directory directory = FSDirectory.open(indexDir);
        IndexWriter writer = new IndexWriter(directory, config);

        TopDocs hits = indexSearcher.search(blQuery, null, Integer.MAX_VALUE);
        for (int i = 0; i < hits.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = hits.scoreDocs[i];
            Document doc = indexSearcher.doc(scoreDoc.doc);
            count++;

            System.out.println(IndexConst.CHECKSPELL_OBJECT_VAL_KEY + " " + count + " " + doc.get(IndexConst.KEYWORD_KEYWORD_FIELD));

            String keyword = doc.get(IndexConst.KEYWORD_KEYWORD_FIELD);
            Document d = new Document();
            d.add(new Field(IndexConst.CHECKSPELL_KEYWORD_FIELD, keyword, Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.CHECKSPELL_OBJECT_FIELD, IndexConst.CHECKSPELL_OBJECT_VAL_KEY, Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.CHECKSPELL_IDOBJECT_FIELD, doc.get(IndexConst.KEYWORD_IDKEYWORD_FIELD), Field.Store.YES, Field.Index.ANALYZED));

            String dictionaryEntry = keyword.toLowerCase();
            dictionaryEntry = dictionaryEntry.replaceAll(delims, " ");
            d.add(new Field(IndexConst.CHECKSPELL_KEYWORD_COMPARE_FIELD, dictionaryEntry, Field.Store.YES, Field.Index.ANALYZED));

            String End = dictionaryEntry.replace(" ", "0");
            AnalysisResult result = analyze(End);

            for (String gram3 : result.gram3s) {
                d.add(new Field(IndexConst.CHECKSPELL_3GRAM_FIELD, gram3, Field.Store.YES, Field.Index.ANALYZED));
            }
            for (String gram4 : result.gram4s) {
                d.add(new Field(IndexConst.CHECKSPELL_4GRAM_FIELD, gram4, Field.Store.YES, Field.Index.ANALYZED));
            }
            d.add(new Field(IndexConst.CHECKSPELL_3GRAM_START_FIELD, result.start3, Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.CHECKSPELL_4GRAM_START_FIELD, result.start4, Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.CHECKSPELL_3GRAM_END_FIELD, result.end3, Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.CHECKSPELL_4GRAM_END_FIELD, result.end4, Field.Store.YES, Field.Index.ANALYZED));

            writer.addDocument(d);
            d = null;
        }

        //index authorName
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

            System.out.println(IndexConst.CHECKSPELL_OBJECT_VAL_AUTHOR + " " + count + " " + doc.get(IndexConst.AUTHOR_AUTHORNAME_FIELD));

            String keyword = doc.get(IndexConst.AUTHOR_AUTHORNAME_FIELD);
            Document d = new Document();
            d.add(new Field(IndexConst.CHECKSPELL_KEYWORD_FIELD, keyword, Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.CHECKSPELL_OBJECT_FIELD, IndexConst.CHECKSPELL_OBJECT_VAL_AUTHOR, Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.CHECKSPELL_IDOBJECT_FIELD, doc.get(IndexConst.AUTHOR_IDAUTHOR_FIELD), Field.Store.YES, Field.Index.ANALYZED));

            String dictionaryEntry = keyword.toLowerCase();
            dictionaryEntry = dictionaryEntry.replaceAll(delims, " ");
            d.add(new Field(IndexConst.CHECKSPELL_KEYWORD_COMPARE_FIELD, dictionaryEntry, Field.Store.YES, Field.Index.ANALYZED));

            String End = dictionaryEntry.replace(" ", "0");
            AnalysisResult result = analyze(End);

            for (String gram3 : result.gram3s) {
                d.add(new Field(IndexConst.CHECKSPELL_3GRAM_FIELD, gram3, Field.Store.YES, Field.Index.ANALYZED));
            }
            for (String gram4 : result.gram4s) {
                d.add(new Field(IndexConst.CHECKSPELL_4GRAM_FIELD, gram4, Field.Store.YES, Field.Index.ANALYZED));
            }
            d.add(new Field(IndexConst.CHECKSPELL_3GRAM_START_FIELD, result.start3, Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.CHECKSPELL_4GRAM_START_FIELD, result.start4, Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.CHECKSPELL_3GRAM_END_FIELD, result.end3, Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.CHECKSPELL_4GRAM_END_FIELD, result.end4, Field.Store.YES, Field.Index.ANALYZED));
            writer.addDocument(d);
            d = null;
        }
        writer.optimize();
        writer.close();
        return count;
    }

    public static AnalysisResult analyze(String input) throws IOException {
        AnalysisResult result = new AnalysisResult();
        result.input = input;
        Analyzer analyzer = new NGramAnalyzer(3, 4);
        TokenStream tokenStream = analyzer.tokenStream("dummy", new StringReader(input));
        CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);

        while (tokenStream.incrementToken()) {
            String text = charTermAttribute.toString();
            if (text.length() == 3) {
                result.gram3s.add(text);
            } else if (text.length() == 4) {
                result.gram4s.add(text);
            } else {
                continue;
            }
        }
        result.start3 = input.substring(0, Math.min(input.length(), 3));
        result.start4 = input.substring(0, Math.min(input.length(), 4));
        result.end3 = input.substring(Math.max(0, input.length() - 3), input.length());
        result.end4 = input.substring(Math.max(0, input.length() - 4), input.length());
        return result;
    }

    public static void main(String[] args) throws IOException {
        String path = "C:\\";
        int count = index(path);
        System.out.println(count);
    }
}
