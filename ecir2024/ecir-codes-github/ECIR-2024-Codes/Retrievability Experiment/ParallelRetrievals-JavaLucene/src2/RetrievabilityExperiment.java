import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarStyle;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Paths;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.management.Query;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;


public class RetrievabilityExperiment {
    private static final String FIELDNAME = "CONTENT";
    private static final String indexPath = "../../../Wikipedia-pages/index-enwiki";
//    private static final List<String> querySetFiles = Collections.singletonList("../../Query-Sets/bashir-all-queries-0.json");
    private static final List<Integer> cList = Arrays.asList(10, 20, 30, 50, 100);

    private static final Logger logger = LoggerFactory.getLogger(RetrievabilityExperiment.class);

    public static void main(String[] args) throws IOException, ParseException {

        if(args.length == 0) {
            System.out.println("Need to provide the path of the query file.");
            System.exit(0);
        }
        
//        List<String> querySetFiles = Collections.singletonList("../../Query-Sets/bashir-all-queries-0.json");
        List<String> querySetFiles = Collections.singletonList("../../Query-Sets/bashir-all-queries-"+args[0]+".json");
        WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer();
        float k1 = 1.2f;
        float b = 0.75f;
        BM25Similarity similarityModel = new BM25Similarity(k1, b);
        String similarityModelName = "bm25";

        Map<String, Counter> allRd = new ConcurrentHashMap<>();
        for (int c : cList) {
            allRd.put("rd_" + similarityModelName + "_" + c, new Counter());
        }

        try (IndexReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
            IndexSearcher searcher = new IndexSearcher(indexReader);
            searcher.setSimilarity(similarityModel);

            ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors() - 1);

            for (String path : querySetFiles) {
                List<String> queries = loadQueriesFromJson(path);

                try (ProgressBar pb = new ProgressBar("Query Ngrams", queries.size(), 1000, System.err, ProgressBarStyle.ASCII, " queries", 1, true, new DecimalFormat("0.0"), ChronoUnit.SECONDS, 0, Duration.ZERO)) {
                    List<List<String>> queryChunks = splitList(queries, forkJoinPool.getParallelism());
                    queries = null;

                    forkJoinPool.submit(() -> {
                        queryChunks.parallelStream().forEach(chunk -> {
                            for (String query : chunk) {
                                try {
                                    retrieve(query, analyzer, searcher, allRd, similarityModelName);
                                    pb.step(); // Update the progress bar
                                } catch (ParseException | IOException e) {
                                    logger.error("Error retrieving query", e);
                                }
                            }
                        });
                    }).get();
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("Error processing query chunks", e);
                }
            }

            forkJoinPool.shutdown();
            try {
                forkJoinPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                logger.error("Error waiting for fork-join pool to terminate", e);
            }

            if (forkJoinPool.isTerminated()) {
//                try (FileWriter writer = new FileWriter("../../for-Bashir-queries/bashir_allrd_counters_0.json")) {
                try (FileWriter writer = new FileWriter("../../for-Bashir-queries/bashir_allrd_counters_"+args[0]+".json")) {
                    Gson gson = new Gson();
                    gson.toJson(allRd, writer);
                    System.out.println("Results saved to a file.");
                } catch (IOException e) {
                    logger.error("Error writing results to file", e);
                }
            }
        }

        System.out.println("\nCompleted!\n");
        // Serialize the 'allRd' object to a file (e.g., using ObjectOutputStream)
    }

    private static <T> List<List<T>> splitList(List<T> list, int chunks) {
        int chunkSize = (list.size() + chunks - 1) / chunks;
        return IntStream.range(0, chunks)
                .mapToObj(i -> list.subList(i * chunkSize, Math.min(chunkSize * (i + 1), list.size())))
                .collect(Collectors.toList());
    }

    private static void retrieve(String query, WhitespaceAnalyzer analyzer, IndexSearcher searcher, Map<String, Counter> allRd, String similarityModelName) throws ParseException, IOException {
        if (query == null || analyzer == null || searcher == null || allRd == null) {
            logger.error("One or more required objects are null");
            return;
        }
        // QueryParser queryParser = new QueryParser(FIELDNAME, analyzer);
        // org.apache.lucene.search.Query luceneQuery = queryParser.parse(QueryParser.escape(query));

        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

        String[] queryTerms = query.split("\\s+");

        for (String term : queryTerms) {
            org.apache.lucene.search.Query termQuery = new TermQuery(new Term(FIELDNAME, term));
            booleanQueryBuilder.add(termQuery, BooleanClause.Occur.MUST);
        }

        org.apache.lucene.search.Query luceneQuery = booleanQueryBuilder.build();

        int cMax = Collections.max(cList);
        TopDocs topDocs = searcher.search(luceneQuery, cMax);

        if (topDocs == null) {
            logger.error("TopDocs is null");
            return;
        }

        for (int c : cList) {
            Counter counter = allRd.get("rd_" + similarityModelName + "_" + c);
            if (counter == null) {
                logger.error("Counter not found for key: rd_" + similarityModelName + "_" + c);
                continue;
            }
            for (ScoreDoc scoreDoc : Arrays.copyOf(topDocs.scoreDocs, Math.min(c, topDocs.scoreDocs.length))) {
                counter.increment(scoreDoc.doc);
            }
        }
    }


    @SuppressWarnings("unchecked")
    private static List<String> loadQueriesFromJson(String filename) {
    List<String> queries = new ArrayList<>();
    try {
        Gson gson = new Gson();
        Type listType = new TypeToken<List<String>>(){}.getType();
        FileReader fileReader = new FileReader(filename);
        queries = gson.fromJson(fileReader, listType);
        fileReader.close();
    } catch (IOException e) {
        logger.error("Error loading queries from JSON file", e);
    }
    return queries;
}


    static class Counter {
        private final ConcurrentHashMap<Integer, Integer> counts = new ConcurrentHashMap<>();

        public void increment(int key) {
            counts.compute(key, (k, v) -> v == null ? 1 : v + 1);
        }

        public int get(int key) {
            return counts.getOrDefault(key, 0);
        }

        public Set<Map.Entry<Integer, Integer>> entrySet() {
            return counts.entrySet();
        }
    }
}

