package com.bird.config;

import com.bird.vector.EmIndex;
import com.bird.vector.EmPQ;
import com.bird.vector.Embedding;
import com.bird.vector.VectorSearch;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;

/**
 * Created with IntelliJ IDEA.
 *
 * @description：
 * @author： liuxiangqian
 * @date： 2025/1/18
 */
@Configuration
public class MyBeans {
    /**
     * INDEX_DIR：   索引的目录名称
     * PGMODEL_DIR： 量化乘积的目录名称
     */
    private final static String INDEX_DIR = "index";
    private final static String PGMODEL_DIR = "pqmodel";

    @Resource
    private ConfigBean configBean;

    private static int pqSegmentCount = 16;
    private static int clusterCount = 16;
    private static int maxIterCount = 100;
    private static int vectorDimention = 768;

    @Bean
    public VectorSearch achVectorSearch() {
        Embedding embedding = new Embedding(configBean.getEmbeddingModelDir(), false);
        EmPQ pq = new EmPQ(pqSegmentCount, clusterCount, maxIterCount, vectorDimention);

        String pgPath = configBean.getAchDataDir() + PGMODEL_DIR;
        pq.load(pgPath);

        EmIndex emIndex = new EmIndex(pq);
        VectorSearch vectorSearch = new VectorSearch(emIndex, embedding);
        String achIndexPath = configBean.getAchDataDir() + INDEX_DIR;
        vectorSearch.load(achIndexPath);

        return vectorSearch;
    }

    @Bean
    public VectorSearch patVectorSearch() {
        Embedding embedding = new Embedding(configBean.getEmbeddingModelDir(), false);
        EmPQ pq = new EmPQ(pqSegmentCount, clusterCount, maxIterCount, vectorDimention);

        String pgPath = configBean.getPatDataDir() + PGMODEL_DIR;
        pq.load(pgPath);

        EmIndex emIndex = new EmIndex(pq);
        VectorSearch vectorSearch = new VectorSearch(emIndex, embedding);
        String achIndexPath = configBean.getPatDataDir() + INDEX_DIR;
        vectorSearch.load(achIndexPath);

        return vectorSearch;
    }
}
