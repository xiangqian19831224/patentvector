package com.bird.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bird.config.ConfigBean;
import com.bird.vector.VectorSearch;
import com.bird.vector.utils.Separators;
import com.github.xiaoymin.knife4j.annotations.ApiOperationSupport;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @description：
 * @author： liuxiangqian
 * @date： 2025/1/18
 */
@RestController
@RequestMapping("/ict")
@Slf4j
public class SearchController {
    @Resource
    private ConfigBean configBean;

    @Autowired
    @Qualifier("achVectorSearch")
    private VectorSearch achVectorSearch;

//    @Autowired
//    @Qualifier("patVectorSearch")
//    private VectorSearch patVectorSearch;

    @ApiOperationSupport(order = 1)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "requestId", value = "请求id", defaultValue = "12345678"),
            @ApiImplicitParam(name = "userId", value = "用户标识", defaultValue = "12345678"),
            @ApiImplicitParam(name = "query", value = "搜索词汇", required = true),
            @ApiImplicitParam(name = "clusterTopn", value = "聚类数", required = false),
            @ApiImplicitParam(name = "topn", value = "搜索结果数", required = true),
    })
    @ApiOperation(value = "全量搜索")
    @RequestMapping(value = "/achievement/search", method = RequestMethod.GET)
    public JSONObject achsearch(
            @RequestParam(value = "requestId", defaultValue = "111", required = false) String requestId,
            @RequestParam(value = "userId", defaultValue = "222", required = false) int userId,
            @RequestParam(value = "query", defaultValue = "", required = true) String query,
            @RequestParam(value = "clusterTopn", defaultValue = "3", required = false) int clusterTopn,
            @RequestParam(value = "topn", defaultValue = "10", required = true) int topn
    ) {
        JSONObject jsonObject = new JSONObject(true);

        long start = System.currentTimeMillis();
        List<Pair<Integer, Pair<Float, List<String>>>> result = achVectorSearch.searchText(query, clusterTopn, topn);
        long end = System.currentTimeMillis();
        long took = end - start;

        JSONArray jsonArray = new JSONArray();
        result.forEach(doc -> {
            int id = doc.getKey();
            Pair<Float, List<String>> simTexts = doc.getValue();
            float sim = simTexts.getKey();
            //该场景每个文档只有一个
            String text = simTexts.getValue().get(0);

            JSONObject docObj = new JSONObject(true);
            docObj.put("id", id);
            docObj.put("similary", sim);
            parseAchFields(text, docObj);

            jsonArray.add(docObj);
        });

        jsonObject.put("query", query);
        jsonObject.put("took", took);
        jsonObject.put("count", jsonArray.size());
        jsonObject.put("patents", jsonArray);

        return jsonObject;
    }

//
//    @ApiOperationSupport(order = 2)
//    @ApiImplicitParams({
//            @ApiImplicitParam(name = "requestId", value = "请求id", defaultValue = "12345678"),
//            @ApiImplicitParam(name = "userId", value = "用户标识", defaultValue = "12345678"),
//            @ApiImplicitParam(name = "query", value = "搜索词汇", required = true),
//            @ApiImplicitParam(name = "clusterTopn", value = "聚类数", required = false),
//            @ApiImplicitParam(name = "topn", value = "搜索结果数", required = true),
//    })
//    @ApiOperation(value = "全量搜索")
//    @RequestMapping(value = "/patent/search", method = RequestMethod.GET)
//    public JSONObject patsearch(
//            @RequestParam(value = "requestId", defaultValue = "111", required = false) String requestId,
//            @RequestParam(value = "userId", defaultValue = "222", required = false) int userId,
//            @RequestParam(value = "query", defaultValue = "", required = true) String query,
//            @RequestParam(value = "clusterTopn", defaultValue = "3", required = false) int clusterTopn,
//            @RequestParam(value = "topn", defaultValue = "10", required = true) int topn
//    ) {
//        JSONObject jsonObject = new JSONObject(true);
//
//        long start = System.currentTimeMillis();
//        List<Pair<Integer, Pair<Float, List<String>>>> result = patVectorSearch.searchText(query, clusterTopn, topn);
//        long end = System.currentTimeMillis();
//        long took = end - start;
//
//        JSONArray jsonArray = new JSONArray();
//        result.forEach(doc -> {
//            int id = doc.getKey();
//            Pair<Float, List<String>> simTexts = doc.getValue();
//            float sim = simTexts.getKey();
//            //该场景每个文档只有一个
//            String text = simTexts.getValue().get(0);
//
//            JSONObject docObj = new JSONObject(true);
//            docObj.put("id", id);
//            docObj.put("similary", sim);
//            parsePatFields(text, docObj);
//
//            jsonArray.add(docObj);
//        });
//
//        jsonObject.put("query", query);
//        jsonObject.put("took", took);
//        jsonObject.put("count", jsonArray.size());
//        jsonObject.put("patents", jsonArray);
//
//        return jsonObject;
//    }

    private void parseAchFields(String text, JSONObject docObj) {
        String[] fieldNames = configBean.getAchFieldNames().split(Separators.COMMA);
        String[] fieldValues = text.split(Separators.TAB);
        if (fieldNames.length != fieldValues.length) {
            log.info("字段数目不匹配  文档资料最后n栏都是空的话，会出现该情况");
        }

        int len = fieldValues.length;
        for (int i = 0; i < len; i++) {
            String fName = fieldNames[i];
            String fValue = fieldValues[i];

            docObj.put(fName, fValue);
        }
    }

    private void parsePatFields(String text, JSONObject docObj) {
        String[] fieldNames = configBean.getPatFieldNames().split(Separators.COMMA);
        String[] fieldValues = text.split(Separators.TAB);
        if (fieldNames.length != fieldValues.length) {
            log.info("字段数目不匹配  文档资料最后n栏都是空的话，会出现该情况");
        }

        int len = fieldValues.length;
        for (int i = 0; i < len; i++) {
            String fName = fieldNames[i];
            String fValue = fieldValues[i];

            docObj.put(fName, fValue);
        }
    }
}

