package com.bird.vector.common;

import com.bird.vector.utils.Separators;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @description：
 * @author： liuxiangqian
 * @date： 2024/10/25
 */
@Slf4j
public class TextTools {
    /**
     * 对于长字符串进行切分机制
     * MAX_EMBEDDING：       最大嵌入字符串长度
     * EMBEDDING_OVERLAP：   最大重叠长度
     */
    private static int MAX_EMBEDDING = 1024;
    private static int EMBEDDING_OVERLAP = 128;

    public static Pair<List<Integer>, List<String>> loadIdsAndTextsForCsv(String csvFilePath) {
        List<String> texts = new ArrayList<>();
        List<Integer> ids = new ArrayList<>();
        int lineCount = 0;
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(csvFilePath)))) {
            String line = null;
            bufferedReader.readLine();
            while ((line = bufferedReader.readLine()) != null) {
                if (StringUtils.isAllBlank(line)) {
                    continue;
                }

                String[] idAndText = line.split(Separators.TAB);
                if (idAndText.length < 2) {
                    continue;
                }

                int id = Integer.parseInt(idAndText[0].trim());

                StringBuilder sb = new StringBuilder();
                for (int i = 1; i < idAndText.length; i++) {
                    line = idAndText[i];
                    sb.append(line).append(Separators.TAB);
                }

                //只去除最后一个多余的tab字符
                line = sb.toString();
                line = line.substring(0, line.length() - 1);

                collectVectorContents(id, ids, texts, line);

                lineCount++;
                if (lineCount % 10000 == 0) {
                    log.info("当前读取到第{}个向量", lineCount);
                }
            }
        } catch (Exception e) {
            log.error("加载数据文件失败，ERROR:{}", e);
        }

        log.info("数据加载完毕... ...");

        return Pair.of(ids, texts);
    }


    public static Pair<List<Integer>, List<String>> loadIdsAndTextsForXLSX(String xlsxFilePath) {
        List<String> texts = new ArrayList<>();
        List<Integer> ids = new ArrayList<>();
        int lineCount = 0;
        try {
            //创建工作簿对象
            XSSFWorkbook xssfWorkbook = new XSSFWorkbook(new FileInputStream(xlsxFilePath));
            //获取工作簿下sheet的个数
            int sheetNum = xssfWorkbook.getNumberOfSheets();
            log.info("该excel文件中总共有：" + sheetNum + "个sheet");
            //遍历工作簿中的所有数据
            for (int i = 0; i < sheetNum; i++) {
                //读取第i个工作表
                log.info("读取第" + (i + 1) + "个sheet");
                XSSFSheet sheet = xssfWorkbook.getSheetAt(i);
                //获取最后一行的num，即总行数。此处从s开始
                int maxRow = sheet.getLastRowNum();
                for (int row = 1; row <= maxRow; row++) {
                    int maxRol = sheet.getRow(row).getLastCellNum();
                    String idStr = sheet.getRow(row).getCell(0) + "";
                    StringBuilder sb = new StringBuilder();
                    for (int rol = 1; rol < maxRol; rol++) {
                        String fieldValue = sheet.getRow(row).getCell(rol) + "";
                        fieldValue = fieldValue.replaceAll("\t", "").trim();
                        sb.append(fieldValue).append(Separators.TAB);
                    }

                    //去除最后一个tab字符
                    sb.deleteCharAt(sb.length() - 1);
                    float idFloat = Float.parseFloat(idStr);
                    int idInt = (int) idFloat;

                    collectVectorContents(idInt, ids, texts, sb.toString());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("数据加载完毕... ...");

        return Pair.of(ids, texts);
    }

    /**
     * 对文档向量化内容进行处理
     * 1. 字段单独作为向量内容  单域召回保障精确
     * 2. 整体内容作为向量内容  整体召回保障精确
     * 3. 每部分向量化的内容要进行长度限制  确保检索的精准
     *
     * @param idInt       文档id
     * @param ids         文档id list
     * @param texts       文档id对应的文档内容段
     * @param fieldValues id所有域的数据
     */
    private static void collectVectorContents(int idInt, List<Integer> ids, List<String> texts, String fieldValues) {
        List<String> contents = cutString(fieldValues, MAX_EMBEDDING, EMBEDDING_OVERLAP);
        contents.forEach(content -> {
            if (!StringUtils.isEmpty(content)) {
                ids.add(idInt);
                texts.add(content);
            }
        });
    }

    private static List<String> cutString(String sentence, int maxlength, int overlapLength) {
        List<String> contents = new ArrayList<>(4);
        if (sentence.length() < maxlength) {
            contents.add(sentence);
        } else {
            int senLen = sentence.length();
            for (int i = 0; i < senLen; ) {
                int start = i;
                int end = start + maxlength;
                if (end > senLen) {
                    end = senLen;
                }

                String content = sentence.substring(start, end);
                contents.add(content);

                if (start + maxlength >= senLen) {
                    break;
                } else {
                    i += maxlength - overlapLength;
                }
            }
        }

        return contents;
    }
}
