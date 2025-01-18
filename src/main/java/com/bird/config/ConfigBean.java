/**
 * ConfigBean
 *
 * @author liuxiangqian
 * @date 2019/5/6
 */
package com.bird.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "myconf")
@Component
@Data
public class ConfigBean {
    /**
     * embeddingModelDir:   嵌入向量模型目录
     * achDataDir:          成果数据目录
     * achFieldNames:       成果字段
     * patDataDir:          专利数据目录
     * patFieldNames:       专利字段
     */
    private String embeddingModelDir;
    private String achDataDir;
    private String achFieldNames;

    private String patDataDir;
    private String patFieldNames;
}
