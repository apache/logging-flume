package org.apache.flume.CustomInterceptor;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * 使用说明：
 * ======================================================
 * # 定义拦截器
 * agent.sources.kafkaSource.interceptors = i0
 * # 设置拦截器类型
 * # gift_record:giftRecord的意思是会把日志中的gift_record替换为giftRecord
 * agent.sources.kafkaSource.interceptors.i0.type = yehua.MySearchAndReplaceInterceptor
 * agent.sources.kafkaSource.interceptors.i0.searchReplace = "gift_record:giftRecord,video_info:videoInfo"
 * ======================================================
 */
public class MySearchAndReplaceInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory
            .getLogger(MySearchAndReplaceInterceptor.class);

    /**
     * 需要替换的字符串信息
     * 格式："key:value,key:value"
     */
    private final String search_replace;
    private String[] splits;
    private String[] key_value;
    private String key;
    private String value;
    private HashMap<String, String> hashMap = new HashMap<String, String>();
    private Pattern compile = Pattern.compile("\"type\":\"(\\w+)\"");
    private Matcher matcher;
    private String group;

    private MySearchAndReplaceInterceptor(String search_replace) {
        this.search_replace = search_replace;
    }

    /**
     * 初始化放在，最开始执行一次
     * 把配置的数据初始化到map中，方便后面调用
     */
    public void initialize() {
        try{
            if(StringUtils.isNotBlank(search_replace)){
                splits = search_replace.split(",");
                for (String key_value_pair:splits) {
                    key_value = key_value_pair.split(":");
                    key = key_value[0];
                    value = key_value[1];
                    hashMap.put(key,value);
                }
            }
        }catch (Exception e){
            logger.error("数据格式错误，初始化失败。"+search_replace,e.getCause());
        }

    }
    public void close() {

    }


    /**
     * 具体的处理逻辑
     * @param event
     * @return
     */
    public Event intercept(Event event) {
        try{
            String origBody = new String(event.getBody());
            matcher = compile.matcher(origBody);
            if(matcher.find()){
                group = matcher.group(1);
                if(StringUtils.isNotBlank(group)){
                    String newBody = origBody.replaceAll("\"type\":\""+group+"\"", "\"type\":\""+hashMap.get(group)+"\"");
                    event.setBody(newBody.getBytes());
                }
            }
        }catch (Exception e){
            logger.error("拦截器处理失败！",e.getCause());
        }
        return event;
    }

    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    public static class Builder implements Interceptor.Builder {
        private static final String SEARCH_REPLACE_KEY = "searchReplace";

        private String searchReplace;

        public void configure(Context context) {
            searchReplace = context.getString(SEARCH_REPLACE_KEY);
            Preconditions.checkArgument(!StringUtils.isEmpty(searchReplace),
                    "Must supply a valid search pattern " + SEARCH_REPLACE_KEY +
                            " (may not be empty)");
        }

        public Interceptor build() {
            Preconditions.checkNotNull(searchReplace,
                    "Regular expression searchReplace required");
            return new MySearchAndReplaceInterceptor(searchReplace);
        }

    }
}
