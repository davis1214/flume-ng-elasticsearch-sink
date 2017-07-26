package org.apache.flume.sink.elasticsearch;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.joda.time.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.*;
//import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_CATEGORY_FIELDS;
//import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_CATEGORY_REGEX;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Serialize flume events into the same format LogStash uses</p>
 * <p/>
 * This can be used to send events to ElasticSearch and use clients such as
 * Kabana which expect Logstash formated indexes
 * <p/>
 * <pre>
 * {
 *    "@timestamp": "2010-12-21T21:48:33.309258Z",
 *    "@tags": [ "array", "of", "tags" ],
 *    "@type": "string",
 *    "@source": "source of the event, usually a URL."
 *    "@source_host": ""
 *    "@source_path": ""
 *    "@fields":{
 *       # a set of fields for this event
 *       "user": "jordan",
 *       "command": "shutdown -r":
 *     }
 *     "@message": "the original plain-text message"
 *   }
 * </pre>
 * <p/>
 * If the following headers are present, they will map to the above logstash
 * output as long as the logstash fields are not already present.</p>
 * <p/>
 * <pre>
 *  timestamp: long -> @timestamp:Date
 *  host: String -> @source_host: String
 *  src_path: String -> @source_path: String
 *  type: String -> @type: String
 *  source: String -> @source: String
 * </pre>
 */
public class ElasticSearchLogStashRegexEventSerializer implements
        ElasticSearchEventSerializer {

    private static String categories = null;
    private static final Map<String, String[]> categoryMap = Maps.newHashMap();
    private static final Map<String, Pattern> cache = Maps.newHashMap();
    private Map<String, Map<String, String>> converMaps = Maps.newHashMap();
    private static final Logger logger = LoggerFactory
            .getLogger(ElasticSearchLogStashRegexEventSerializer.class);


    private String kvField = null;
    @Override
    public XContentBuilder getContentBuilder(Event event) throws IOException {
        //TODO 查看下XContentBuilder的生成方式, Xgenerator
        XContentBuilder builder = jsonBuilder().startObject();
        Date date = appendBody(builder, event);
        appendHeaders(builder, event, date);
        builder.endObject();
        return builder;
    }

    private Date appendBody(XContentBuilder builder, Event event)
            throws IOException {
        byte[] body = event.getBody();
        Map<String, String> headers = event.getHeaders();
        String category = headers.get("category");
        if (category == null || category.isEmpty()) {
            category = headers.get("topic");
        }

        String[] regFields = categoryMap.get(category);

        if (regFields == null) {
            category = DEFAULT_CATEGORY_NAME;
            regFields = categoryMap.get(DEFAULT_CATEGORY_NAME);
        }

        if (regFields == null) {
            builder.field("@message", new String(body, charset));
            return null;
        }


        String regex = regFields[0];
        String fields = regFields[1];
        String split = regFields[2];
        String timeField = regFields[3];
        String timeFormat = regFields[4];
        String kvField = regFields[5];
        String kvFieldSplit = regFields[6];
        String kvSplit = regFields[7];
        String kvMinNum = regFields[8];
        boolean storeOrgLog = regFields[9].equals("false") ? false : true;

        if(storeOrgLog){
            builder.field("@message", new String(body, charset));
        }

        SimpleDateFormat df = null;
        if (timeFormat != null && timeFormat.contains("MMM")) {
            df = new SimpleDateFormat(timeFormat, Locale.ENGLISH);
        } else if (timeFormat != null) {
            df = new SimpleDateFormat(timeFormat);

        }

        Iterable<String> fieldIte = Splitter.on(',').omitEmptyStrings().split(fields);
        String raw = new String(body, charset);

        Map<String, String> converMap = converMaps.get(category);

        Date date = null;
        try {

            int kvNum = kvMinNum != null ? Integer.valueOf(kvMinNum) : 1;
            List<String> vals = parseRegex(raw, regex, split);

            if(vals.size() == 0 && !storeOrgLog) {
                if(!storeOrgLog) {
                    builder.field("@message", new String(body, charset));
                }
                return date;
            }


            /**分隔所有字段存入map*/
            int i = 0;
            Map<String, String> allFields = Maps.newHashMap();
            String extra = "";
            for (String key : fieldIte) {
                String value = i >= vals.size() ? "" : vals.get(i++);
                if(key.equals(kvField) && !value.isEmpty()){    //该字段需按key：value方式解析
                    String[] kvs = StringUtils.split(value, kvFieldSplit);
                    if(kvs.length < kvNum){                     //如果分割出的key:value对数小于kvNum，则认为此kvField字符串无效，不再继续切分key value;
                        allFields.put(key, value);
                        continue;
                    }

                    for(String kvStr : kvs){
                        String[] kv = StringUtils.split(kvStr, kvSplit, 2);
                        if(kv.length == 2 && checkFieldNameIsValid(kv[0])){
                            allFields.put(kv[0], kv[1]);
                        }else{
                            extra += kvStr;
                        }
                    }
                }else {
                    allFields.put(key, value);
                }
            }
            if(!extra.isEmpty()){
                String existExtraInfo = allFields.get("extraInfo");
                existExtraInfo = existExtraInfo == null ? "": existExtraInfo;
                allFields.put("extraInfo", existExtraInfo + extra);
            }



            /** 调整map字段格式 */
            for (Map.Entry<String, String> entry : allFields.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                if (key.equals(timeField) && !value.isEmpty() && df != null) {
                    try {
                        date = df.parse(value);
                    } catch (Exception e) {
                        logger.error("parseError:" + raw);
                        if(!storeOrgLog) {
                            builder.field("@message", new String(body, charset));
                        }
                        break;
                    }
                    builder.field(key, value);
                    continue;
                }


                if(converMap != null){
                    String type = converMap.get(key);
                    try {
                        if (type == null) {
                            builder.field(key, value);
                            continue;
                        }
                        type = type.toLowerCase();
                        if (type.equals("int") || type.equals("integer")) {
                            builder.field(key, new Integer(value));
                        }else if (type.equals("long")) {
                            builder.field(key, new Long(value));
                        }else if (type.equals("float")) {
                            builder.field(key, new Float(value));
                        }else if (type.equals("double")) {
                            builder.field(key, new Double(value));
                        }
                    }catch (Exception e){
                        logger.error("parseError category:" + category + " key:" + key + " value" + value + " errorMessage:" + raw);
                        if(!storeOrgLog) {
                            builder.field("@message", new String(body, charset));
                        }
                        break;
                    }
                }else {
                    builder.field(key, value);
                    //ContentBuilderUtil.appendField(builder, key, value.getBytes(charset));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            if(!storeOrgLog) {
                builder.field("@message", new String(body, charset));
            }
            logger.error("log:"+raw+ " errorMessage:"+e.getMessage());
        }

        return date;

    }

    /**
     * 检查字段名称是否有效，首字母必须为26英文字母，不包含逗号；
     * @param fieldName
     * @return
     */
    private boolean checkFieldNameIsValid(String fieldName) {
        char firstChar = fieldName.charAt(0);
        int asic = (int)firstChar;
        if (asic < 65   || (asic > 90 && asic < 97) || asic > 122) {
            return  false;
        }
        if(fieldName.contains(",") || fieldName.contains(">") ||fieldName.contains("<")){
            return false;
        }
        return  true;

    }

    private List<String> parseRegex(String line, String regex, String split) {

        List<String> vals = new ArrayList<String>();


        if (line == null || regex == null || line.trim().length() < 1) {
            return vals;
        }

        if (split != null) {
            for (String str : line.split(split, -1)) {
                vals.add(str);
            }
            return vals;
        }


        Pattern p = cache.get(regex);
        if (p == null) {
            p = Pattern.compile(regex);
            cache.put(regex, p);
        }
        Matcher m = p.matcher(line);
        if (m.matches()) {
            int count = m.groupCount();
            for (int i = 1; i <= count; i++) {
                vals.add(m.group(i));
            }
        }
        return vals;
    }

    private void appendHeaders(XContentBuilder builder, Event event) throws IOException {
        appendHeaders(builder, event, null);
    }

    private void appendHeaders(XContentBuilder builder, Event event, Date date)
            throws IOException {
        Map<String, String> headers = Maps.newHashMap(event.getHeaders());

        if (date != null) {
            builder.field("@timestamp", date);
        } else {
            String timestamp = headers.get("timestamp");
            if (!StringUtils.isBlank(timestamp)
                    && StringUtils.isBlank(headers.get("@timestamp"))) {
                long timestampMs = Long.parseLong(timestamp);
                builder.field("@timestamp", new Date(timestampMs));
            }else{
                builder.field("@timestamp", new Date());
            }
        }

        String source = headers.get("source");
        if (!StringUtils.isBlank(source)
                && StringUtils.isBlank(headers.get("@source"))) {
            ContentBuilderUtil.appendField(builder, "@source",
                    source.getBytes(charset));
        }

        String type = headers.get("type");
        if (!StringUtils.isBlank(type)
                && StringUtils.isBlank(headers.get("@type"))) {
            ContentBuilderUtil.appendField(builder, "@type", type.getBytes(charset));
        }

        String host = headers.get("host");
        if (!StringUtils.isBlank(host)
                && StringUtils.isBlank(headers.get("@source_host"))) {
            ContentBuilderUtil.appendField(builder, "@source_host",
                    host.getBytes(charset));
        }

        String srcPath = headers.get("src_path");
        if (!StringUtils.isBlank(srcPath)
                && StringUtils.isBlank(headers.get("@source_path"))) {
            ContentBuilderUtil.appendField(builder, "@source_path",
                    srcPath.getBytes(charset));
        }

        builder.startObject("@fields");
        for (String key : headers.keySet()) {
            byte[] val = headers.get(key).getBytes(charset);
            ContentBuilderUtil.appendField(builder, key, val);
        }
        builder.endObject();
    }

    @Override
    public void configure(Context context) {
        //categoryMap.put(DEFAULT_CATEGORY_NAME, new String[]{DEFAULT_CATEGORY_REGEX, DEFAULT_CATEGORY_FIELDS, null});

        this.categories = context.getString(CATEGORIES);
        logger.info("**** categories:" + this.categories);
        if (!StringUtils.isNotBlank(this.categories)) {
            return;
        }

        Iterable<String> categoryArr = Splitter.on(' ').omitEmptyStrings().split(categories);
        for (String category : categoryArr) {
            String regex = context.getString(category + ".regex", null);
            String fields = context.getString(category + ".fields", null);
            String split = context.getString(category + ".split" , null);
            String timeField = context.getString(category + ".timeField", null);
            String timeFormat = context.getString(category + ".timeFormat", null);
            String converFields = context.getString(category + ".converFields", null);
            String converTypes = context.getString(category + ".converTypes", null);
            String kvField = context.getString(category + ".kvField", null);
            String kvFieldSplit = context.getString(category + ".kvFieldSplit", null);
            String kvSplit = context.getString(category + ".kvSplit", null);
            String kvMinNum = context.getString(category + ".kvMinNum", null);
            String storeOrgLog = context.getString(category + ".storeOrgLog", "true");

            if (converFields != null && !converFields.isEmpty() && converTypes != null && !converTypes.isEmpty()) {
                String[] cFields = converFields.split(",");
                String[] cTypes = converTypes.split(",");
                if (cFields.length != cTypes.length) {
                    logger.error("converFields' length is not equal with converTypes' length!");
                } else {
                    Map<String, String> maps = Maps.newHashMap();
                    for (int i = 0; i < cFields.length; i++) {
                        maps.put(cFields[i], cTypes[i]);
                    }
                    converMaps.put(category, maps);
                }
            }

            if (split != null && split.length() > 2) {
                split = split.substring(1, split.length() - 1);  //去掉前后的引号
            } else {
                split = null;
            }
            logger.info("category:"+category+" **** regex:" + regex + " fields:" + fields + " splitChar:" + split +
                    " timeField:" + timeField + " timeFormat:" + timeFormat +
                    " converFields:" + converFields + " converTypes:" + converTypes +
                    " kvFiled:" + kvField + " kvFieldSplit:" + kvFieldSplit + " kvSplit:" + kvSplit);
            if(regex == null && fields == null && split == null){
                continue;
            }

            categoryMap.put(category, new String[]{regex, fields, split, timeField, timeFormat, kvField, kvFieldSplit, kvSplit, kvMinNum, storeOrgLog});

            if(regex != null && !regex.isEmpty()){
                cache.put(regex, Pattern.compile(regex));
            }
        }
    }

    @Override
    public void configure(ComponentConfiguration conf) {
        // NO-OP...
    }
}
