/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.elasticsearch;

import com.google.common.base.Splitter;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_CATEGORY_NAME;
//import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_CATEGORY_REGEX;
//import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_CATEGORY_FIELDS;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.CATEGORIES;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Basic serializer that serializes the event body and header fields into
 * individual fields</p>
 * <p/>
 * A best effort will be used to determine the content-type, if it cannot be
 * determined fields will be indexed as Strings
 */
public class ElasticSearchRegexSerializer implements
        ElasticSearchEventSerializer {
    private static String categories = null;
    private static final Map<String, String[]> categoryMap = new HashMap<String, String[]>();
    private static final Map<String, Pattern> cache = new HashMap<String, Pattern>();
    private static final Logger logger = LoggerFactory
            .getLogger(ElasticSearchRegexSerializer.class);

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
            String regex = context.getString(category + ".regex");
            String fields = context.getString(category + ".fields");
            String split = context.getString(category + ".split");
            if (StringUtils.isNotBlank(split) && split.length() > 2) {
                split = split.substring(1, split.length() - 1);
            } else {
                split = null;
            }
            logger.info("**** regex:" + regex + " fields:" + fields + " splitChar:" + split);
            if (regex == null && fields == null && split == null) {
                continue;
            }

            categoryMap.put(category, new String[]{regex, fields, split});
            if (regex != null && !regex.isEmpty()) {
                cache.put(regex, Pattern.compile(regex));
            }
        }


    }

    @Override
    public void configure(ComponentConfiguration conf) {
        // NO-OP...
    }

    @Override
    public XContentBuilder getContentBuilder(Event event) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        appendBody(builder, event);
        appendHeaders(builder, event);
        return builder;
    }

    private void appendBody(XContentBuilder builder, Event event)
            throws IOException {
        ContentBuilderUtil.appendField(builder, "body", event.getBody());

        Map<String, String> headers = event.getHeaders();
        String category = headers.get("category");
        if (category == null || category.isEmpty()) {
            category = headers.get("topic");
        }
        String[] regFields = categoryMap.get(category);

        if (regFields == null) {
            regFields = categoryMap.get(DEFAULT_CATEGORY_NAME);
        }
        if (regFields == null) {
            return;
        }

        String regex = regFields[0];
        String fields = regFields[1];
        String split = regFields[2];

        Iterable<String> fieldIte = Splitter.on(',').omitEmptyStrings().split(fields);
        String raw = new String(event.getBody(), charset);


        try {
            List<String> vals = parseRegex(raw, regex, split);

            int i = 0;
            for (String key : fieldIte) {
                String value = i >= vals.size() ? "" : vals.get(i++);

                ContentBuilderUtil.appendField(builder, key, value.getBytes(charset));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


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


    private void appendHeaders(XContentBuilder builder, Event event)
            throws IOException {
        Map<String, String> headers = event.getHeaders();

        for (String key : headers.keySet()) {
            ContentBuilderUtil.appendField(builder, key,
                    headers.get(key).getBytes(charset));
        }
    }

}
