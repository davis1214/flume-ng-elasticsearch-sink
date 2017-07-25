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

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
//import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Test;

import java.util.Map;

import static org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer.charset;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;

public class TestElasticSearchRegexSerializer {

  @Test
  public void testRoundTrip() throws Exception {
/*    ElasticSearchRegexSerializer fixture = new ElasticSearchRegexSerializer();
    Context context = new Context();
    context.put("categories","category1");
    context.put("category1.regex","^([^\\s]*) - - \\[(.*)\\] \".* ([^\\s]*) [^\\s]*\" ([^\\s]*) [^\\s]* ([^\\s]*) [^\\s]* \"[^\"]*\" \"[^\"]*\" \"[^\"]*\" \"([^\"]*)\" \"([^\"]*)\"$");
    context.put("category1.fields", "sip,localTime,api,httpCode,requestTime,cip,vip");


    fixture.configure(context);

    String message = "10.2.26.41 - - [17/Aug/2015:17:49:22 +0800] \"GET /outgiveList.do HTTP/1.1\" 200 30287 0.385 - \"http://sell.koudai.com/adminHome.do\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25\" \"-\" \"114.113.31.3\" \"sell.koudai.com\"";

    Map<String, String> headers = Maps.newHashMap();
    headers.put("headerNameOne", "headerValueOne");
    headers.put("headerNameTwo", "headerValueTwo");
    headers.put("headerNameThree", "headerValueThree");
    Event event = EventBuilder.withBody(message.getBytes(charset));
    event.setHeaders(headers);*/

/*
    XContentBuilder expected = jsonBuilder().startObject();
    expected.field("body", new String(message.getBytes(), charset));



    for (String headerName : headers.keySet()) {
      expected.field(headerName, new String(headers.get(headerName).getBytes(),
          charset));
    }
    expected.endObject();

    XContentBuilder actual = fixture.getContentBuilder(event);

    assertEquals(new String(expected.bytes().array()), new String(actual
        .bytes().array()));*/

  }
}
