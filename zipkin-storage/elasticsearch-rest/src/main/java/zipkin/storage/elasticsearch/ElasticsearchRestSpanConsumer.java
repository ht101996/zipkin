/**
 * Copyright 2015-2016 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.storage.elasticsearch;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import zipkin.Codec;
import zipkin.Span;
import zipkin.internal.Util;
import zipkin.storage.AsyncSpanConsumer;
import zipkin.storage.Callback;

import static zipkin.internal.ApplyTimestampAndDuration.guessTimestamp;
import static zipkin.storage.elasticsearch.ElasticsearchSpanConsumer.prefixWithTimestampMillis;

final class ElasticsearchRestSpanConsumer implements AsyncSpanConsumer {
  final RestClient client;
  final IndexNameFormatter indexNameFormatter;

  ElasticsearchRestSpanConsumer(RestClient client, IndexNameFormatter indexNameFormatter) {
    this.client = client;
    this.indexNameFormatter = indexNameFormatter;
  }

  @Override public void accept(List<Span> spans, final Callback<Void> callback) {
    StringBuilder request = new StringBuilder();
    for (Span s : spans) {
      createSpanIndexRequest(s, request);
    }

    HttpEntity entity = new NStringEntity(request.toString(), ContentType.APPLICATION_JSON);
    client.performRequest("POST", "/_bulk", Collections.<String, String>emptyMap(), entity,
        new ResponseListener() {
          @Override public void onSuccess(Response response) {
            callback.onSuccess(null);
          }

          @Override public void onFailure(Exception e) {
            callback.onError(e);
          }
        });
  }

  // mostly copy/pasted from ElasticSearchSpanConsumer
  void createSpanIndexRequest(Span span, StringBuilder builder) {
    Long timestamp = guessTimestamp(span);
    long timestampMillis; // which index to store this span into
    final byte[] spanBytes;
    if (timestamp != null) {
      timestampMillis = TimeUnit.MICROSECONDS.toMillis(timestamp);
      spanBytes = prefixWithTimestampMillis(Codec.JSON.writeSpan(span), timestampMillis);
    } else {
      timestampMillis = System.currentTimeMillis();
      spanBytes = Codec.JSON.writeSpan(span);
    }
    String index = indexNameFormatter.indexNameForTimestamp(timestampMillis);
    builder.append("{\"index\":{\"_index\":\"").append(index).append("\",\"_type\":\"span\"}}\n");
    builder.append(new String(spanBytes, Util.UTF_8)).append('\n');
  }
}
