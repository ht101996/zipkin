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

import java.io.IOException;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import zipkin.storage.AsyncSpanConsumer;
import zipkin.storage.AsyncSpanStore;
import zipkin.storage.SpanStore;
import zipkin.storage.StorageComponent;

public final class ElasticsearchRestStorage implements StorageComponent {
  final ElasticsearchStorage delegate;
  final RestClient client;

  public ElasticsearchRestStorage(ElasticsearchStorage delegate) {
    this.delegate = delegate;
    this.client = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();
  }

  @Override public SpanStore spanStore() {
    return delegate.spanStore();
  }

  @Override public AsyncSpanStore asyncSpanStore() {
    return delegate.asyncSpanStore();
  }

  @Override public AsyncSpanConsumer asyncSpanConsumer() {
    return new ElasticsearchRestSpanConsumer(client, delegate.indexNameFormatter);
  }

  @Override public CheckResult check() {
    return delegate.check();
  }

  @Override public void close() throws IOException {
    client.close();
    delegate.close();
  }

  void clear() {
    delegate.clear();
  }
}
