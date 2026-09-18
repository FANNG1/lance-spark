/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.search;

import org.lance.spark.LanceSparkReadOptions;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Everything a distributed search needs to open the dataset without going through {@code
 * LanceNamespace.queryTable}: the read options, the namespace client to recreate on the executor,
 * and the storage options the driver started from.
 *
 * <p>Travels from {@link LanceDistributedSearchTable} down to every {@link
 * LanceDistributedSearchInputPartition}, so the whole distributed path passes one object around
 * instead of four parameters that are only meaningful together.
 */
public final class LanceDistributedSearchContext implements Serializable {
  private static final long serialVersionUID = -529183742019374615L;

  private final LanceSparkReadOptions readOptions;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;
  private final Map<String, String> initialStorageOptions;

  public LanceDistributedSearchContext(
      LanceSparkReadOptions readOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      Map<String, String> initialStorageOptions) {
    this.readOptions = Objects.requireNonNull(readOptions, "readOptions");
    this.namespaceImpl = Objects.requireNonNull(namespaceImpl, "namespaceImpl");
    this.namespaceProperties = immutableCopy(namespaceProperties);
    this.initialStorageOptions = immutableCopy(initialStorageOptions);
  }

  public LanceSparkReadOptions getReadOptions() {
    return readOptions;
  }

  public String getNamespaceImpl() {
    return namespaceImpl;
  }

  public Map<String, String> getNamespaceProperties() {
    return namespaceProperties;
  }

  public Map<String, String> getInitialStorageOptions() {
    return initialStorageOptions;
  }

  private static Map<String, String> immutableCopy(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new HashMap<>(properties));
  }
}
