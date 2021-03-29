/**
 * Copyright Pravega Authors.
 *
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
package io.pravega.segmentstore.server.attributes;

import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.storage.Storage;

/**
 * Defines a Factory for an ContainerAttributeIndex.
 */
public interface AttributeIndexFactory {
    /**
     * Creates a new ContainerAttributeIndex for a specific Segment Container.
     *
     * @param containerMetadata The Segment Container's Metadata.
     * @param storage           The Storage to read from and write to.
     * @return A new instance of a class implementing ContainerAttributeIndex.
     */
    ContainerAttributeIndex createContainerAttributeIndex(ContainerMetadata containerMetadata, Storage storage);
}
