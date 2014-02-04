/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

@InterfaceStability.Evolving
/**
 * Computes size of each region for given table and given column families.
 * The value is used by MapReduce for better scheduling.
 * */
public class RegionSizeCalculator {

  private final Log LOG = LogFactory.getLog(RegionSizeCalculator.class);

  /**
   * Maps each region represented by HRegionInfo to its size in bytes.
   * */
  private final Map<byte[], Long> sizeMap = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);

  static final String DISABLE_REGIONSIZECALCULATOR = "hbase.regionsizecalculator.disabled";

  /**
   * Computes size of each region for table and given column families.
   * */
  public RegionSizeCalculator(HTable table) throws IOException {
    Configuration configuration = table.getConfiguration();

    if (configuration.getBoolean(DISABLE_REGIONSIZECALCULATOR, false)) {
      LOG.info("Region size calculation disabled.");
      return;
    }

    LOG.info("Calculating region sizes for table \"" + new String(table.getTableName()) + "\".");

    //TODO filter regions by table !!!!!!

    HBaseAdmin admin = new HBaseAdmin(configuration);

    ClusterStatus clusterStatus = admin.getClusterStatus();
    Collection<ServerName> servers = clusterStatus.getServers();

    for (ServerName serverName: servers) {
      ServerLoad serverLoad = clusterStatus.getLoad(serverName);

      for (Map.Entry<byte[], RegionLoad> regionEntry: serverLoad.getRegionsLoad().entrySet()) {
        byte[] regionId = regionEntry.getKey();
        RegionLoad regionLoad = regionEntry.getValue();

        long regionSize = 1024 * 1024 * (regionLoad.getMemStoreSizeMB() + regionLoad.getStorefileSizeMB());

        sizeMap.put(regionId, regionSize);
      }
    }

  }


  /**
   * Returns size of given region in bytes. Returns 0 if region was not found.
   * */
  public long getRegionSize(byte[] regionId) {
    Long size = sizeMap.get(regionId);
    return (size == null) ? 0 : size;
  }

  public Map<byte[], Long> getRegionSizeMap() {
    return Collections.unmodifiableMap(sizeMap);
  }
}
