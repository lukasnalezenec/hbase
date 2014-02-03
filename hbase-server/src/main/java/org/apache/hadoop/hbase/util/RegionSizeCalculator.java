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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  private final Map<HRegionInfo, Long> sizeMap = new HashMap<HRegionInfo, Long>();

  static final String DISABLE_REGIONSIZECALCULATOR = "hbase.regionsizecalculator.disabled";

  public RegionSizeCalculator(HTable table) throws IOException {
    this(table, null);
  }

  /**
   * Computes size of each region for table and given column families.
   * */
  public RegionSizeCalculator(HTable table, byte[][] families) throws IOException {
    Configuration configuration = table.getConfiguration();

    if (configuration.getBoolean(DISABLE_REGIONSIZECALCULATOR, false)) {
      LOG.info("Region size calculation disabled.");
      return;
    }

    LOG.info("Calculating region sizes for table \"" + new String(table.getTableName()) + "\".");

    Path tablePath = FSUtils.getTableDir(FSUtils.getRootDir(configuration), table.getName());
    FileSystem fs = tablePath.getFileSystem(configuration);

    final Set<String> filteredFamilies = makeFamilyFilter(families);

    Set<HRegionInfo> regionInfos = table.getRegionLocations().keySet();

    for (HRegionInfo info : regionInfos) {
      long regionSize = 0;

      String regionDirectory = info.getEncodedName();
      Path fullRegionPath = new Path(tablePath, regionDirectory);

      List<Path> allFamilyPaths = FSUtils.getFamilyDirs(fs, fullRegionPath);

      for (Path familyPath : allFamilyPaths) {
        String familyDirName = familyPath.getName();

        boolean familyIncluded = filteredFamilies == null || filteredFamilies.contains(familyDirName);
        if (familyIncluded && fs.exists(familyPath)) {
          regionSize += fs.getContentSummary(familyPath).getSpaceConsumed();
        }
      }

      sizeMap.put(info, regionSize);
    }
  }

  /**
   * Returns size of given region in bytes. Returns 0 if region was not found.
   * */
  public long getRegionSize(HRegionInfo region) {
    Long size = sizeMap.get(region);
    return (size == null) ? 0 : size;
  }

  public Map<HRegionInfo, Long> getRegionSizeMap() {
    return Collections.unmodifiableMap(sizeMap);
  }

  //converts list of families to Set<String>
  private Set<String> makeFamilyFilter(byte[][] families) {
    if (families != null) {
      Set<String> result = new HashSet<String>(families.length);
      for (byte[] family : families) {
        result.add(Bytes.toString(family));
      }

      if (!result.isEmpty()) return result;
    }

    return null;
  }
}
