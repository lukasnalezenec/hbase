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
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormatBase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@InterfaceStability.Evolving
public class RegionSizeCalculator {

  final Log LOG = LogFactory.getLog(RegionSizeCalculator.class);

  Map<HRegionInfo, Long> sizeMap = new HashMap<HRegionInfo, Long>();

  Set<String> filteredFamilies;

  public RegionSizeCalculator(HTable table) throws IOException {
    this(table, null);
  }

  /**
   * Computes size of each region for table and given column families.
   * */
  public RegionSizeCalculator(HTable table, byte[][] families) throws IOException {

    Configuration configuration = table.getConfiguration();
    Path tablePath = FSUtils.getTableDir(FSUtils.getRootDir(configuration), table.getName());
    FileSystem fs = tablePath.getFileSystem(configuration);

    makeFamilyFilter(families);

    Set<HRegionInfo> regionInfos = table.getRegionLocations().keySet();

    for (HRegionInfo info : regionInfos) {
      long estimation = 0;

      String regionDirectory = info.getEncodedName();
      Path fullRegionPath = new Path(tablePath, regionDirectory);

      List<Path> allFamilyPaths = FSUtils.getFamilyDirs(fs, fullRegionPath);

      for (Path familyPath : allFamilyPaths) {
        String familyDirName = familyPath.getName();

        boolean familyIncluded = this.filteredFamilies == null || filteredFamilies.contains(familyDirName);
        if (familyIncluded && fs.exists(familyPath)) {
          estimation += fs.getContentSummary(familyPath).getSpaceConsumed();
        }

      }

      sizeMap.put(info, estimation);
    }
  }

  public long getRegionSize(HRegionInfo region) {
    Long size = sizeMap.get(region);
    return (size == null) ? 0 : size;
  }

  public Map<HRegionInfo, Long> getRegionSizeMap() {
    return Collections.unmodifiableMap(sizeMap);
  }

  //converts list of families to Set<String>
  private void makeFamilyFilter(byte[][] families) {
    if (families != null) {
      Set<String> result = new HashSet<String>(families.length);
      for (byte[] family : families) {
        result.add(new String(family));
      }

      if (!result.isEmpty()) filteredFamilies = result;
    }
  }
}
