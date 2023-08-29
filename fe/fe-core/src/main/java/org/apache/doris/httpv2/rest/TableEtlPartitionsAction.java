// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.httpv2.rest;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.ListPartitionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DorisHttpException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlColumn;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlPartition;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlPartitionInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class TableEtlPartitionsAction extends RestBaseController {

    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_etl_partitions", method = RequestMethod.GET)
    protected Object etlPartitions(
            @PathVariable(value = DB_KEY) final String dbName,
            @PathVariable(value = TABLE_KEY) final String tblName,
            HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);

        Map<String, Object> resultMap = new HashMap<>();

        try {
            String fullDbName = getFullDbName(dbName);
            // check privilege for select, otherwise return 401 HTTP status
            checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tblName, PrivPredicate.SELECT);
            OlapTable table;
            try {
                Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
                table = (OlapTable) db.getTableOrMetaException(tblName, Table.TableType.OLAP);
            } catch (MetaNotFoundException e) {
                return ResponseEntityBuilder.okWithCommonError(e.getMessage());
            }
            table.readLock();
            try {
                try {
                    EtlPartitionInfo etlPartitionInfo = createEtlPartitionInfo(table);
                    resultMap.put("partitionInfo", etlPartitionInfo);
                } catch (Exception e) {
                    // Transform the general Exception to custom DorisHttpException
                    return ResponseEntityBuilder.okWithCommonError(e.getMessage());
                }
            } finally {
                table.readUnlock();
            }
        } catch (DorisHttpException e) {
            // status code  should conforms to HTTP semantic
            resultMap.put("status", e.getCode().code());
            resultMap.put("exception", e.getMessage());
        }

        return ResponseEntityBuilder.ok(resultMap);
    }


    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_etl_columns", method = RequestMethod.GET)
    protected Object etlColumns(
            @PathVariable(value = DB_KEY) final String dbName,
            @PathVariable(value = TABLE_KEY) final String tblName,
            HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);

        Map<String, Object> resultMap = new HashMap<>();

        try {
            String fullDbName = getFullDbName(dbName);
            // check privilege for select, otherwise return 401 HTTP status
            checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tblName, PrivPredicate.SELECT);
            OlapTable table;
            try {
                Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
                table = (OlapTable) db.getTableOrMetaException(tblName, Table.TableType.OLAP);
            } catch (MetaNotFoundException e) {
                return ResponseEntityBuilder.okWithCommonError(e.getMessage());
            }
            table.readLock();
            try {
                try {
                    List<EtlColumn> etlColumns = createEtlColumns(table);
                    resultMap.put("columns", etlColumns);
                } catch (Exception e) {
                    // Transform the general Exception to custom DorisHttpException
                    return ResponseEntityBuilder.okWithCommonError(e.getMessage());
                }
            } finally {
                table.readUnlock();
            }
        } catch (DorisHttpException e) {
            // status code  should conforms to HTTP semantic
            resultMap.put("status", e.getCode().code());
            resultMap.put("exception", e.getMessage());
        }

        return ResponseEntityBuilder.ok(resultMap);
    }

    private EtlPartitionInfo createEtlPartitionInfo(OlapTable table) throws LoadException {
        PartitionType type = table.getPartitionInfo().getType();

        List<String> partitionColumnRefs = Lists.newArrayList();
        List<EtlPartition> etlPartitions = Lists.newArrayList();
        if (type == PartitionType.RANGE) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
            for (Column column : rangePartitionInfo.getPartitionColumns()) {
                partitionColumnRefs.add(column.getName());
            }

            for (Map.Entry<Long, PartitionItem> entry : rangePartitionInfo.getAllPartitionItemEntryList(true)) {
                long partitionId = entry.getKey();

                Partition partition = table.getPartition(partitionId);
                if (partition == null) {
                    throw new LoadException("partition does not exist. id: " + partitionId);
                }

                // bucket num
                int bucketNum = partition.getDistributionInfo().getBucketNum();

                // is max partition
                Range<PartitionKey> range = entry.getValue().getItems();
                boolean isMaxPartition = range.upperEndpoint().isMaxValue();

                // start keys
                List<LiteralExpr> rangeKeyExprs = range.lowerEndpoint().getKeys();
                List<Object> startKeys = Lists.newArrayList();
                for (int i = 0; i < rangeKeyExprs.size(); ++i) {
                    LiteralExpr literalExpr = rangeKeyExprs.get(i);
                    Object keyValue = literalExpr.getRealValue();
                    startKeys.add(keyValue);
                }

                // end keys
                // is empty list when max partition
                List<Object> endKeys = Lists.newArrayList();
                if (!isMaxPartition) {
                    rangeKeyExprs = range.upperEndpoint().getKeys();
                    for (int i = 0; i < rangeKeyExprs.size(); ++i) {
                        LiteralExpr literalExpr = rangeKeyExprs.get(i);
                        Object keyValue = literalExpr.getRealValue();
                        endKeys.add(keyValue);
                    }
                }

                etlPartitions.add(new EtlPartition(partitionId, startKeys, endKeys, isMaxPartition, bucketNum));
            }
        } else if (type == PartitionType.UNPARTITIONED) {
            for (Partition partition : table.getPartitions()) {
                if (partition == null) {
                    throw new LoadException("partition does not exist.");
                }
                // bucket num
                int bucketNum = partition.getDistributionInfo().getBucketNum();

                etlPartitions.add(new EtlPartition(partition.getId(), Lists.newArrayList(), Lists.newArrayList(),
                        true, bucketNum));
            }
        } else {
            ListPartitionInfo partitionInfo = (ListPartitionInfo) table.getPartitionInfo();
            for (Column column : partitionInfo.getPartitionColumns()) {
                partitionColumnRefs.add(column.getName());
            }
            Map<Long, PartitionItem> idToItem = partitionInfo.getIdToItem(false);
            for (Long partitionId : idToItem.keySet()) {
                Partition partition = table.getPartition(partitionId);

                if (partition == null) {
                    throw new LoadException("partition does not exist. id: " + partitionId);
                }

                // bucket num
                int bucketNum = partition.getDistributionInfo().getBucketNum();

                // partitionKeys
                List<Object> startKeys = Lists.newArrayList();
                List<PartitionKey> partitionKeys = idToItem.get(partitionId).getItems();
                for (PartitionKey partitionKey : partitionKeys) {
                    for (LiteralExpr key : partitionKey.getKeys()) {
                        startKeys.add(key.getRealValue());
                    }
                }
                etlPartitions.add(new EtlPartition(partitionId, startKeys, Lists.newArrayList(),
                        false, bucketNum));
            }
        }

        // distribution column refs
        List<String> distributionColumnRefs = Lists.newArrayList();
        DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
        Preconditions.checkState(distributionInfo.getType() == DistributionInfoType.HASH);
        for (Column column : ((HashDistributionInfo) distributionInfo).getDistributionColumns()) {
            distributionColumnRefs.add(column.getName());
        }

        return new EtlPartitionInfo(type.typeString, partitionColumnRefs, distributionColumnRefs, etlPartitions);
    }

    private List<EtlColumn> createEtlColumns(OlapTable table) {
        List<Column> columns = table.getBaseSchema();
        List<EtlColumn> etlColumns = new ArrayList<>(columns.size());

        for (Column column : columns) {
            etlColumns.add(createEtlColumn(column));
        }
        return etlColumns;
    }

    private EtlColumn createEtlColumn(Column column) {
        // column name
        String name = column.getName();
        // column type
        PrimitiveType type = column.getDataType();
        String columnType = column.getDataType().toString();
        // is allow null
        boolean isAllowNull = column.isAllowNull();
        // is key
        boolean isKey = column.isKey();

        // aggregation type
        String aggregationType = null;
        if (column.getAggregationType() != null) {
            aggregationType = column.getAggregationType().toString();
        }

        // default value
        String defaultValue = null;
        if (column.getDefaultValue() != null) {
            defaultValue = column.getDefaultValue();
        }
        if (column.isAllowNull() && column.getDefaultValue() == null) {
            defaultValue = "\\N";
        }

        // string length
        int stringLength = 0;
        if (type.isStringType()) {
            stringLength = column.getStrLen();
        }

        // decimal precision scale
        int precision = 0;
        int scale = 0;
        if (type.isDecimalV2Type() || type.isDecimalV3Type()) {
            precision = column.getPrecision();
            scale = column.getScale();
        }

        return new EtlColumn(name, columnType, isAllowNull, isKey, aggregationType, defaultValue,
                stringLength, precision, scale);
    }
}
