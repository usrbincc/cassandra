/*
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
package org.apache.cassandra.repair.messages;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Repair options.
 */
public class RepairOption
{
    public static final String SEQUENTIAL_KEY = "sequential";
    public static final String PRIMARY_RANGE_KEY = "primaryRange";
    public static final String INCREMENTAL_KEY = "incremental";
    public static final String JOB_THREADS_KEY = "jobThreads";
    public static final String RANGES_KEY = "ranges";
    public static final String COLUMNFAMILIES_KEY = "columnFamilies";
    public static final String DATACENTERS_KEY = "dataCenters";
    public static final String HOSTS_KEY = "hosts";

    // we don't want to push nodes too much for repair
    public static final int MAX_JOB_THREADS = 4;

    private static final Logger logger = LoggerFactory.getLogger(RepairOption.class);

    /**
     * Construct RepairOptions object from given map of Strings.
     * <p>
     * Available options are:
     *
     * <table>
     *     <thead>
     *         <tr>
     *             <th>key</th>
     *             <th>value</th>
     *             <th>default (when key not given)</th>
     *         </tr>
     *     </thead>
     *     <tbody>
     *         <tr>
     *             <td>sequential</td>
     *             <td>"true" if perform sequential repair.</td>
     *             <td>true</td>
     *         </tr>
     *         <tr>
     *             <td>primaryRange</td>
     *             <td>"true" if perform repair only on primary range.</td>
     *             <td>false</td>
     *         </tr>
     *         <tr>
     *             <td>incremental</td>
     *             <td>"true" if perform incremental repair.</td>
     *             <td>false</td>
     *         </tr>
     *         <tr>
     *             <td>jobThreads</td>
     *             <td>Number of threads to use to run repair job.</td>
     *             <td>1</td>
     *         </tr>
     *         <tr>
     *             <td>ranges</td>
     *             <td>Ranges to repair. A range is expressed as &lt;start token&gt;:&lt;end token&gt;
     *             and multiple ranges can be given as comma separated ranges(e.g. aaa:bbb,ccc:ddd).</td>
     *             <td></td>
     *         </tr>
     *         <tr>
     *             <td>columnFamilies</td>
     *             <td>Specify names of ColumnFamilies to repair.
     *             Multiple ColumnFamilies can be given as comma separated values(e.g. cf1,cf2,cf3).</td>
     *             <td></td>
     *         </tr>
     *         <tr>
     *             <td>dataCenters</td>
     *             <td>Specify names of data centers who participate in this repair.
     *             Multiple data centers can be given as comma separated values(e.g. dc1,dc2,dc3).</td>
     *             <td></td>
     *         </tr>
     *         <tr>
     *             <td>hosts</td>
     *             <td>Specify names of hosts who participate in this repair.
     *             Multiple hosts can be given as comma separated values(e.g. cass1,cass2).</td>
     *             <td></td>
     *         </tr>
     *     </tbody>
     * </table>
     *
     * @param options options to parse
     * @param partitioner partitioner is used to construct token ranges
     * @return RepairOptions object
     */
    public static RepairOption parse(Map<String, String> options, IPartitioner partitioner)
    {
        boolean sequential = !options.containsKey(SEQUENTIAL_KEY) || Boolean.parseBoolean(options.get(SEQUENTIAL_KEY));
        boolean primaryRange = Boolean.parseBoolean(options.get(PRIMARY_RANGE_KEY));
        boolean incremental = Boolean.parseBoolean(options.get(INCREMENTAL_KEY));

        int jobThreads = 1;
        if (options.containsKey(JOB_THREADS_KEY))
        {
            try
            {
                jobThreads = Integer.parseInt(options.get(JOB_THREADS_KEY));
            }
            catch (NumberFormatException ignore) {}
        }
        // ranges
        String rangesStr = options.get(RANGES_KEY);
        Set<Range<Token>> ranges = new HashSet<>();
        if (rangesStr != null)
        {
            StringTokenizer tokenizer = new StringTokenizer(rangesStr, ",");
            while (tokenizer.hasMoreTokens())
            {
                String[] rangeStr = tokenizer.nextToken().split(":", 2);
                if (rangeStr.length < 2)
                {
                    continue;
                }
                Token parsedBeginToken = partitioner.getTokenFactory().fromString(rangeStr[0].trim());
                Token parsedEndToken = partitioner.getTokenFactory().fromString(rangeStr[1].trim());
                ranges.add(new Range<>(parsedBeginToken, parsedEndToken));
            }
        }

        RepairOption option = new RepairOption(sequential, primaryRange, incremental, jobThreads, ranges);

        // data centers
        String dataCentersStr = options.get(DATACENTERS_KEY);
        Collection<String> dataCenters = new HashSet<>();
        if (dataCentersStr != null)
        {
            StringTokenizer tokenizer = new StringTokenizer(dataCentersStr, ",");
            while (tokenizer.hasMoreTokens())
            {
                dataCenters.add(tokenizer.nextToken().trim());
            }
        }
        option.getDataCenters().addAll(dataCenters);

        // hosts
        String hostsStr = options.get(HOSTS_KEY);
        Collection<String> hosts = new HashSet<>();
        if (hostsStr != null)
        {
            StringTokenizer tokenizer = new StringTokenizer(hostsStr, ",");
            while (tokenizer.hasMoreTokens())
            {
                hosts.add(tokenizer.nextToken().trim());
            }
        }
        option.getHosts().addAll(hosts);

        // columnfamilies
        String cfStr = options.get(COLUMNFAMILIES_KEY);
        Collection<String> columnFamilies = new HashSet<>();
        if (cfStr != null)
        {
            StringTokenizer tokenizer = new StringTokenizer(cfStr, ",");
            while (tokenizer.hasMoreTokens())
            {
                columnFamilies.add(tokenizer.nextToken().trim());
            }
        }
        option.getColumnFamilies().addAll(columnFamilies);

        // validate options
        if (jobThreads > MAX_JOB_THREADS)
        {
            throw new IllegalArgumentException("Too many job threads. Max is " + MAX_JOB_THREADS);
        }
        if (primaryRange && (!dataCenters.isEmpty() || !hosts.isEmpty()))
        {
            throw new IllegalArgumentException("You need to run primary range repair on all nodes in the cluster.");
        }

        return option;
    }

    private final boolean sequential;
    private final boolean primaryRange;
    private final boolean incremental;
    private final int jobThreads;

    private final Collection<String> columnFamilies = new HashSet<>();
    private final Collection<String> dataCenters = new HashSet<>();
    private final Collection<String> hosts = new HashSet<>();
    private final Collection<Range<Token>> ranges = new HashSet<>();

    public RepairOption(boolean sequential, boolean primaryRange, boolean incremental, int jobThreads, Collection<Range<Token>> ranges)
    {
        if (sequential && incremental)
        {
            String message = "It is not possible to mix sequential repair and incremental repairs.";
            logger.error(message);
            throw new IllegalArgumentException(message);
        }

        if (!FBUtilities.isUnix() && sequential)
        {
            logger.warn("Snapshot-based repair is not yet supported on Windows.  Reverting to parallel repair.");
            this.sequential = false;
        }
        else
        {
            this.sequential = sequential;
        }
        this.primaryRange = primaryRange;
        this.incremental = incremental;
        this.jobThreads = jobThreads;
        this.ranges.addAll(ranges);
    }

    public boolean isSequential()
    {
        return sequential;
    }

    public boolean isPrimaryRange()
    {
        return primaryRange;
    }

    public boolean isIncremental()
    {
        return incremental;
    }

    public int getJobThreads()
    {
        return jobThreads;
    }

    public Collection<String> getColumnFamilies()
    {
        return columnFamilies;
    }

    public Collection<Range<Token>> getRanges()
    {
        return ranges;
    }

    public Collection<String> getDataCenters()
    {
        return dataCenters;
    }

    public Collection<String> getHosts()
    {
        return hosts;
    }

    @Override
    public String toString()
    {
        return "repair options (" +
                       "sequential: " + sequential +
                       ", primary range: " + primaryRange +
                       ", incremental: " + incremental +
                       ", job threads: " + jobThreads +
                       ", ColumnFamilies: " + columnFamilies +
                       ", dataCenters: " + dataCenters +
                       ", hosts: " + hosts +
                       ", # of ranges: " + ranges.size() +
                       ')';
    }
}
