/*
 *  Copyright 2013 University of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.package edu.usc.goffish.gopher.sample;
 */
package edu.usc.pgroup.louvain.hadoop.tools;

import java.io.*;
import java.util.*;

/**
 * Created by Charith Wickramaarachchi on 7/3/14.
 */
public class DataConverter {

    /**
     * Data Converter accepts graph in Metis format and create partition files
     * Input format metis formatted graph and partition file
     * Output graph partition files
     * @param args
     */

    private static List<Integer> mapping = new ArrayList<Integer>();

    public static void main(String[] args) throws Exception {

        BufferedReader partitionReader = new BufferedReader(new FileReader(args[1]));

        String line = partitionReader.readLine();

        int numberOfPartitions = Integer.parseInt(args[2]);
        int idx=0;

        System.out.println("Loading Partition data...");

        while (line != null) {

            int v = Integer.parseInt(line.trim());
            mapping.add(idx++,v);

            line = partitionReader.readLine();
        }

        System.out.println("Loading Partition data complete...");

        partitionReader.close();

        File file  = new File(args[0]);

        String fileName = file.getName();
        String graphFileName = fileName.replaceFirst("[.][^.]+$", "");


        PrintWriter []writer = new PrintWriter[numberOfPartitions];

        for(int i=0;i < numberOfPartitions;i++) {
            writer[i] = new PrintWriter(new FileWriter(file.getParent()+ File.separator + graphFileName + "_" + i + ".part"));
        }



        List<List<Integer>> partitions = new ArrayList<List<Integer>>(numberOfPartitions);

        List<List<HashMap.SimpleEntry>> remoteList = new ArrayList<List<AbstractMap.SimpleEntry>>();



        for(int i=0; i < numberOfPartitions; i++) {
            partitions.add(i,new ArrayList<Integer>());
            remoteList.add(i,new ArrayList<AbstractMap.SimpleEntry>());
        }

        BufferedReader graphReader = new BufferedReader(new FileReader(args[0]));

        line = graphReader.readLine();
        line = graphReader.readLine();


        int vid=0;
        while ( line != null) {

            StringTokenizer tokenizer = new StringTokenizer(line);

            int currentParttition = mapping.get(vid);
            List<Integer> currentList = partitions.get(currentParttition);
            currentList.add(vid);

            List<HashMap.SimpleEntry> currentRemote = remoteList.get(currentParttition);
            while (tokenizer.hasMoreTokens()) {
                String e = tokenizer.nextToken();

                int e1 = Integer.parseInt(e);

                if(mapping.get(vid) == mapping.get(e1-1)) {
                    currentList.add((e1-1));

                } else {

                    currentRemote.add(new HashMap.SimpleEntry(vid,(e1-1)));

                }

            }
            currentList.add(-1);
            vid++;
            line = graphReader.readLine();
        }

        System.out.println("Partitioning done....");


        System.out.println("Start Renumbering....");


        HashMap<Integer,Integer> localMap = new HashMap<Integer, Integer>();



        for(int i=0;i < numberOfPartitions;i++) {

            List<Integer> currentPart = partitions.get(i);

            List<Integer> newCurrentPart = new ArrayList<Integer>(partitions.size());
            int localV=0;
            for(int j =0 ; j < currentPart.size(); j++) {

                int val = currentPart.get(j);

                if(val == -1) {
                    newCurrentPart.add(j,-1);
                } else {
                    if(localMap.containsKey(val)) {
                        newCurrentPart.add(j,localMap.get(val));
                    } else {
                        int v= localV++;
                        localMap.put(val,v);
                        newCurrentPart.add(j,v);
                    }
                }

            }

            currentPart.clear();
            partitions.set(i,newCurrentPart);
        }

        System.out.println("Re-Numbering done..");

        System.out.println("Start Writing Partitions...");


        for(int i =0; i < numberOfPartitions;i++) {

            PrintWriter currentWriter = writer[i];

            System.out.println("Writing Local Partition data for partition " + i);
            List<Integer> currentPartition = partitions.get(i);

            for(int j=0;j < currentPartition.size();j++) {

                int val = currentPartition.get(j);
                if(val != -1) {
                    currentWriter.write("" + val +" ");
                } else {
                    currentWriter.println();
                }
            }

            currentWriter.println("****");
            System.out.println("Writing Remote Partition data for partition " + i);

            List<HashMap.SimpleEntry> currentRemoteList = remoteList.get(i);

            for(int j=0; j < currentRemoteList.size(); j++) {

                HashMap.SimpleEntry entry = currentRemoteList.get(i);
                int source = (Integer)entry.getKey();
                int sink = (Integer)entry.getValue();
                int sinkPartition = mapping.get(sink);
                int sinkLocal = localMap.get(sink);
                int sourceLocal = localMap.get(source);

                currentWriter.println("" + sourceLocal + " " + sinkPartition +"," + sinkLocal);

            }

            System.out.println("Done partition " + i);

        }



        for(int i =0 ; i < numberOfPartitions;i++) {
            writer[i].close();
        }

        graphReader.close();
    }


}
