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
package edu.usc.pgroup.louvain.hadoop;

import org.apache.hadoop.mapred.FileInputFormat;

import java.io.*;
import java.util.*;

/**
 * Created by Charith Wickramaarachchi on 6/25/14.
 */
public class Graph {


    public static final int WEIGHTED = 0;
    public static final int UNWEIGHTED = 1;


    private int nb_nodes;
    private long nb_links;
    private double total_weight;

    private List<Long> degrees = new ArrayList<Long>(1000);
    private List<Integer> links = new ArrayList<Integer>(5000);
    private List<Float> weights = new ArrayList<Float>(5000);

    private HashMap<Integer, List<HashMap.SimpleEntry>> remoteMappings = new HashMap<Integer, List<HashMap.SimpleEntry>>();


    Graph() {
        nb_nodes = 0;
        nb_links = 0;
        total_weight = 0;

    }


    Graph(InputStream inputStream, int type) throws Exception {
        // Assume metis undirected format and unweighted graph.

        BufferedReader fileReader = new BufferedReader(new InputStreamReader(inputStream));

        String line = fileReader.readLine();

        TreeMap<Integer,String> graph = new TreeMap<Integer, String>();

        boolean readingGraph = true;
        while (line != null) {

            if ("****".equals(line.trim())) {
                readingGraph = false;
                continue;
            }

            if (readingGraph) {
                StringTokenizer tokenizer = new StringTokenizer(line);
                int nodeId = Integer.parseInt(tokenizer.nextToken());
                graph.put(nodeId,line);
            } else {
                //read remote mappings
                String parts[] = line.split(" ");
                int source = Integer.parseInt(parts[1]);
                String []sinkP = parts[1].split(",");
                int tartgetP = Integer.parseInt(sinkP[0]);
                int sink  = Integer.parseInt(sinkP[1]);

                if(remoteMappings.containsKey(source)) {
                    remoteMappings.get(source).add(new HashMap.SimpleEntry(tartgetP,sink));
                } else {
                    ArrayList<HashMap.SimpleEntry> list = new ArrayList<HashMap.SimpleEntry>();
                    list.add(new HashMap.SimpleEntry(tartgetP,sink));
                    remoteMappings.put(source,list);
                }

            }



            line = fileReader.readLine();
        }


        Iterator<Integer> it = graph.keySet().iterator();

        while (it.hasNext()) {
            int n = it.next();
            StringTokenizer tokenizer = new StringTokenizer(graph.get(n));

            int nodeId = Integer.parseInt(tokenizer.nextToken());
            int count = 0;

            while (tokenizer.hasMoreTokens()) {
                int e = Integer.parseInt(tokenizer.nextToken());
                links.add(e);
                count++;
            }

            degrees.add(nodeId, (long) count);

        }

        graph = null;

        this.nb_nodes = degrees.size();
        this.nb_links = degrees.get(nb_nodes - 1);
        this.total_weight = 0;

        // Compute total weight
        for (int i = 0; i < nb_nodes; i++) {
            total_weight += (double) weighted_degree(i);
        }


    }

    Graph(String fileName, int type) throws Exception {

        this(new FileInputStream(fileName), type);

    }


    void display() {
        for (int node = 0; node < nb_nodes; node++) {
            Pair<Integer, Integer> p = neighbors(node);
            System.out.print("" + node + ":");

            for (int i = 0; i < nb_neighbors(node); i++) {
                if (true) {
                    if (weights.size() != 0)
                        System.out.print(" (" + links.get(p.getElement0() + i) + " " + weights.get(p.getElement1() + i) + ")");
                    else
                        System.out.print(" " + links.get(p.getElement0() + i));
                }
            }
            System.out.print("\n");
        }
    }


    void display_binary(OutputStream out) {
        //TODO
    }


    boolean check_symmetry() {
        int error = 0;
        for (int node = 0; node < nb_nodes; node++) {
            Pair<Integer, Integer> p = neighbors(node);
            for (int i = 0; i < nb_neighbors(node); i++) {
                int neigh = links.get(p.getElement0() + i);
                float weight = weights.get(p.getElement1() + i);

                Pair<Integer, Integer> p_neigh = neighbors(neigh);
                for (int j = 0; j < nb_neighbors(neigh); j++) {
                    int neigh_neigh = links.get(p_neigh.getElement0() + j);
                    float neigh_weight = weights.get(p_neigh.getElement1() + j);

                    if (node == neigh_neigh && weight != neigh_weight) {
                        System.out.println("" + node + " " + neigh + " " + weight + " " + neigh_neigh);
                        if (error++ == 10) {
                            System.exit(0);
                        }
                    }
                }
            }
        }
        return (error == 0);
    }


    // return the number of neighbors (degree) of the node
    public long nb_neighbors(int node) {
        assert (node >= 0 && node < nb_nodes);

        if (node == 0)
            return degrees.get(0);
        else
            return (degrees.get(node) - degrees.get(node - 1));

    }

    // return the number of self loops of the node
    public double nb_selfloops(int node) {
        assert (node >= 0 && node < nb_nodes);

        Pair<Integer, Integer> p = neighbors(node);
        for (int i = 0; i < nb_neighbors(node); i++) {
            if (links.get(p.getElement0() + i) == node) {
                if (weights.size() != 0)
                    return (double) weights.get(p.getElement1() + i);
                else
                    return 1.0;
            }
        }
        return 0.0;
    }

    // return the weighted degree of the node
    public double weighted_degree(int node) {
        assert (node >= 0 && node < nb_nodes);

        if (weights.size() == 0)
            return (double) nb_neighbors(node);
        else {
            Pair<Integer, Integer> p = neighbors(node);
            double res = 0;
            for (int i = 0; i < nb_neighbors(node); i++) {
                res += (double) weights.get(p.getElement1() + i);
            }
            return res;
        }
    }

    // return pointers to the first neighbor and first weight of the node
    public Pair<Integer, Integer> neighbors(int node) {
        if (node == 0)
            return new Pair<Integer, Integer>(0, 0);
        else if (weights.size() != 0)
            return new Pair(degrees.get(node - 1), degrees.get(node - 1));
        else
            return new Pair(degrees.get(node - 1), 0);
    }


    public int getNb_nodes() {
        return nb_nodes;
    }

    public long getNb_links() {
        return nb_links;
    }

    public double getTotal_weight() {
        return total_weight;
    }

    public List<Long> getDegrees() {
        return degrees;
    }

    public List<Integer> getLinks() {
        return links;
    }

    public List<Float> getWeights() {
        return weights;
    }

    public void setNb_nodes(int nb_nodes) {
        this.nb_nodes = nb_nodes;
    }

    public void setNb_links(long nb_links) {
        this.nb_links = nb_links;
    }

    public void setTotal_weight(double total_weight) {
        this.total_weight = total_weight;
    }
}
