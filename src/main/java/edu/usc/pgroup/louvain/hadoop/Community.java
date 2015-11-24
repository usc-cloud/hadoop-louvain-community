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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;

/**
 * Created by Charith Wickramaarachchi on 6/25/14.
 */
public class Community {

    private List<Double> neigh_weight;
    private List<Integer> neigh_pos;
    private int neigh_last;

    private Graph g;

    private int size;

    private List<Integer> n2c; // community to which each node belongs
    private List<Double> in, tot; // used to compute the modularity participation of each community


    private Vector<Integer> n2c_new = new Vector<Integer>();

    // number of pass for one level computation
    // if -1, compute as many pass as needed to increase modularity
    private int nb_pass;

    // a new pass is computed if the last one has generated an increase
    // greater than min_modularity
    // if 0. even a minor increase is enough to go for one more pass
    private double min_modularity;

    public Community(InputStream inputStream,int type,int nb_pass,double min_modularity) throws Exception {
        g= new Graph(inputStream,type);

        size = g.getNb_nodes();

        neigh_weight = new ArrayList<Double>(size);
        Util.initArrayList((ArrayList<Double>) neigh_weight,size,-1.0);
        neigh_pos = new ArrayList<Integer>(size);
        Util.initArrayList((ArrayList<Integer>)neigh_pos,size,0);

        neigh_last = 0;

        n2c = new ArrayList<Integer>(size);
        in = new ArrayList<Double>(size);
        tot = new ArrayList<Double>(size);

        for (int i = 0; i < size; i++) {
            n2c.add(i);
            tot.add(g.weighted_degree(i));
            in.add(g.nb_selfloops(i));
        }

        this.nb_pass = nb_pass;
        this.min_modularity = min_modularity;

    }

    public Community(Graph g, int nb_pass, double min_modularity){

        this.g = g;
        size = g.getNb_nodes();

        neigh_weight = new ArrayList<Double>(size);
        Util.initArrayList((ArrayList<Double>) neigh_weight,size,-1.0);
        neigh_pos = new ArrayList<Integer>(size);
        Util.initArrayList((ArrayList<Integer>)neigh_pos,size,0);
        neigh_last = 0;

        n2c = new ArrayList<Integer>(size);
        in = new ArrayList<Double>(size);
        tot = new ArrayList<Double>(size);
        for (int i = 0; i < size; i++) {
            n2c.add(i);
            tot.add(g.weighted_degree(i) + g.weighted_degree_wremote(i));
            in.add(g.nb_selfloops(i));
        }

        this.nb_pass = nb_pass;
        this.min_modularity = min_modularity;

    }

    public void remove(int node, int comm, double dnodecomm) {

        assert(node >= 0 && node < size);

        tot.set(comm,tot.get(comm)-g.weighted_degree(node) - g.weighted_degree_wremote(node));
        in.set(comm, in.get(comm) - (2 * dnodecomm + g.nb_selfloops(node)));
        n2c.set(node,-1);

    }

    public void insert(int node, int comm, double dnodecomm) {

        assert(node >= 0 && node < size);

        tot.set(comm,tot.get(comm) + g.weighted_degree(node) + g.weighted_degree_wremote(node));
        in.set(comm, in.get(comm) +  (2 * dnodecomm + g.nb_selfloops(node)));
        n2c.set(node,comm);

    }

    public double modularity_gain(int node, int comm, double dnodecomm, double w_degree){
        assert(node >= 0 && node < size);

        double totc = tot.get(comm);
        double degc = w_degree;
        double m2 = g.getTotal_weight();
        double dnc =  dnodecomm;

        return (dnc - totc * degc / m2);

    }

    public void neigh_comm( int node) {
        for ( int i = 0; i < neigh_last; i++) {;
            neigh_weight.set(neigh_pos.get(i),-1.0);
        }
        neigh_last = 0;

        Pair<Integer, Integer> p  = g.neighbors(node);

        long deg = g.nb_neighbors(node);


        neigh_pos.set(0,n2c.get(node));
        neigh_weight.set(neigh_pos.get(0),0.0);
        neigh_last = 1;

        for (int i = 0; i < deg; i++) {
            int neigh = g.getLinks().getList().get(p.getElement0() + i);;
            int neigh_comm = n2c.get(neigh);
            double neigh_w = (g.getWeights().size() == 0) ? 1. : g.getWeights().getList().get(p.getElement1() + i);;

            if (neigh != node) {
                if (neigh_weight.get(neigh_comm) == -1) {
                    neigh_weight.set(neigh_comm,0.0);
                    neigh_pos.set(neigh_last++,neigh_comm);
                }
                neigh_weight.set(neigh_comm, neigh_weight.get(neigh_comm) + neigh_w);
            }
        }


        if(g.isContainRemote() && g.getRemoteEdges().containsKey(node)) {

            AbstractMap.SimpleEntry<Vector<Integer>, Vector<Float>> p1 = g.remote_neighbors(node);

            deg = g.nb_remote_neighbors(node);

            for (int i = 0; i < deg; i++) {
                int neigh = p1.getKey().getList().get(i);
                int neigh_comm = n2c.get(neigh);
                double neigh_w = (g.getWeights().size() == 0) ? 1.0 : p1.getValue().getList().get(i);

                if (neigh != node) {
                    if (neigh_weight.get(neigh_comm) == -1) {
                        neigh_weight.set(neigh_comm,0.0);
                        neigh_pos.set(neigh_last++,neigh_comm);
                    }
                    neigh_weight.set(neigh_comm,neigh_weight.get(neigh_comm) + neigh_w);
                }
            }
        }
    }

    public double modularity() {
        double q = 0.;
        double m2 = (double) g.getTotal_weight();

        for (int i = 0; i < size; i++) {
            if (tot.get(i) > 0) {
                double tmp = ((double) in.get(i) /(double) m2) - ((double) tot.get(i) / (double)m2)*((double) tot.get(i) / (double)m2);
                q += tmp;
            }

        }

        return q;
    }

    // displays the graph of communities as computed by one_level
    public void partition2graph() {


    }
    // displays the current partition (with communities renumbered from 0 to k-1)
    public void display_partition() {


        ArrayList<Integer> renumber = new ArrayList<Integer>(size);
        Util.initArrayList(renumber,size,-1);
        for (int node = 0; node < size; node++) {
            renumber.set(n2c.get(node),renumber.get(n2c.get(node)) + 1);
        }

        int fin = 0;
        for (int i = 0; i < size; i++)
            if (renumber.get(i) != -1) {
                renumber.set(i,fin++);
            }


        for (int i = 0; i < size; i++) {
            System.out.println("" + i + " " + renumber.get(n2c.get(i)));
        }

    }


    // displays the current partition (with communities renumbered from 0 to k-1)
    public void display_partition(String fileName) throws Exception{


        Path pt = new Path(fileName);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(pt)) {
            fs.delete(pt, true);

        }


        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));

        PrintWriter out = new PrintWriter(br);
        ArrayList<Integer> renumber = new ArrayList<Integer>(size);
        Util.initArrayList(renumber,size,-1);
        for (int node = 0; node < size; node++) {
            renumber.set(n2c.get(node),renumber.get(n2c.get(node)) + 1);
        }

        int fin = 0;
        for (int i = 0; i < size; i++)
            if (renumber.get(i) != -1) {
                renumber.set(i,fin++);
            }


        for (int i = 0; i < size; i++) {
            out.println("" + i + " " + renumber.get(n2c.get(i)));
        }


        out.flush();
        out.close();



    }


    // generates the binary graph of communities as computed by one_level
    public Graph partition2graph_binary(){
        ArrayList<Integer> renumber = new ArrayList<Integer>(size);
        Util.initArrayList(renumber,size,-1);
        for (int node = 0; node < size; node++) {
            renumber.set(n2c.get(node),renumber.get(n2c.get(node)) + 1);
        }

        int fin = 0;
        for (int i = 0; i < size; i++)
            if (renumber.get(i) != -1) {
                renumber.set(i,fin++);
            }




        ArrayList<ArrayList<Integer>> comm_nodes = new ArrayList<ArrayList<Integer>>(fin);
        for(int i=0;i < fin;i++) {
            comm_nodes.add(null);
        }
        n2c_new.getList().clear();


        for (int node = 0; node < size; node++) {

            if(comm_nodes.get(renumber.get(n2c.get(node))) == null) {
                comm_nodes.set(renumber.get(n2c.get(node)),new ArrayList<Integer>());
            }

            comm_nodes.get(renumber.get(n2c.get(node))).add(node);
            n2c_new.setRandom(node,renumber.get(n2c.get(node)));
        }

        // Compute weighted graph
        Graph g2 = new Graph();
        g2.setNb_nodes(comm_nodes.size());

        int comm_deg = comm_nodes.size();
        for (int comm = 0; comm < comm_deg; comm++) {
            HashMap<Integer,Double> m = new HashMap<Integer, Double>();

            int comm_size = comm_nodes.get(comm).size();
            for (int node = 0; node < comm_size; node++) {
                Pair<Integer, Integer> p = g.neighbors(comm_nodes.get(comm).get(node));
                long deg = g.nb_neighbors(comm_nodes.get(comm).get(node));
                for (int i = 0; i < deg; i++) {
                    int neigh = g.getLinks().getList().get(p.getElement0() + i);
                    int neigh_comm = renumber.get(n2c.get(neigh));
                    double neigh_weight = (g.getWeights().size() == 0) ? 1. : g.getWeights().getList().get(p.getElement1() + i);;


                    if (!m.containsKey(neigh_comm)) {
                        m.put(neigh_comm,neigh_weight);
                    } else {
                        m.put(neigh_comm,m.get(neigh_comm) + neigh_weight);
                    }
                }

                if (g.isContainRemote() && g.getRemoteEdges().containsKey(node)) {
                    HashMap.SimpleEntry<Vector<Integer>,Vector<Float>> p2 = g.remote_neighbors(comm_nodes.get(comm).get(node));
                    deg = g.nb_remote_neighbors(comm_nodes.get(comm).get(node));

                    for (int i = 0; i < deg; i++) {
                        int neigh =p2.getKey().getList().get(i);
                        int neigh_comm = renumber.get(n2c.get(neigh));
                        double neigh_weight = (g.getWeights().size() == 0)?1.:p2.getValue().getList().get(i);

                        if(m.containsKey(neigh_comm)) {
                            m.put(neigh_comm,m.get(neigh_comm) + neigh_weight);
                        } else {
                            m.put(neigh_comm,neigh_weight);
                        }
                    }
                }

            }

            g2.getDegrees().getList().add(comm, (comm == 0) ? m.size() : g2.getDegrees().getList().get(comm - 1) + m.size());
            g2.setNb_links(g2.getNb_links() + m.size());


            Iterator<Integer> it= m.keySet().iterator();

            while (it.hasNext()) {
                int key = it.next();
                double value = m.get(key);

                g2.setTotal_weight(g2.getTotal_weight() + value);
                g2.getLinks().getList().add(key);
                g2.getWeights().getList().add((float) value);
            }


        }
        g2.setRemoteMaps(g.getRemoteMaps());
        return g2;
    }

    public Graph partition2graph_binary_map(List<Pair<Integer, Integer>> map, List<Pair<Integer, Integer>> newMap) {
        return null;
    }

    // compute communities of the graph for one level
    // return true if some nodes have been moved
    public boolean one_level() {
        boolean improvement = false;
        int nb_moves;
        int nb_pass_done = 0;
        double new_mod = modularity();
        double cur_mod = new_mod;


        ArrayList<Integer> random_order = new ArrayList<Integer>(size);
        for (int i = 0; i < size; i++)
            random_order.add(i,i);

        Collections.shuffle(random_order);


        // repeat while
        //   there is an improvement of modularity
        //   or there is an improvement of modularity greater than a given epsilon
        //   or a predefined number of pass have been done
        do {
            cur_mod = new_mod;
            nb_moves = 0;
            nb_pass_done++;

            // for each node: remove the node from its community and insert it in the best community
            for (int node_tmp = 0; node_tmp < size; node_tmp++) {
                //      int node = node_tmp;
                int node = random_order.get(node_tmp);
                int node_comm = n2c.get(node);
                double w_degree = g.weighted_degree(node);
                if(g.isContainRemote()) {
                    w_degree += g.weighted_degree_wremote(node);
                }
                // computation of all neighboring communities of current node
                neigh_comm(node);
                // remove node from its current community
                remove(node, node_comm, neigh_weight.get(node_comm));

                // compute the nearest community for node
                // default choice for future insertion is the former community
                int best_comm = node_comm;
                double best_nblinks = 0.;
                double best_increase = 0.;
                for ( int i = 0; i < neigh_last; i++) {
                    double increase = modularity_gain(node, neigh_pos.get(i), neigh_weight.get(neigh_pos.get(i)), w_degree);
                    if (increase > best_increase) {
                        best_comm = neigh_pos.get(i);
                        best_nblinks = neigh_weight.get(neigh_pos.get(i));
                        best_increase = increase;
                    }
                }

                // insert node in the nearest community
                insert(node, best_comm, best_nblinks);

                if (best_comm != node_comm)
                    nb_moves++;
            }

//            double total_tot = 0.0;
//            double total_in = 0.0;
//            for (int i = 0; i < tot.size(); i++) {
//                total_tot += tot.get(i);
//                total_in += in.get(i);
//            }

            new_mod = modularity();
            if (nb_moves > 0)
                improvement = true;

        } while (nb_moves > 0 && Math.abs(new_mod - cur_mod) > min_modularity);

        return improvement;
    }


    public Graph getG() {
        return g;
    }

    public int getSize() {
        return size;
    }

    public Vector<Integer> getN2c_new() {
        return n2c_new;
    }
}
