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

import javafx.util.*;
import javafx.util.Pair;

import java.io.InputStream;
import java.util.List;

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

    private List<Integer> n2c_new;

    // number of pass for one level computation
    // if -1, compute as many pass as needed to increase modularity
    private int nb_pass;

    // a new pass is computed if the last one has generated an increase
    // greater than min_modularity
    // if 0. even a minor increase is enough to go for one more pass
    private double min_modularity;

    public Community(InputStream inputStream,int type,int nb_pass,double min_modularity) {


    }

    public Community(Graph g, int nb_pass, double min_modularity){

    }

    public void remove(int node, int comm, double dnodecomm) {

    }

    public void insert(int node, int comm, double dnodecomm) {

    }

    public double modularity_gain(int node, int comm, double dnodecomm, double w_degree){
        return 0.0;
    }

    public void neigh_comm( int node) {

    }

    public double modularity() {
        return 0.0;
    }

    // displays the graph of communities as computed by one_level
    public void partition2graph() {

    }
    // displays the current partition (with communities renumbered from 0 to k-1)
    public void display_partition() {

    }

    // generates the binary graph of communities as computed by one_level
    public Graph partition2graph_binary(){
        return null;
    }

    public Graph partition2graph_binary_map(List<Pair<Integer, Integer>> map, List<Pair<Integer, Integer>> newMap) {
        return null;
    }

    // compute communities of the graph for one level
    // return true if some nodes have been moved
    public boolean one_level() {
        return false;
    }


}
