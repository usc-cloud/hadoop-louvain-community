package edu.usc.pgroup.louvain.hadoop;

import org.apache.hadoop.mapred.FileInputFormat;

import java.io.*;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by charith on 6/25/14.
 */
public class Graph {


    public static final  int WEIGHTED  = 0;
    public static final int UNWEIGHTED  = 1;



    private int nb_nodes;
    private long nb_links;
    private double total_weight;

    private List<Long> degrees;
    private List<Integer> links;
    private List<Float> weights;



    Graph(){
        nb_nodes     = 0;
        nb_links     = 0;
        total_weight = 0;
    }



    Graph(InputStream inputStream, int type) throws Exception{
        // Assume metis undirected format and unweighted graph.

        BufferedReader fileReader = new BufferedReader(new InputStreamReader(inputStream));

        String line = fileReader.readLine();


        int nodeId = 0;
        while (line != null) {
            StringTokenizer tokenizer = new StringTokenizer(line);
            int count = 0;
            while (tokenizer.hasMoreTokens()) {
                int e = Integer.parseInt(tokenizer.nextToken());
                links.add(e);
                count++;
            }

            if(nodeId == 0) {
                degrees.add(nodeId,(long)count);
            } else {
                degrees.add(nodeId,(long)(degrees.get(nodeId -1) + count));
            }

            nodeId++;
            line = fileReader.readLine();
        }
        this.nb_nodes = nodeId;


    }

    Graph(String fileName,int type) throws Exception {

        this(new FileInputStream(fileName), type);

    }

    Graph(int nb_nodes, int nb_links, double total_weight, List<Integer> degrees, List<Integer> links, List<Float> weights) {

    }


    void display() {

    }
    void display_reverse(){

    }

    void display_binary(String outfile){

    }


    boolean check_symmetry() {
        return false;
    }


    // return the number of neighbors (degree) of the node
    public long nb_neighbors(int node) {
        assert(node>=0 && node<nb_nodes);

        if (node==0)
            return degrees.get(0);
        else
            return (degrees.get(node)-degrees.get(node-1));

    }

    // return the number of self loops of the node
    public double nb_selfloops(int node){
        assert(node>=0 && node<nb_nodes);

        Pair<Integer, Integer> p = neighbors(node);
        for (int i=0 ; i < nb_neighbors(node) ; i++) {
            if (links.get(p.getElement0() + i) ==node) {
                if (weights.size()!=0)
                    return (double)weights.get(p.getElement1()+ i);
                else
                return 1.0;
            }
        }
        return 0.0;
    }

    // return the weighted degree of the node
    public double weighted_degree(int node){
        assert(node>=0 && node<nb_nodes);

        if (weights.size()==0)
            return (double)nb_neighbors(node);
        else {
            Pair<Integer, Integer> p = neighbors(node);
            double res = 0;
            for ( int i=0 ; i<nb_neighbors(node) ; i++) {
                res += (double)weights.get(p.getElement1()+i);
            }
            return res;
        }
    }

    // return pointers to the first neighbor and first weight of the node
    public Pair<Integer, Integer> neighbors(int node){
        if (node==0)
            return new Pair<Integer, Integer>(0,0);
        else if (weights.size()!=0)
            return new Pair(degrees.get(node-1),degrees.get(node-1));
        else
            return new Pair(degrees.get(node-1),0);
    }



}
