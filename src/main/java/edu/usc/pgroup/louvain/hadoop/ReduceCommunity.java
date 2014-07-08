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

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Charith Wickramaarachchi on 6/30/14.
 */
public class ReduceCommunity extends Reducer<Text, BytesWritable, Text, Text> {

    private double precision = 0.000001;
    private boolean verbose;
    int display_level = -1;
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {

        Graph g = reconstructGraph(values);

        Community c = new Community(g,-1,precision);

        double mod = c.modularity();

        boolean improvement = true;


        if (verbose)
            System.out.print(" new modularity : " + mod);


        int level =2;

        if (verbose) {
            System.out.print( "level " + level );
            System.out.print("  start computation");
            System.out.print( "  network size: "
                    + c.getG().getNb_nodes() +  " nodes, "
                    + c.getG().getNb_links() + " links, "
                    + c.getG().getTotal_weight() + " weight." );
        }


        improvement = c.one_level();
        double new_mod = c.modularity();

        if (++level == display_level)
            g.display();
        if (display_level == -1)
            c.display_partition();

        Graph g2 = c.partition2graph_binary();

        if(verbose) {
            System.out.print( "  network size: "
                    + c.getG().getNb_nodes() +  " nodes, "
                    + c.getG().getNb_links() + " links, "
                    + c.getG().getTotal_weight() + " weight." );
        }


        c = new Community(g2,-1,precision);
        if (verbose)
            System.out.println("  modularity increased from " +  mod + " to " + new_mod);

        mod = new_mod;

        do{


            if (verbose) {
                System.out.print( "level in loop" + level );
                System.out.print("  start computation");
                System.out.print( "  network size: "
                        + c.getG().getNb_nodes() +  " nodes, "
                        + c.getG().getNb_links() + " links, "
                        + c.getG().getTotal_weight() + " weight." );
            }

            improvement = c.one_level();
            new_mod = c.modularity();

            if (++level == display_level)
                g.display();
            if (display_level == -1)
                c.display_partition();

            g2 = c.partition2graph_binary();

            if(verbose) {
                System.out.print( "  network size: "
                        + c.getG().getNb_nodes() +  " nodes, "
                        + c.getG().getNb_links() + " links, "
                        + c.getG().getTotal_weight() + " weight." );
            }


            c = new Community(g2,-1,precision);


            if (verbose)
                System.out.println("  modularity increased from " +  mod + " to " + new_mod);

            mod = new_mod;


        } while (improvement);


        System.out.println( " Final modularity : " + new_mod);
    }


    private Graph reconstructGraph(Iterable<BytesWritable> values) {

        return null;

    }
}
