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

import com.sun.corba.se.spi.orbutil.fsm.Input;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by Charith Wickramaarachchi on 6/30/14.
 */
public class MapCommunity extends Mapper<Text, BytesWritable, Text, BytesWritable> {

    private boolean verbose = false;
    private int nb_pass = 0;
    private double precision = 0.000001;

    private int display_level =-1;
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        /**
         * FileFormat
         *
         * Metis format
         * ******
         * Remote
         */

        InputStream inputStream = new ByteArrayInputStream(value.getBytes());

        int rank = 0;

        if(verbose) {
            System.out.println("Begin");
        }

        try {
            Community c = new Community(inputStream,-1,nb_pass,precision);
            Graph g = new Graph();
            boolean improvement = true;
            double mod = c.modularity(), new_mod;
            int level = 0;

            if (verbose) {
                System.out.print("" + rank  + ":" +  "level " + level );
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

            g = c.partition2graph_binary();

            if(verbose) {
                System.out.print( "  network size: "
                        + c.getG().getNb_nodes() +  " nodes, "
                        + c.getG().getNb_links() + " links, "
                        + c.getG().getTotal_weight() + " weight." );
            }



            // Renumber

            //Send to reducer



        } catch (Exception e) {
            e.printStackTrace();
            throw new InterruptedException(e.toString());
        }

    }
}
