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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;

/**
 * Created by Charith Wickramaarachchi on 6/30/14.
 */
public class MapCommunity extends Mapper<Text, BytesWritable, Text, BytesWritable> {

    private boolean verbose = false;
    private int nb_pass = 0;
    private double precision = 0.000001;

    private int display_level =-1;

    private String outpath;



    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        /**
         * FileFormat
         *
         * Metis format
         * ******
         * Remote
         */

        //example: 4elt_0.part

        String fileName = key.toString();

        String _parts[] = fileName.split("_");

        String dotParts[] = _parts[1].split("\\.");

        InputStream inputStream = new ByteArrayInputStream(value.getBytes());
        int rank = Integer.parseInt(dotParts[0]);

        if(verbose) {
            System.out.println("Begin");
        }

        try {
            Community c = new Community(inputStream,-1,nb_pass,precision);
            Graph g =null;
            boolean improvement = true;
            double mod = c.modularity(), new_mod;
            int level = 0;

            if (verbose) {
                System.out.print("" + rank  + ":" +  "level " + level );
                System.out.print("  start computation");
                System.out.println( "  network size: "
                        + c.getG().getNb_nodes() +  " nodes, "
                        + c.getG().getNb_links() + " links, "
                        + c.getG().getTotal_weight() + " weight." );
            }





            improvement = c.one_level();
            new_mod = c.modularity();

            if (++level == display_level)
                g.display();
            if (display_level == -1){
                String filepath = outpath + File.separator + "out_" + level + "_" + rank + ".txt";
                c.display_partition(filepath);
            }
            g = c.partition2graph_binary();

            if(verbose) {
                System.out.println( "  network size: "
                        + c.getG().getNb_nodes() +  " nodes, "
                        + c.getG().getNb_links() + " links, "
                        + c.getG().getTotal_weight() + " weight." );
            }



            GraphMessage msg = createGraphMessage(g,c,rank);


            //Send to reducer

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oo = new ObjectOutputStream(bos);
            oo.writeObject(msg);
            context.write(new Text("one"),new BytesWritable(bos.toByteArray()));


        } catch (Exception e) {
            e.printStackTrace();
            throw new InterruptedException(e.toString());
        }


    }


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        verbose = configuration.getBoolean(LouvainMR.VERBOSE,false);
        nb_pass = configuration.getInt(LouvainMR.NB_PASS, 0);
        precision = configuration.getDouble(LouvainMR.PRECISION, 0.000001);
        display_level = configuration.getInt(LouvainMR.DISPLAY_LEVEL, -1);
        outpath = configuration.get(LouvainMR.OUT_PATH);

        System.out.println("verbose = " + verbose);
        System.out.println("display_level = " + display_level);
        System.out.println("outpath = " + outpath);


        super.setup(context);

    }

    private GraphMessage createGraphMessage(Graph g, Community c, int partitionId) {

        GraphMessage msg = new GraphMessage();

        msg.setNb_links(g.getNb_links());
        msg.setNb_nodes(g.getNb_nodes());
        msg.setTotal_weight(g.getTotal_weight());
        msg.setLinks(g.getLinks().getList().toArray(new Integer[0]));
        msg.setDegrees(g.getDegrees().getList().toArray(new Long[0]));
        msg.setWeights(g.getWeights().getList().toArray(new Float[0]));
        msg.setRemoteMap(g.getRemoteMaps().getList().toArray(new RemoteMap[0]));
        msg.setN2c(c.getN2c_new().getList().toArray(new Integer[0]));

        msg.setCurrentPartition(partitionId);
        return msg;
    }

}
