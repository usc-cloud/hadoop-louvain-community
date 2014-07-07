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

import java.util.ArrayList;

/**
 * Created by Charith Wickramaarachchi on 7/7/14.
 */
public class Test {

    public static void main(String[] args) {
        ArrayList<Integer> list = new ArrayList<Integer>(6);


        for(int i =0; i < 6;i++) {
            list.add(null);
        }
        int array[] = {2,3,1,4,5,0};

        for(int i : array) {
            list.set(i,i);
        }
        System.out.println("Size: " + list.size());
        for(int i : list) {
            System.out.println(i);
        }


    }
}
