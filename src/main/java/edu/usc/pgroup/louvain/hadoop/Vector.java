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
 * Created by Charith Wickramaarachchi on 7/10/14.
 */
public class Vector<T> {

    private ArrayList<T> list;

    public Vector() {
        list = new ArrayList<T>();
    }

    public Vector(int initialCapacity) {
        list = new ArrayList<T>(initialCapacity);
        for(int i=0;i<initialCapacity;i++) {
            list.add(null);
        }
    }


    public ArrayList<T> getList() {
        return list;
    }

    public void setRandom(int index, T t) {
        if (index < list.size()) {
            list.set(index, t);
        } else {
            while(index >= list.size()) {
                list.add(null);
            }
            list.set(index,t);
        }
    } 
    public int size() {
        return list.size();
    }

}
