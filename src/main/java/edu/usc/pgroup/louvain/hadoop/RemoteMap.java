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

import java.io.Serializable;

/**
 * Created by Charith Wickramaarachchi on 7/17/14.
 */
class RemoteMap implements Serializable {

    int source;

    int sink;

    int sinkPart;

    public RemoteMap(int source, int sink, int sinkPart) {
        this.source = source;
        this.sink = sink;
        this.sinkPart = sinkPart;
    }

    public int getSource() {
        return source;
    }

    public int getSink() {
        return sink;
    }

    public int getSinkPart() {
        return sinkPart;
    }
}