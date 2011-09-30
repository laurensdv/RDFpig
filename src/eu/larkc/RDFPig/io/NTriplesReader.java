/*
 * Copyright 2011 LarKC project consortium
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package eu.larkc.RDFPig.io;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;

public class NTriplesReader extends LoadFunc {

	protected static Logger log = LoggerFactory.getLogger(NTriplesReader.class);

    @SuppressWarnings("rawtypes")
	protected RecordReader in = null;
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    String location;

    @Override
    public Tuple getNext() throws IOException {

    	while (true) {
			mProtoTuple=null;
   	        try {
	            boolean notDone = in.nextKeyValue();
	            if (!notDone) {
	                return null;
	            }
	            Text value = null;
	            try {
					value = (Text) in.getCurrentValue();

					byte[] buf = value.getBytes();
					int len = value.getLength();

					if (len<3)
						continue; // Ignore lines with less than 3 bytes

					//Get rid of any trailing whitespace
					while (Character.isWhitespace(buf[len-1]))
						len--;

					if (buf[len-1]!='.')
						continue;//throw new ExecException("Could not parse triple, no trailing \'.\': " + value);
					else
						len--;

					 //Get rid of any trailing whitespace
					while (Character.isWhitespace(buf[len-1]))
						len--;


					int start = 0;
					while(Character.isWhitespace(buf[start]))
						start++;

					// Parse subject
					boolean isURI=buf[0]=='<';
					for (int i = 0; i < len; i++) {
					    if (isURI && buf[i] == '>') {
					        readField(buf, start, i+1);
					        start = i + 1;
					        break;
					    }
					    else if (Character.isWhitespace(buf[i])) {
					    	 readField(buf, start, i);
					         start = i + 1;
					         break;
					    }
					}

					while(Character.isWhitespace(buf[start]))
							start++;

					// Parse predicate (always URI)
					for (int i = start; i < len; i++) {
					    if ( buf[i] == '>') {
					        readField(buf, start, i+1);
					        start = i + 1;
					        break;
					    }
					}

					while(Character.isWhitespace(buf[start]))
						start++;

					// Parse object
					if (buf[start]=='<') //URI
						for (int i = start+1; i < len; i++) {
					        if (buf[i] == '>') {
					            readField(buf, start, i+1);
					            start = i + 1;
					            break;
					        }
						}
					else if (buf[start]=='"')  //Literal
						for (int i = start+1; i < len; i++)  {
							if (buf[i] == '"' && i>0 && buf[i-1] != '\\') {
					            readField(buf, start, i+1);
					            start = i + 1;
					            break;
					        }
						}
					else if (buf[start]=='_') {//BNode
						int i = start+1;
					    for (; i < len; i++) {
					       if (Character.isWhitespace(buf[i])) {
					        	 readField(buf, start, i);
					             start = i + 1;
					             break;
					        }
					    }
					    // We are at end of line, read it
					    readField(buf,start,i);

					}
					else
						continue;//throw new ExecException("Could not parse triple, invalid term in object position: " + value);
					// After the first three terms, the rest are ignored

					if (mProtoTuple.size()!=3)
						continue;

					Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);
					mProtoTuple = null;
					return t;
				} catch (Exception e) {
					e.printStackTrace();
					System.err.println("For line: " + value);
					mProtoTuple=null;
				}
	        } catch (Exception e) {
	            int errCode = 6018;
	            String errMsg = "Error while reading input";
	            throw new ExecException(errMsg, errCode,
	                    PigException.REMOTE_ENVIRONMENT, e);
	        }
    	}
    }

    private void readField(byte[] buf, int start, int end) {
        if (mProtoTuple == null) {
            mProtoTuple = new ArrayList<Object>();
        }

        if (start == end) {
            // NULL value
            mProtoTuple.add(null);
        } else {
            mProtoTuple.add(new DataByteArray(buf, start, end));
        }
    }

    @SuppressWarnings("rawtypes")
	@Override
    public InputFormat getInputFormat() {
    	if(location.endsWith(".bz2") || location.endsWith(".bz")) {
            return new Bzip2TextInputFormat();
        } else if (location.endsWith(".lzo")) {
        	return new LzoTextInputFormat();
        } else {
            return new PigTextInputFormat();
        }
    }

    @Override
	@SuppressWarnings("rawtypes")
	public void prepareToRead(RecordReader reader, PigSplit split) {
        in = reader;
    }

    @Override
    public void setLocation(String location, Job job)
            throws IOException {
    	this.location = location;
        FileInputFormat.setInputPaths(job, location);
    }
}