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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.zebra.mapreduce.BasicTableOutputFormat;
import org.apache.hadoop.zebra.pig.TableStorer;
import org.apache.pig.data.Tuple;

public class RDFPigStorer extends TableStorer {

	public class MyTableOutputFormat extends BasicTableOutputFormat {

		// Wrapper around the Zebra record reader
		public class MyTableRecordReader extends
				RecordWriter<BytesWritable, Tuple> {

			RecordWriter<BytesWritable, Tuple> realRecordWriter = null;
			long count = 0;

			public MyTableRecordReader(
					RecordWriter<BytesWritable, Tuple> realRecordWriter) {
				this.realRecordWriter = realRecordWriter;
			}

			@Override
			public void close(TaskAttemptContext arg0) throws IOException,
					InterruptedException {
				realRecordWriter.close(arg0);

				Path[] paths = BasicTableOutputFormat.getOutputPaths(arg0);
				Path output = new Path(paths[0], "_count-"
						+ arg0.getTaskAttemptID().getTaskID().getId());
				FileSystem fs = output.getFileSystem(arg0.getConfiguration());
				FSDataOutputStream fout = fs.create(output);
				fout.writeLong(count);
				fout.close();
			}

			@Override
			public void write(BytesWritable arg0, Tuple arg1)
					throws IOException, InterruptedException {
				realRecordWriter.write(arg0, arg1);
				count++;
			}

		}

		@Override
		public RecordWriter<BytesWritable, Tuple> getRecordWriter(
				TaskAttemptContext taContext) throws IOException {
			RecordWriter<BytesWritable, Tuple> realRecordWriter = super
					.getRecordWriter(taContext);
			return new MyTableRecordReader(realRecordWriter);
		}

	}

	@SuppressWarnings("rawtypes")
	@Override
	public OutputFormat getOutputFormat() throws IOException {
		return new MyTableOutputFormat();
	}

	public RDFPigStorer() {
	}

	public RDFPigStorer(String storageHintString) {
		super(storageHintString);
	}

	public RDFPigStorer(String storageHintString, String partitionClassString) {
		super(storageHintString, partitionClassString);
	}

	public RDFPigStorer(String storageHintString, String partitionClassString,
			String partitionClassArgumentsString) {
		super(storageHintString, partitionClassString,
				partitionClassArgumentsString);
	}
}
