
package com.google.appengine.demos.mapreduce.entitycount;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.tools.mapreduce.DatastoreMutationPool;
import com.google.appengine.tools.mapreduce.Mapper;

import java.util.Random;
import java.io.*;
import java.net.URL;

import javax.servlet.ServletContext;

/**
 * Reads hotel reviews and creates corresponding cloud storage entities.
 * 
 * @author aruokone
 */
class EntityCreator extends Mapper<Long, Void, Void> {

	private static final long serialVersionUID = 409204195454478863L;
	private static int counter = 0;

	private final String kind;
	private final int payloadBytesPerEntity;
	private final Random random = new Random();
	// [START datastoreMutationPool]
	private transient DatastoreMutationPool pool;

	// [END datastoreMutationPool]

	public EntityCreator(String kind, int payloadBytesPerEntity) {
		this.kind = checkNotNull(kind, "Null kind");
		this.payloadBytesPerEntity = payloadBytesPerEntity;
	}

	private String randomString(int length) {
		StringBuilder out = new StringBuilder(length);

		for (int i = 0; i < length; i++) {
			if (random.nextInt(10) % 3 == 0) {
				out.append("wow");
				out.append((char) (' '));
			} else {
				out.append((char) ('a' + random.nextInt(26)));
				out.append((char) (' '));

			}
		}
		return out.toString();
	}

	// [START begin_and_endSlice]
	@Override
	public void beginSlice() {
		pool = DatastoreMutationPool.create();
	}

	@Override
	public void endSlice() {
		pool.flush();
	}

	// [END begin_and_endSlice]

	@Override
	public void map(Long ignored) {
		
		 String name = "hotel" + counter % 4; counter++;
		 
		 Entity entity = new Entity(kind, name); entity.setProperty("payload",
		 new Text(randomString(payloadBytesPerEntity))); pool.put(entity);

	}
}
