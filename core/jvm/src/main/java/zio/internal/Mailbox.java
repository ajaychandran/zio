package zio.internal;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

final public class Mailbox<A> implements Serializable {

	private final int grow;

	private transient volatile int insert;
	private transient volatile Segment write;
	private transient Segment read;

	public Mailbox() {
		this(4, 1);
	}

	public Mailbox(int step, int grow) {
		this.grow = grow;
		this.read = this.write = new Segment(null, step, 0);
	}

	public void add(A data) {
		assert (data != null);

		// load instance fields locally to prevent reload after sync
		final AtomicReferenceFieldUpdater<Segment, Segment> NEXT = Segment.NEXT;
		final AtomicReferenceFieldUpdater<Mailbox, Segment> WRITE = Mailbox.WRITE;
		final int grow = this.grow;

		Segment write = this.write;
		final int step = write.length();

		// acquire and increment global index
		final int insert = INSERT.getAndAdd(this, 1);

		// fast path
		if (insert < step) {
			// global first segment
			write.lazySet(insert, data);
			return;
		}

		var canGrow = true;
		int start, index;
		Segment next;

		// locate segment
		do {

			start = write.start;

			while (insert < start) {
				// segment is in the back
				canGrow = false;
				write = write.prev;
				start = write.start;
			}

			index = insert - start;

			if (index < step) {
				// segment located
				write.lazySet(index, data);
				if (index == grow && canGrow) {
					// expand eagerly
					next = new Segment(write, step, start + step);
					NEXT.compareAndSet(write, null, next);
				}
				return;
			}

			next = write.next;
			if (null == next) {
				// expand
				next = new Segment(write, step, start + step);
				if (NEXT.compareAndSet(write, null, next)) {
					WRITE.lazySet(this, next);
				}
			} else {
				// next segment may be stale
				WRITE.compareAndSet(this, write, next);
			}

			// reload post expansion
			write = this.write;

		} while (true);
	}

	public boolean isEmpty() {
		Segment read = this.read;
		int index = read.index;
		final int start = read.start;

		return start + index == this.insert;
	}

	public boolean nonEmpty() {
		Segment read = this.read;
		int index = read.index;
		final int start = read.start;

		return start + index < this.insert;
	}

	public A poll() {

		// load instance fields locally to prevent reload after sync
		final Object READ = Mailbox.READ;

		Segment read = this.read;
		int index = read.index;
		final int start = read.start;
		final int step = read.length();

		// acquire global index
		final int insert = this.insert;

		if (start + index == insert) {
			// queue is empty
			return null;
		}

		boolean canDrop = true;
		A data;
		Segment next;

		do {

			for (; index < step; index += 1) {
				data = (A) (read.get(index));

				if (null == data) {
					// element not inserted yet
					canDrop = false;
					continue;
				}

				if (canDrop) {
					// elements upto index were read
					this.read.index = index + 1;
				}

				if (READ != data) {
					// release element
					read.lazySet(index, READ);
					return data;
				}
			}

			// acquire next segment
			next = read.next;

			if (null == next) {
				// end of queue
				return null;
			}

			read = next;
			index = read.index;

			if (canDrop) {
				// drop segment
				this.read = read;
			}

		} while (true);
	}

	private static class Segment extends AtomicReferenceArray<Object> {

		final Segment prev;
		final int start;
		int index;

		volatile Segment next;

		Segment(Segment prev, int size, int start) {
			super(size);
			this.prev = prev;
			this.start = start;
		}

		static final AtomicReferenceFieldUpdater<Segment, Segment> NEXT = AtomicReferenceFieldUpdater
				.newUpdater(Segment.class, Segment.class, "next");
	}

	private static final Object READ = new Object();

	private static final AtomicReferenceFieldUpdater<Mailbox, Segment> WRITE = AtomicReferenceFieldUpdater
			.newUpdater(Mailbox.class, Segment.class, "write");
	private static final AtomicIntegerFieldUpdater<Mailbox> INSERT = AtomicIntegerFieldUpdater.newUpdater(Mailbox.class,
			"insert");
}
