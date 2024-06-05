package zio.internal;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

final class Mailbox<A> {

	private final int grow;

	private transient volatile int insert;
	private transient volatile Segment write;
	private transient Segment read;
	private transient int index;

	public Mailbox(int step) {
		this(step, 1);
	}

	public Mailbox(int step, int grow) {
		this.grow = grow;
		this.read = this.write = new Segment(null, step, 0);
	}

	public void add(A data) {
		if (null == data)
			throw new NullPointerException();

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
		return read.start + index == insert;
	}

	public boolean nonEmpty() {
		return read.start + index < insert;
	}

	public A poll() {

		final int step = read.length();
		int index = this.index;

		if (read.start + index == insert) {
			// queue is empty
			return null;
		}

		if (index == step) {
			Segment next;
			// busy wait for queue expansion
			while (null == (next = read.next)) {
			}

			// drop segment
			read = next;
			index = 0;
		}

		Object data;
		// busy wait for data synchronization
		while (null == (data = read.get(index))) {
		}

		// complete read
		read.lazySet(index, null);
		this.index = index + 1;

		return (A) (data);
	}

	private static class Segment extends AtomicReferenceArray<Object> {

		final Segment prev;
		final int start;

		volatile Segment next;

		Segment(Segment prev, int size, int start) {
			super(size);
			this.prev = prev;
			this.start = start;
		}

		static final AtomicReferenceFieldUpdater<Segment, Segment> NEXT = AtomicReferenceFieldUpdater
				.newUpdater(Segment.class, Segment.class, "next");
	}

	private static final AtomicReferenceFieldUpdater<Mailbox, Segment> WRITE = AtomicReferenceFieldUpdater
			.newUpdater(Mailbox.class, Segment.class, "write");
	private static final AtomicIntegerFieldUpdater<Mailbox> INSERT = AtomicIntegerFieldUpdater.newUpdater(Mailbox.class,
			"insert");
}
