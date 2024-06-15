package zio.internal;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

abstract class MailboxClassPad implements Serializable {
	protected int _0;
	protected long _1;
	protected long _2;
	protected long _3;
	protected long _4;
	protected long _5;
	protected long _6;
	protected long _7;
	protected long _8;
	protected long _9;
	protected long _a;
	protected long _b;
	protected long _c;
	protected long _d;
	protected long _e;
}

abstract class MailboxWrite extends MailboxClassPad {
	protected transient volatile Mailbox.Node write;
}

abstract class MailboxWritePad extends MailboxWrite {
	protected int __0;
	protected long __1;
	protected long __2;
	protected long __3;
	protected long __4;
	protected long __5;
	protected long __6;
	protected long __7;
	protected long __8;
	protected long __9;
	protected long __a;
	protected long __b;
	protected long __c;
	protected long __d;
	protected long __e;
	protected long __f;
}

public final class Mailbox<A> extends MailboxWritePad {

	private transient Node read;

	public Mailbox() {
		read = write = new Node(null);
	}

	public void add(A data) {
		final Node next = new Node(data);
		NEXT.setRelease(WRITE.getAndSet(this, next), next);
	}

	public boolean isEmpty() {
		return null == NEXT.getAcquire(read);
	}

	public boolean nonEmpty() {
		return null != NEXT.getAcquire(read);
	}

	@SuppressWarnings("unchecked")
	public A poll() {
		Node next = (Node) (NEXT.getAcquire(read));

		if (next == null)
			return null;

		read = next;
		final Object data = next.data;
		next.data = null;
		return (A) (data);
	}

	static class Node implements Serializable {

		Object data;
		volatile Node next;

		Node(Object data) {
			this.data = data;
		}

		Node(Object data, Node next) {
			this.data = data;
			this.next = next;
		}
	}

	static final VarHandle NEXT;
	static final VarHandle WRITE;

	static {
		try {
			MethodHandles.Lookup lookup = MethodHandles.lookup();
			NEXT = MethodHandles.privateLookupIn(Node.class, lookup).findVarHandle(Node.class, "next", Node.class);
			WRITE = MethodHandles.privateLookupIn(MailboxWrite.class, lookup).findVarHandle(MailboxWrite.class, "write",
					Node.class);
		} catch (ReflectiveOperationException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
}
