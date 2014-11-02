/** Reliable Data Transport class.
 *
 *  This class implements a reliable data transport service.
 *  It uses a go-back-N sliding window protocol on a packet basis.
 *
 *  An application layer thread provides new packet payloads to be
 *  sent using the provided send() method, and retrieves newly arrived
 *  payloads with the receive() method. Each application layer payload
 *  is sent as a separate UDP packet, along with a sequence number and
 *  a type flag that identifies a packet as a data packet or an
 *  acknowledgment. The sequence numbers are 15 bits.
 */

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Rdt implements Runnable {
	private int wSize;	// protocol window size
	private long timeout;	// retransmission timeout in ns
	private Substrate sub;	// Substrate object for packet IO

	private ArrayBlockingQueue<String> fromSrc;
	private ArrayBlockingQueue<String> toSnk;

	// Sending structures and necessary information
	private Packet[] sendBuf; // not yet acked packets
	private Long[] resendTime; // their resend times 	
	private short sendBase = 0;	// seq# of first packet in send window
	private short sendSeqNum = 0;	// next seq# after send window
	private short dupAcks = 0; // should only happen for sendBase-1 packet
	private LinkedList<Short> resendList;	// their seq#s, in order of resend times

	// Receiving structures and necessary information
	private Packet[] recvBuf;	 // undelivered packets
	private short recvBase = 0;  // seq# of oldest undelivered packet [to app]
	private short expSeqNum = 0; // seq# of packet expted to receive [from sub]
	private short lastRcvd = -1; // last packet received properly

	// Time keeping variabels
	private long now = 0; //current time, relative to t0
	private long sendAgain = 0;	// time when oldest un-acked packet gets sent again

	private Thread myThread;
	private boolean quit;

	/** Initialize a new Rdt object.
	 *  @param wSize is the window size used by protocol; the sequence #
	 *  space is twice the window size
	 *  @param timeout is the time to wait before retransmitting
	 *  @param sub is a reference to the Substrate object that this object
	 *  uses to handle the socket IO
	 */
	Rdt(int wSize, double timeout, Substrate sub) 
	{
		this.wSize = Math.min(wSize,(1 << 14) - 1);
		this.timeout = ((long) (timeout * 1000000000)); // sec to ns
		this.sub = sub;

		// create queues for application layer interface
		fromSrc = new ArrayBlockingQueue<String>(1000,true);
		toSnk = new ArrayBlockingQueue<String>(1000,true);
		quit = false;

		sendBuf = new Packet[2*wSize];
		resendList = new LinkedList<Short>();	
		resendTime = new Long[2*wSize];
		recvBuf = new Packet[2*wSize];
	}

	/** Start the Rdt running. */
	public void start() throws Exception {
		myThread = new Thread(this); myThread.start();
	}

	/** Stop the Rdt.  */
	public void stop() throws Exception { quit = true; myThread.join(); }

	/** Increment sequence number, handling wrap-around.
	 *  @param x is a sequence number
	 *  @return next sequence number after x
	 */
	private short incr(short x) {
		x++; 
		return (x < 2*wSize ? x : 0);
	}

	/** Compute the difference between two sequence numbers,
	 *  accounting for "wrap-around"
	 *  @param x is a sequence number
	 *  @param y is another sequence number
	 *  @return difference, assuming x is "clockwise" from y
	 */
	private int diff(short x, short y) {
		return (x >= y ? x-y : (x + 2*wSize) - y);
	}

	/** Main thread for the Rdt object.
	 *
	 *  Inserts payloads received from the application layer into
	 *  packets, and sends them to the substrate. The packets include
	 *  the number of packets and chars sent so far (including the
	 *  current packet). It also takes packets received from
	 *  the substrate and sends the extracted payloads
	 *  up to the application layer. To ensure that packets are
	 *  delivered reliably and in-order, using a sliding
	 *  window protocol with the go-back-N feature.
	 */
	public void run() {
		//System.out.println("Enter run()");
		long t0 = System.nanoTime();
		long now = 0;		// current time (relative to t0)

		while (!quit || resendList.size() != 0) {
			////System.out.println("In while loop");

			now = System.nanoTime() - t0;
			sendAgain = now + timeout;
			Packet p = new Packet();

			// TODO
			// if receive buffer has a packet that can be
			//    delivered, deliver it to sink
			if (recvBuf[recvBase] !=  null) { 
				//p = recvBuf[recvBase];
				p.type = recvBuf[recvBase].type;
				p.seqNum = recvBuf[recvBase].seqNum;
				p.payload = recvBuf[recvBase].payload;
				toSnk.add(p.payload);
				recvBuf[recvBase] = null;
				recvBase = incr(recvBase);
				System.out.println("packet " + p.seqNum + " sent to sink");
			}
			// else if the substrate has an incoming packet
			//      get the packet from the substrate and process it
			else if (sub.incoming()) {
				//System.out.println("Incoming packet from sub");
				p = sub.receive();
				
				System.out.println("p.seqNum: " + p.seqNum + ", p.payload: " + 
					p.payload + ", p.type: " + p.type);

				// 	if it's a data packet
				if (p.type == 0) {
					//System.out.println("Received data packet");
					//if expected packet, add to recv buffer and update info
					if (p.seqNum == expSeqNum) {
						recvBuf[recvBase] = p;
						// recvBuf[recvBase].type = p.type;
						// recvBuf[recvBase].seqNum = p.seqNum;
						// recvBuf[recvBase].payload = "";
						System.out.println("p.seqNum: " + p.seqNum);
						System.out.println("recvBuf[recvBase]: " + recvBuf[recvBase].seqNum);
						recvBase = incr(recvBase);
						System.out.println("expSeqNum: " + expSeqNum);
						expSeqNum = incr(expSeqNum);
						System.out.println("expSeqNum after incr: " + expSeqNum);
						lastRcvd = incr(lastRcvd);
					}
					//send ack back to sub
					Packet ack = new Packet();
					ack.type = 1;
					ack.seqNum = lastRcvd;
					sub.send(ack);
					//System.out.println("Sent ack packet");
				}

				//if ack
				else if (now < sendAgain) {
				//System.out.println("Received ack packet");

					//while (now < sendAgain) {
					if (p.seqNum == expSeqNum) {	
						//System.out.println("Received expected ack packet");
	
						resendList.remove(); //remove oldest (first) entry from resentList	
						//running into nullptr exception (probably when there are no more packets to send)
						//what do I set resendTime[sendBase] to?  0? now?
						resendTime[sendBase] = null; //essentially clear oldest entry from resentTime array
						sendBuf[sendBase] = null; //essentially remove received packet from unacked sent packet array
						sendBase = incr(sendBase);
						sendSeqNum = incr(sendSeqNum);	
						dupAcks = 0;
						// if (resendTime[sendBase] != null) {
						// 	sendAgain = resendTime[sendBase]; //set to oldest packet's resentTime
						// }
						// else {
						// 	sendAgain = now + timeout;
						// }
						sendAgain = resendTime[sendBase];
					}

					//not expected seq num, but within window
					if ((p.seqNum != expSeqNum) && (diff(p.seqNum,sendBase) < wSize)) {
						//assume all packets between expSeqNum and p.seqNum were correctly received
						//process conents of the first "if" statement x+1 times
						//where x+1 = diff(p.seqNum, expSeqNum)
						//System.out.println("Received unexpected ack packet but in window");

						int numUpdates = diff(p.seqNum,expSeqNum) + 1;
						for (int x = 1; x <=numUpdates; ++x) {
							resendList.remove();
							resendTime[sendBase] = 0; 
							sendBuf[sendBase] = null;
							sendBase = incr(sendBase);
							sendSeqNum = incr(sendSeqNum);
							sendAgain = resendTime[sendBase];
							dupAcks = 0;						
						}
					}
					//if seq num == sendBase-1 (with handled wrap around)
					if (p.seqNum == diff(sendBase, (short)(wSize+1))) {
						//System.out.println("Received duplicate ack packet");
					
						dupAcks++;
						//resend immediately (in next else if block) by ensuring now > sendAgain
						if (dupAcks == 3) sendAgain = 0;
							
					}
				}	
			}
			// else if the resend timer has expired, re-send all
			// 		un-acked packets and reset their timers
			else if (now > sendAgain && sub.ready()) {
				//System.out.println("Timeout occured, resend all unacked packets");

				while (resendList.size() != 0) {
					//System.out.println("Timeout occured, resent packet");

					short curSeqNum = resendList.poll(); //seq# of packet (remove item from resendList)
					sub.send(sendBuf[curSeqNum]); //resend packet = sendBuf[curSeqNum]
					resendTime[curSeqNum] = now + timeout; //reset timeout
					dupAcks = 0;
				}
			}


			// else if there is a message from the source waiting to be sent 
			//      and the send window is not full
			//		and the substrate can accept a packet
			else if ((fromSrc.size() !=0) && 
				(diff(sendSeqNum,sendBase) < wSize) && sub.ready()) {
				//System.out.println("Msg from src ready to be sent");
			
				//create a packet containing the message and send it
				Packet dataPkt = new Packet();
				dataPkt.payload = fromSrc.poll();
				dataPkt.type = 0;
				dataPkt.seqNum = sendSeqNum;
				sub.send(dataPkt);

				//update send buffer and related data
				sendBuf[dataPkt.seqNum] = dataPkt;
				resendTime[dataPkt.seqNum] = now + timeout;
				resendList.add(dataPkt.seqNum);
				sendSeqNum = incr(sendSeqNum);
			}

			// else nothing to do, so sleep for 1 ms
			else {
				try {
					Thread.sleep(1);
				} catch(Exception e) {
					System.err.println("Rdt:run: sleep exception " + e);
					System.exit(1);
				}
			}
		}
	}

	/** Send a message to peer.
	 *  @param message is a string to be sent to the peer
	 */
	public void send(String message) {
		try {
			fromSrc.put(message);
		} catch(Exception e) {
			System.err.println("Rdt:send: put exception" + e);
			System.exit(1);
		}
	}
		
	/** Test if Rdt is ready to send a message.
	 *  @return true if Rdt is ready
	 */
	public boolean ready() { return fromSrc.remainingCapacity() > 0; }

	/** Get an incoming message.
	 *  @return next message
	 */
	public String receive() {
		String s = null;
		try {
			s = toSnk.take();
		} catch(Exception e) {
			System.err.println("Rdt:send: take exception" + e);
			System.exit(1);
		}
		return s;
	}
	
	/** Test for the presence of an incoming message.
	 *  @return true if there is an incoming message
	 */
	public boolean incoming() { return toSnk.size() > 0; }
}