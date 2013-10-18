package hydna.ntnu.student.impl;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import com.hydna.Channel;
import com.hydna.ChannelData;
import com.hydna.ChannelError;
import com.hydna.ChannelEvent;
import com.hydna.ChannelMode;
import com.hydna.ChannelSignal;

import aQute.bnd.annotation.component.Component;
import hydna.ntnu.student.api.HydnaApi;
import hydna.ntnu.student.listener.api.HydnaListener;

@Component
public class HydnaImpl implements HydnaApi{

	private HydnaListener listener;
	private Channel channel;
	private boolean stayConnected;
	private ChannelEvent event;
	private Thread thread;

	@Override
	public void connectChannel(String channelURL, String mode) {
		this.channel = new Channel();
		this.stayConnected = true;
		
		try {
			if(mode.equals("r")) this.channel.connect(channelURL, ChannelMode.READ);
			else if(mode.equals("rw")) this.channel.connect(channelURL, ChannelMode.READWRITE);
			else if(mode.equals("re")) this.channel.connect(channelURL, ChannelMode.READEMIT);
			else if(mode.equals("rwe")) this.channel.connect(channelURL, ChannelMode.READWRITEEMIT);
			else if(mode.equals("w")) this.channel.connect(channelURL, ChannelMode.WRITE);
			else if(mode.equals("we")) this.channel.connect(channelURL, ChannelMode.WRITEEMIT);
			else if(mode.equals("e")) this.channel.connect(channelURL, ChannelMode.EMIT);
			else if(mode.equals("l")) this.channel.connect(channelURL, ChannelMode.LISTEN);
			
			startMessageListening();
		} catch(ChannelError error) {
			this.listener.systemMessage("Channel error: "+ error);
			stayConnected(false);
		} catch (InterruptedException e) {
			this.listener.systemMessage("Interrupted Exception for thread sleep: "+e);
		}
		
	}
	
	public void startMessageListening() throws InterruptedException, ChannelError {
		Runnable runnable = new Runnable() {
			@Override
			public void run() {
				while(stayConnected) {
			        if (channel.hasEvents()) {
			        	try {
							event = channel.nextEvent();
						} catch (ChannelError | InterruptedException e) {
							listener.systemMessage("Channel error "+e);
							
						}
			        	if(event instanceof ChannelData) {
			        		if(event.isUtf8Content()) {
			        			listener.messageRecieved(event.getString());
			        		}
			        		else {
			        			listener.messageRecieved("Recieved binary data, use orignial library to handle that");
			        		}
			        	}
			        	else {
			        		listener.signalRecieved(event.getString());
			        	}
			        } 
			    }
				
			}
		};
		thread = new Thread(runnable);
		thread.start();
	}

	@Override
	public void sendMessage(String message) {
		try {
			channel.send(message);
		} catch (ChannelError e) {
			this.listener.systemMessage("Channel Error: "+e);
		}
	}

	@Override
	public void registerListener(HydnaListener listener) {
		this.listener = listener;
	}

	@Override
	public void stayConnected(boolean stayConnected) {
		this.stayConnected = stayConnected;
	}
	
	@Override
	public void emitSignal(String signal) {
		
		try {
			System.out.println("EMITTED SIGNAL");
			channel.emit(signal);
		} catch (ChannelError e) {
			this.listener.systemMessage("Channel Error: "+e);
		}
		
	}

}
