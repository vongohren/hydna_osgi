package hydna.ntnu.student.impl;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import aQute.bnd.annotation.component.Component;
import hydna.Channel;
import hydna.ChannelData;
import hydna.ChannelError;
import hydna.ChannelMode;
import hydna.ChannelSignal;
import hydna.ntnu.student.api.HydnaApi;
import hydna.ntnu.student.listener.api.HydnaListener;

@Component
public class HydnaImpl implements HydnaApi{

	private HydnaListener listener;
	private Channel channel;
	private boolean stayConnected;

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
			
			while(!channel.isConnected()) {
		        channel.checkForChannelError();
		        Thread.sleep(1000);
		    }
			startMessageListening();
			
			
		} catch(ChannelError error) {
			this.listener.systemMessage("Channel error: "+ error);
		} catch (InterruptedException e) {
			this.listener.systemMessage("Interrupted Exception for thread sleep: "+e);
		}
		
	}
	public void startMessageListening() {
		try {
			while(stayConnected) {
		        if (!channel.isDataEmpty()) {
		            ChannelData data = channel.popData();
		            ByteBuffer payload = data.getContent();
	
		            Charset charset = Charset.forName("US-ASCII");
	            	CharsetDecoder decoder = charset.newDecoder();
					
	            	String m = decoder.decode(payload).toString();
					
	            	this.listener.messageRecieved(m);
		        } 
		        else if (!channel.isSignalEmpty()) {
		            ChannelSignal signal = channel.popSignal();
		            ByteBuffer payload = signal.getContent();
	
		            Charset charset = Charset.forName("US-ASCII");
	            	CharsetDecoder decoder = charset.newDecoder();
					
	            	String m = decoder.decode(payload).toString();
					
	            	this.listener.signalRecieved(m);
		        }
		        else {
		            channel.checkForChannelError();
		        }
		    }
		}
		catch(ChannelError e) {
			this.listener.systemMessage("Channel error: "+ e);
		} catch (CharacterCodingException e) {
			this.listener.systemMessage("CharacterCodingException: "+ e);
		}
	}

	@Override
	public void sendMessage(String message) {
		try {
			channel.writeString(message);
		} catch (ChannelError e) {
			this.listener.systemMessage("Channel Error: "+e);
		}
	}

	@Override
	public void registerListener(HydnaListener listener) {
		System.out.println("REGISTERING LISTENER");
		this.listener = listener;
	}

	@Override
	public void stayConnected(boolean stayConnected) {
		this.stayConnected = stayConnected;
	}
	@Override
	public void emitSignal(String signal) {
		// TODO Auto-generated method stub
		
	}

}
