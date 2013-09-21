package hydna.examples;

import hydna.Channel;
import hydna.ChannelData;
import hydna.ChannelError;
import hydna.ChannelMode;

/**
 *  Hello world example
 */
public class HelloWorld {
	public static void main(String[] args) throws ChannelError, InterruptedException {
		Channel channel = new Channel();
	    channel.connect("student.hydna.net/1", ChannelMode.READWRITE);
	
	    while(!channel.isConnected()) {
	        channel.checkForChannelError();
	        Thread.sleep(1000);
	    }
	    
	    String message = channel.getMessage();

	    if (!message.equals("")) {
	    	System.out.println(message);
	    }

	    channel.writeString("Hello world from java");

	    for (;;) {
	        if (!channel.isDataEmpty()) {
	            ChannelData data = channel.popData();
            	System.out.println(data.getString());
	            break;
	        } else {
	            channel.checkForChannelError();
	        }
	    }
	    channel.close();
	}
}
