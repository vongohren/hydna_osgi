package hydna.ntnu.student.api;

import hydna.ntnu.student.listener.api.HydnaListener;

public interface HydnaApi {
	public void connectChannel(String channelURL, String mode);
	public void sendMessage(String message);
	public void emitSignal(String signal);
	public void registerListener(HydnaListener listener);
	public void stayConnected(boolean stayConnected);
}
