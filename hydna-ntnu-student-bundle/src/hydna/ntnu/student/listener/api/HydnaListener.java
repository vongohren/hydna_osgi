package hydna.ntnu.student.listener.api;

public interface HydnaListener {
	public void messageRecieved(String msg);
	public void signalRecieved(String msg);
	public void systemMessage(String msg);
}
