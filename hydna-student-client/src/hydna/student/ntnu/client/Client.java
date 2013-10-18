package hydna.student.ntnu.client;

import hydna.ntnu.student.api.HydnaApi;
import hydna.ntnu.student.listener.api.HydnaListener;

import aQute.bnd.annotation.component.Activate;
import aQute.bnd.annotation.component.Component;
import aQute.bnd.annotation.component.Reference;

@Component
public class Client {
	
	private HydnaApi hydnaSvc;
	private HydnaListener listener;

	@Activate
	public void activate() {
		System.out.println("STARTED");
		this.listener = new HydnaListener() {
			
			@Override
			public void systemMessage(String msg) {
				System.out.println("got sysmsg: "+msg);
			}
			
			@Override
			public void signalRecieved(String msg) {
				System.out.println("got signal: "+msg);
			}
			
			@Override
			public void messageRecieved(String msg) {
				System.out.println("got msg: "+msg);
			}
		};
		hydnaSvc.registerListener(this.listener);
		hydnaSvc.connectChannel("students.hydna.net/room", "rwe");
		hydnaSvc.emitSignal("SINGALS CLIENT");
		hydnaSvc.sendMessage("MESSAGE CLIENT");
	}
	
	@Reference
	public void setHydnaApi(HydnaApi hydna) {
		System.out.println("SETTIsNG SERVICSE");
		this.hydnaSvc = hydna;
	}
	

}
