package org.elasticflow.notifier;

import org.elasticflow.config.GlobalParam;

/**
 * EF Error Notifier
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-31 10:52
 * @modify 2021-06-11 10:45
 */
public class EFNotifier implements EFNotify{
	
	EFNotify mailSender;
	
	EFNotify apiSender;
	
	public EFNotifier() {
		if(GlobalParam.SEND_EMAIL_ON) 
			mailSender = new EFEmailNotifier();
		if(GlobalParam.SEND_API_ON!="") 
			apiSender = new EFApiNotifier();
	}

	@Override
	public boolean send(String subject,String instance, String content,String errorType, boolean sync) {
		boolean sendstate = true;
		if(GlobalParam.SEND_EMAIL_ON) 
			sendstate &= mailSender.send(subject,instance,content,errorType,sync);
		if(GlobalParam.SEND_API_ON!="") 
			sendstate &= apiSender.send(subject,instance,content,errorType,sync);
		return sendstate;
	}
}
