package org.elasticflow.util.email;

import javax.annotation.Resource;
import javax.mail.internet.MimeMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.util.StringUtils;

/**
 * @author chengwen
 * @version 1.2
 * @date 2018-10-11 11:00
 */
public class MailServiceImpl implements MailService {

	private static Logger log = LoggerFactory.getLogger(MailServiceImpl.class);

	@Resource
	private JavaMailSender mailSender;
	@Resource
	private TaskExecutor mailTaskExecutor;

	@Autowired
	private EmailModel emailModel;

	private boolean syncMode = true;

	private StringBuffer message = new StringBuffer();

	public MailServiceImpl() {

	}

	public void sendMail() throws Exception {
		this.init();
	}

	public void sendMail(String subject, String content) throws Exception {
		this.init();
	}

	public MailServiceImpl(boolean syncMode) {
		this.syncMode = syncMode;
	}

	public boolean isSyncMode() {
		return syncMode;
	}

	public void setSyncMode(boolean syncMode) {
		this.syncMode = syncMode;
	}

	public StringBuffer getMessage() {
		return message;
	}

	public void setMessage(StringBuffer message) {
		this.message = message;
	}

	private void init() throws Exception {
		if (this.emailModel == null) {
			this.message.append("no receiver!");
			return;
		}
		if (!syncMode) {
			sendMailByAsynchronousMode();
			this.message.append("send async Mode... ");
		} else {
			sendMailBySynchronizationMode();
			this.message.append("send sync Mode... ");
		}
	}

	/**
	 * send Mail By Synchronization
	 */
	public void sendMailBySynchronizationMode() throws Exception {
		MimeMessage mime = mailSender.createMimeMessage();
		MimeMessageHelper helper = new MimeMessageHelper(mime, true, "utf-8");
		helper.setFrom(emailModel.getFrom());
		helper.setTo(emailModel.getAddress());
		helper.setBcc(emailModel.getBcc());
		if (StringUtils.hasLength(emailModel.getCc())) {
			String cc[] = emailModel.getCc().split(",");
			helper.setCc(cc);
		}
		helper.setSubject(emailModel.getSubject());
		helper.setText(emailModel.getContent(), true);
		mailSender.send(mime);
	}

	/**
	 * send Mail By asSynchronization
	 */
	public void sendMailBySynchronizationMode(String subject, String content) throws Exception {
		MimeMessage mime = mailSender.createMimeMessage();
		MimeMessageHelper helper = new MimeMessageHelper(mime, true, "utf-8");
		helper.setFrom(emailModel.getFrom());
		helper.setTo(emailModel.getAddress());
		helper.setBcc(emailModel.getBcc());
		if (StringUtils.hasLength(emailModel.getCc())) {
			String cc[] = emailModel.getCc().split(",");
			helper.setCc(cc);
		}

		emailModel.setSubject(subject);
		emailModel.setContent(content);
		helper.setSubject(emailModel.getSubject());
		helper.setText(emailModel.getContent(), true);
		mailSender.send(mime);
	}

	public void sendMailByAsynchronousMode() {
		mailTaskExecutor.execute(new Runnable() {
			public void run() {
				try {
					sendMailBySynchronizationMode();
				} catch (Exception e) {
					log.error("AsynchronousMode send email has a exception:", e);
				}
			}
		});
	}

	public void sendMailByAsynchronousMode(final String subject, final String content) {
		mailTaskExecutor.execute(new Runnable() {
			public void run() {
				try {
					sendMailBySynchronizationMode(subject, content);
				} catch (Exception e) {
					log.error("AsynchronousMode send email has a exception:", e);
				}
			}
		});
	}
}
