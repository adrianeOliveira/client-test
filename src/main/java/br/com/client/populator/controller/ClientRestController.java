package br.com.client.populator.controller;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping(value="/client")
public class ClientRestController {

	private final Logger LOGGER = Logger.getLogger(ClientRestController.class);

	@Value(value = "${aws.sqs.queue.endpoint}")
	private String queueUrl;

	@Value(value = "${aws.sqs.queue.access.key}")
	private String accessKey;

	@Value(value = "${aws.sqs.queue.secret.key}")
	private String secretKey;

	@GetMapping("/fillQueue")
	public HttpStatus fillSqsQueue() {
		try {
			LOGGER.info("## [ClientRestController.fillSqsQueue]: initialized");

			AmazonSQS sqs = getAmazonSQS();
			List<String> stringList = listAllTemplatesKey();

			LOGGER.info("## [ClientRestController.fillSqsQueue]: sending templates key to queue");
			for (String item : stringList) {
				sendMessageToQueue(sqs, item);
			}

			LOGGER.info("## [ClientRestController.fillSqsQueue]: finished\n");
			return HttpStatus.OK;
		} catch (Exception e) {
			LOGGER.error("## [ClientRestController.fillSqsQueue]: error while sending messages to queue.\n {}", e);
			return HttpStatus.INTERNAL_SERVER_ERROR;

		}
	}

	@GetMapping(value = "/lunchTime")
	public HttpStatus triggerLunch() {
		LOGGER.info("## [ClientRestController.triggerLunch]: initialized");
		AmazonSQS sqs = getAmazonSQS();
		sendMessageToQueue(sqs, "KEY_LUNCH_TEMPLATE");
		LOGGER.info("## [ClientRestController.triggerLunch]: finished\n");
		return HttpStatus.OK;
	}

	@GetMapping(value = "/breakfastTime")
	public HttpStatus triggerBreakfast() {
		LOGGER.info("## [ClientRestController.triggerBreakfast]: initialized");
		AmazonSQS sqs = getAmazonSQS();
		sendMessageToQueue(sqs, "KEY_BREAKFAST_TEMPLATE");
		LOGGER.info("## [ClientRestController.triggerBreakfast]: finished\n");
		return HttpStatus.OK;
	}

	@GetMapping(value = "/dinnerTime")
	public HttpStatus triggerDinner() {
		LOGGER.info("## [ClientRestController.triggerDinner]: initialized");
		AmazonSQS sqs = getAmazonSQS();
		sendMessageToQueue(sqs, "KEY_DINNER_TEMPLATE");
		LOGGER.info("## [ClientRestController.triggerDinner]: finished\n");
		return HttpStatus.OK;
	}

	private List<String> listAllTemplatesKey () {
		List<String> list = new ArrayList<>();
		list.add("KEY_BREAKFAST_TEMPLATE");
		list.add("KEY_DINNER_TEMPLATE");
		list.add("KEY_LUNCH_TEMPLATE");
		return list;
	}

	private AmazonSQS getAmazonSQS() {
		LOGGER.info("## Getting AmazonSQS object");
		return AmazonSQSClientBuilder.standard()
				.withRegion(Regions.US_WEST_2)
				.build();
	}

	private void sendMessageToQueue(AmazonSQS sqs, String msg) {
		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, msg)
				.withRequestCredentialsProvider(new AWSStaticCredentialsProvider(credentials));
		// a non-empty MessageGroupId cannot be null or empty when sending messages to a FIFO queue
		/*sendMessageRequest.setMessageGroupId("templateKeyGroup");*/
		SendMessageResult sendMessageResult = sqs.sendMessage(sendMessageRequest);
		String sequenceNumber = sendMessageResult.getSequenceNumber();
		String messageId = sendMessageResult.getMessageId();
		LOGGER.info("## Message sent.");
		LOGGER.info("## Sequence number <" + sequenceNumber + ">");
		LOGGER.info("## Message Id <" + messageId + ">");
	}

}
