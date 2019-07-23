/**
 * SimpleFlowToQueue.java
 * 
 * This sample demonstrates how to create a Flow to a durable or temporary Queue, 
 * and the use of a client acknowledgment of messages.
 * 
 * For the case of a durable Queue, this sample requires that a durable Queue 
 * called 'my_sample_queue' be provisioned on the appliance with at least 
 * 'Consume' permissions.
 * 
 * Copyright 2009-2019 Solace Corporation. All rights reserved.
 */

package com.solace.subscriber;

import com.solacesystems.jcsmp.*;
import com.solacesystems.jcsmp.statistics.StatType;

public class SimpleFlowToQueue implements XMLMessageListener {

    protected JCSMPSession session = null;
    protected String queueName = null;
    protected int msgCount = 0;
    protected boolean logMessages=false;

    // XMLMessageListener
    public void onException(JCSMPException exception) {
        exception.printStackTrace();
    }

    // XMLMessageListener
    public void onReceive(BytesXMLMessage message) {
        if(logMessages) {
            System.out.println("Received messasge number: " + msgCount);
            msgCount++;
        }
//        message.ackMessage();
    }


    // close the session and exit.
    protected void finish(final int status) {
        if (session != null) {
            System.out.println("Number of messages received: "
                    + session.getSessionStats().getStat(StatType.TOTAL_MSGS_RECVED));
            session.closeSession();
        }
        System.exit(status);
    }


    void createSession(String[] args) throws InvalidPropertiesException {
        // check command line arguments
        if (args.length < 4) {
            System.out.println("Usage: SimpleFlowToQueue <msg_backbone_ip:port> <vpn> <client-username> <queue-name> <log-messages>");
            System.out.println();
            System.out.println(" Note: the client-username provided must have adequate permissions in its client");
            System.out.println("       profile to send and receive guaranteed messages, and to create endpoints.");
            System.out.println("       Also, the message-spool for the VPN must be configured with >0 capacity.");
            System.exit(-1);
        }
        // Create a JCSMP Session
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);      // msg-backbone ip:port
        properties.setProperty(JCSMPProperties.VPN_NAME, args[1]);  // message-vpn
        properties.setProperty(JCSMPProperties.USERNAME, args[2]);  // client-username (assumes no password)
        queueName = args[3];

        if(args.length>4 && args[4]!=null && !args[4].isEmpty()){
            logMessages = Boolean.valueOf(args[4]);
        }

        session = JCSMPFactory.onlyInstance().createSession(properties);

        /*
         *  SUPPORTED_MESSAGE_ACK_CLIENT means that the received messages on the Flow 
         *  must be explicitly acknowledged, otherwise the messages are redelivered to the client
         *  when the Flow reconnects.
         *  SUPPORTED_MESSAGE_ACK_CLIENT is used here to simply to show
         *  SUPPORTED_MESSAGE_ACK_CLIENT. Clients can use SUPPORTED_MESSAGE_ACK_AUTO 
         *  instead to automatically acknowledge incoming Guaranteed messages.
         */
        properties.setProperty(JCSMPProperties.MESSAGE_ACK_MODE, JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
                
        // Disable certificate checking
        properties.setBooleanProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false);

        // Channel properties
        JCSMPChannelProperties cp = (JCSMPChannelProperties) properties.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
        // cp.setCompressionLevel(9);

        session = JCSMPFactory.onlyInstance().createSession(properties);
    }


    public void run(String[] args) {
        FlowReceiver receiver = null;
        try {
            // create a session and connect it to the router
            createSession(args);
            session.connect();

            System.out.printf("Attempting to provision the queue '%s' on the broker.%n", queueName);

            final EndpointProperties endpointProps = new EndpointProperties();
            // set queue permissions to "consume" and access-type to "exclusive"
            endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
            endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
            // create the queue object locally
            final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);

            // actually provision it, and do not fail if it already exists
            session.provision(queue, endpointProps, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
            Topic topicSub = JCSMPFactory.onlyInstance().createTopic("bar1/topicToQueueMapping/*");
                session.addSubscription(queue, topicSub, JCSMPSession.WAIT_FOR_CONFIRM);

            System.out.printf("Attempting to bind to the queue '%s' on the broker.%n", queueName);

            // create a Flow be able to bind to and consume messages from the Queue.
            final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
            flow_prop.setEndpoint(queue);
            /*
             * "SUPPORTED_MESSAGE_ACK_CLIENT"
             *  means the message needs to be manually acked by the client for an ack to propagate back to the router
             *
             *  "SUPPORTED_MESSAGE_ACK_AUTO"
             *  means the API will automatically ack back to Solace at the end of the message received callback
             */
            flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_AUTO);
            // bind to the queue and start the receiver
            receiver = session.createFlow(this, flow_prop, endpointProps);

            /***
             * Start the queue subscriber
             */
            receiver.start();

            System.out.printf("Process will exit on console input...");
            System.in.read();

        } catch (JCSMPTransportException ex) {
            System.err.println("Encountered a JCSMPTransportException, closing receiver... " + ex.getMessage());
            if (receiver != null) {
                receiver.close();
                // At this point the receiver handle is unusable; a new one
                // should be created.
            }
            finish(1);
        } catch (JCSMPException ex) {
            System.err.println("Encountered a JCSMPException, closing receiver... " + ex.getMessage());
            // Possible causes:
            // - Authentication error: invalid username/password
            // - Invalid or unsupported properties specified
            if (receiver != null) {
                receiver.close();
                // At this point the receiver handle is unusable; a new one
                // should be created.
            }
            finish(1);
        } catch (Exception ex) {
            System.err.println("Encountered an Exception... " + ex.getMessage());
            finish(1);
        }
    }

    public static void main(String[] args) {
        SimpleFlowToQueue app = new SimpleFlowToQueue();
        app.run(args);
    }
}
