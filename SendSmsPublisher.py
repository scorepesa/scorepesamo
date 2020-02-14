from amqplib import client_0_8 as amqp
from utils import LocalConfigParser
#from dicttoxml import dicttoxml
import json
from flask import current_app
import pika
import amqp as amqp2


class SendSmsPublisher(object):

    def __init__(self, queue_name, exchange_name):
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.configs = LocalConfigParser.parse_configs("RABBIT")
        self.sdp_configs = LocalConfigParser.parse_configs("SDP")
        self.logger = current_app.logger

    def __del__(self):
        self.logger.info("Gabbage Collect SendSmsPublisher Obj .....")
        if self.exchange_name:
           self.exchange_name = None
        if self.queue_name:
           self.queue_name = None
        if self.configs:
           self.configs = None
        if self.sdp_configs:
           self.sdp_configs = None
        if self.logger:
           self.logger = None

    def publish(self, message, routing_key):

        self.logger.info("FOUND MESSAGE posting to Q: %r " % message)

        try:
            conn = amqp.Connection(host=self.configs['rabbithost'],
                userid=self.configs['rabbitusername'],
                password=self.configs['rabbitpassword'],
                virtual_host=self.configs['rabbitvhost'] or "/",
                insist=False)
        except Exception, e:
            self.logger.error("Error attempting to get Rabbit Connection: %r "
             % e)
            return False

        self.logger.info("Connection to rabbit established ...")
        try:
            self.logger.info("Attempting to queue message")
            ch = conn.channel()

            #ch.exchange_declare(exchange=self.exchange_name, type="fanout",
            #     durable=True, auto_delete=False)

            #ch.queue_declare(queue=self.queue_name, durable=True,
            #     exclusive=False, auto_delete=False)
            #ch.queue_bind(queue=self.queue_name, exchange=self.exchange_name,
            #     routing_key=routing_key)

            msg = amqp.Message(json.dumps(message))
            msg.properties["content_type"] = "text/plain"
            msg.properties["delivery_mode"] = 2

            ch.basic_publish(exchange=self.exchange_name,
                             routing_key=routing_key,
                             msg=msg)

            self.logger.info("Message queued success ... ")
            ch.close()
            return True
        except Exception, e:
            self.logger.error("Error attempting to publish to Rabbit: %r " % e)
            conn.close()
            return False
        else:
            ch.close()
            conn.close()


    def publish_betrader(self, message, routing_key, correlationId,
         rkeyheader, pub_type):
        self.logger.info("FOUND MESSAGE posting to Q: %r :: %s :: %s :: %s" % (message, routing_key, pub_type, correlationId))
        try:
           credentials=pika.PlainCredentials(self.configs['rbt-betrader-username'], self.configs['rbt-betrader-password'])
           connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.configs['rbt-betrader-host'],
                virtual_host=self.configs['rbt-betrader-vhost'],
                port=int(self.configs['rbt-betrader-port']),
                 credentials=credentials))
        except Exception, ex:
           self.logger.error("Error attempting RbtMQ Connection: %r" % ex)
           return False

        try:
            channel = connection.channel()
            self.logger.info("Abount to publish")

            if channel.basic_publish(exchange=self.exchange_name,
                      routing_key=routing_key,
                      body=json.dumps(message),
                      properties=pika.BasicProperties(headers=
                      {"replyRoutingKey": rkeyheader,
                           "correlationId": correlationId},
                       correlation_id=str(correlationId),
                       delivery_mode=1)):
                self.logger.info("Message publish was confirmed")
                channel.close()
                connection.close()
                return True
            else:
                self.logger.info("Message could not be confirmed")
                channel.close()
                connection.close()
                return False
        except Exception, e:
            self.logger.error("Error attempting RbtMQ publish: %r params %r"
             % (e, (self.configs['rbt-betrader-host'],
              self.configs['rbt-betrader-username'],
               self.configs['rbt-betrader-password'],
                self.configs['rbt-betrader-vhost'],
                 self.configs['rbt-betrader-port'])))
            connection.close()
            return False
        else:
            connection.close()
            self.close()

    def publishBt(self, message, routing_key, correlationId,
         rkeyheader, pub_type):

        self.logger.info("FOUND BT MESSAGE posting to Q: %r " % message)
        self.logger.info("Connect details {0}::{1}::{2}::{3}::{4}".format(self.configs['rbt-betrader-host-prod'], self.configs['rbt-betrader-port'],self.configs['rbt-betrader-username'],self.configs['rbt-betrader-password'],self.configs['rbt-betrader-vhost']))

        try:
            conn = amqp2.connection.Connection(host="tradinggate.betradar.com:5672",
                userid="scorepesa",
                password="AOPPtm0mcz",
                virtual_host="/scorepesa", heartbeat=10)

            self.logger.info("GOT connection {0}::{1}::{2}::{3}::{4}".format(self.configs['rbt-betrader-host-prod'], self.configs['rbt-betrader-port'],self.configs['rbt-betrader-username'],self.configs['rbt-betrader-password'],self.configs['rbt-betrader-vhost']))
        except Exception, e:
            self.logger.error("Error attempting to get BT Rabbit Connection :: %r " % e)
            return False

        self.logger.info("BT Connection to rabbit server established ...")
        try:
            self.logger.info("BT Attempting to queue message")
            conn.connect()
            self.logger.info("BT Connected explicit....." )
            ch = amqp2.Channel(conn)
            ch.open()
            self.logger.info("BT Acquired a channel ...." )

            msg = amqp2.basic_message.Message(json.dumps(message))
            msg.properties["content_type"] = "text/plain"
            msg.properties["delivery_mode"] = 1
            msg.properties["application_headers"] = {"replyRoutingKey": rkeyheader, "correlationId": correlationId}
            msg.properties["correlation_id"]=str(correlationId)
            
            self.logger.info("Done preparing message now to publish {0}".format(msg))

            ch.basic_publish(exchange=self.exchange_name,
                             routing_key=routing_key,
                             msg=msg)

            self.logger.info("BT Message queued success ... {0}::{1}".format(msg, self.exchange_name))
            ch.close()
            ch.collect()
            return True
        except Exception, e:
            self.logger.error("BT Error attempting to publish to Rabbit: %r " % e)
            conn.close()
            return False
        else:
            conn.close()
            return False


    def publishBpoint(self, message, routing_key, dead_letter_x, dead_letter_k):
        self.logger.info("FOUND BP MESSAGE posting to Q: %r " % message)
        self.logger.info("Connect details {0}::{1}::{2}::{3}::{4}".format(self.configs['rabbithost'], self.configs['rabbitusername'],self.configs['rabbitpassword'],self.configs['rabbitvhost'],self.configs['rabbitheartbeat']))

        try:
            conn = amqp2.connection.Connection(host=self.configs['rabbithost'],
                userid=self.configs['rabbitusername'],
                password=self.configs['rabbitpassword'],
                virtual_host=self.configs['rabbitvhost'] or "/", heartbeat=self.configs['rabbitheartbeat'] or "10")

            self.logger.info("GOT connection {0}::{1}::{2}::{3}::{4}".format(self.configs['rabbithost'], self.configs['rabbitusername'], self.configs['rabbitpassword'], self.configs['rabbitvhost'], self.configs['rabbitheartbeat']))
        except Exception, e:
            self.logger.error("Error attempting to get BT Rabbit Connection :: %r " % e)
            return False
        self.logger.info("BP Connection to rabbit server established ...")
        try:
            self.logger.info("BP Attempting to queue message")
            conn.connect()
            self.logger.info("BP Connected explicit....." )
            ch = amqp2.Channel(conn)
            ch.open()
            self.logger.info("BP Acquired a channel ...." )

            msg = amqp2.basic_message.Message(json.dumps(message))
            msg.properties["content_type"] = "text/plain"
            msg.properties["delivery_mode"] = 1
            msg.properties["x-message-ttl"] = ['I',60000]
            msg.properties["x-dead-letter-exchange"] = ['S', dead_letter_x]
            msg.properties["x-dead-letter-routing-key"] = ['S', dead_letter_k]
            msg.properties["application_headers"] = {"replyRoutingKey": routing_key}
            
            self.logger.info("Done preparing message now to publish {0}".format(msg))

            ch.basic_publish(exchange=self.exchange_name,
                             routing_key=routing_key,
                             msg=msg)

            self.logger.info("BP Message queued success ... {0}::{1}".format(msg, self.exchange_name))
            ch.close()
            ch.collect()
            return True
        except Exception, e:
            self.logger.error("BP Error attempting to publish to Rabbit: %r " % e)
            conn.close()
            return False
        else:
            conn.close()
            return False

