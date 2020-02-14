from amqplib import client_0_8 as amqp
from utils import LocalConfigParser
#from dicttoxml import dicttoxml
import datetime
import json
from decimal import Decimal;


class Publisher(object):

    QUEUE = 'SCOREPESA_WITHDRAWAL_QUEUE'
    ROUTING_KEY = 'SCOREPESA_WITHDRAWAL_KEY'

    def __init__(self, db, logger):
        self.configs = LocalConfigParser.parse_configs("RABBIT")
        self.logger = logger
        self.db = db

    def log_to_file(self, message):
        self.logger.info("Calling write to file... ")
        file_name = "/var/log/scorepesa/FAILED_MESSAGES.TXT"
        with open(file_name, "a") as failed_messages_file:
            message_str = "||".join(["%s:%s" 
                % (key, message.get(key)) for key in message])
            failed_messages_file.write(message_str + "\n")
            self.logger.info("Logged message to file proceeding")

    def get_queue_message(self, message):

        queue_message = {
            "shortCode":message.get("short_code"),
            "msisdn":message.get("msisdn"),
            "amount":float(message.get("amount")),
            "withdrawal_id":message.get("withdrawal_id"),
            "request_amount":float(message.get("request_amount")),
            "charge":float(message.get("charge")),
            "refNo":"%s_%s" % (message.get("withdrawal_id"),message.get("msisdn")),
            "queueType":"MPESA_WITHDRAWAL",
            "created":message.get("created").isoformat(),

        }

        return queue_message

    def object_tojson(self, _object):
        dict_object = {}

        for c in _object.__table__.columns:
            value = getattr(_object, c.name)

            if isinstance(value, datetime.datetime):
                value = value.isoformat()
            elif isinstance(value. Decimal):
                value = float(value)
            dict_object.update({c.name: value })

        return dict_object

    def publish_retry(self, message):
        self.logger.info("FOUND RETRY MESSAGE posting to Q: %r " % message)
        _object = message.get('object')

        dict_object = self.object_tojson(_object)

        self.logger.info("Parsed object ...: %r " % dict_object)
        q_message = {}
        q_message['queueType'] = message.get('type')
        q_message.update(dict_object)

        queue_name = "%s_%s" % (message.get('type'), 'SCOREPESA_RETRY_QUEUE')
        exchange_name = "%s_%s" % (message.get('type'), 'SCOREPESA_RETRY_QUEUE')
        queue_route = "%s_%s" % (message.get('type'), 'SCOREPESA_RETRY_ROUTE')

        try:
            conn = amqp.Connection(host=self.configs['rabbithost'],
                userid=self.configs['rabbitusername'],
                password=self.configs['rabbitpassword'],
                virtual_host=self.configs['rabbitvhost'] or "/",
                insist=False)
        except Exception, e:
            self.logger.error("Error attempting to get Rabbit Connection: %r " % e)
            self.log_to_file(message)
            return

        self.logger.info("Connection to rabbit established ...")
        try:
            self.logger.info("Attempting to queue message")
            ch = conn.channel()

            ch.exchange_declare(exchange=exchange_name, type="direct", durable=True, auto_delete=False)

            ch.queue_declare(queue=queue_name, durable=True, exclusive=False, auto_delete=False)
            ch.queue_bind(queue=queue_name, exchange=exchange_name, routing_key=queue_route)

            msg = amqp.Message(json.dumps(q_message))
            msg.properties["content_type"] = "text/plain"
            msg.properties["delivery_mode"] = 2

            ch.basic_publish(exchange=queue_name,
                             routing_key=queue_route,
                             msg=msg)

            self.logger.info("Message queued success ... ")
        except Exception, e:
            self.logger.error("Error attempting to publish to Rabbit: %r " % e)
            self.log_to_file(message)
            self.logger.error("Logged message to file proceeding")
            conn.close()
        else:
            ch.close()
            conn.close()

    def publish_bonus_message(self, message):
        self.logger.info("FOUND MESSAGE Bonus posting to Q: %r " % message)
        q_message = {"queue.QMessage":
                        {"refNo":"%s_%s" % (message.get('msisdn'), message.get('outbox_id')),
                         "msisdn":message.get('msisdn'),
                         "message":message.get('text')}}
        try:
            conn = amqp.Connection(host=self.configs['rabbithost'],
                userid=self.configs['rabbitusername'],
                password=self.configs['rabbitpassword'],
                virtual_host=self.configs['rabbitvhost'] or "/",
                insist=False)
        except Exception, e:
            self.logger.error("Error attempting to get Rabbit Connection: %r " % e)
            self.log_to_file(message)
            return;

        self.logger.info("Connection to rabbit established ...")
        try:
            self.logger.info("Attempting to queue message")
            ch = conn.channel()
            ch.exchange_declare(exchange='SCOREPESA_WINNER_MESSAGES_QUEUE', type="direct", durable=False, auto_delete=False)
            ch.queue_declare(queue='SCOREPESA_WINNER_MESSAGES_QUEUE', durable=True, exclusive=False, auto_delete=False)
            ch.queue_bind(queue='SCOREPESA_WINNER_MESSAGES_QUEUE', exchange='SCOREPESA_WINNER_MESSAGES_QUEUE', routing_key='SCOREPESA_WINNER_MESSAGES_QUEUE')
            msg = amqp.Message(json.dumps(q_message))
            msg.properties["content_type"] = "text/plain"
            msg.properties["delivery_mode"] = 2
            ch.basic_publish(exchange='SCOREPESA_WINNER_MESSAGES_QUEUE',
                             routing_key='SCOREPESA_WINNER_MESSAGES_QUEUE',
                             msg=msg)
            self.logger.info("Bonus Message queued success ... ")
        except Exception, e:
            self.logger.error("Error attempting to publish to Rabbit: Bonus %r " % e)
            self.log_to_file(message)
            self.logger.error("Logged message to file proceeding - Bonus")
            conn.close()
        else:
            ch.close()
            conn.close()



    def publish(self, message, withdrawal, operator):

        if not withdrawal or not withdrawal.get("withdrawal_id"):
            self.logger.info("Failed to create withdrawal message  ...: %r " % withdrawal)
            self.log_to_file(message)
            return

        qbool=True
        exbool=True
        exchange_type = 'direct'
        q_message = self.get_queue_message(message)
        #if operator == 'AIRTEL':
        #   self.QUEUE='WithDraw_30750439'
        #   self.ROUTING_KEY='WithDraw_30750439'
        #   exchange_type = 'fanout'
        #   q_message = {
        #     "id": "%s-%s" % (message.get("withdrawal_id"),message.get("msisdn")),
        #     "amount": float(message.get("amount")),
        #     "msisdn": message.get("msisdn")
        #   }
        #   self.logger.info("Airtel withdraw request %r" % message)
 
        self.logger.info("FOUND MESSAGE Withdrawal posting to Q: %r :: queue %s :: key %s " % (message, self.QUEUE, self.ROUTING_KEY))

        try:
            conn = amqp.Connection(host=self.configs['rabbithost'],
                userid=self.configs['rabbitusername'],
                password=self.configs['rabbitpassword'],
                virtual_host=self.configs['rabbitvhost'] or "/",
                insist=False)
        except Exception, e:
            self.logger.error("Error attempting to get Rabbit Connection: %r " % e)
            self.log_to_file(message)
            return;

        self.logger.info("Connection to rabbit established ...")
        try:
            self.logger.info("Attempting to queue message")
            ch = conn.channel()

            ch.exchange_declare(exchange=self.QUEUE, type=exchange_type, durable=exbool, auto_delete=False)

            ch.queue_declare(queue=self.QUEUE, durable=qbool, exclusive=False, auto_delete=False)
            ch.queue_bind(queue=self.QUEUE, exchange=self.QUEUE, routing_key=self.ROUTING_KEY)

            msg = amqp.Message(json.dumps(q_message))
            msg.properties["content_type"] = "text/plain"
            msg.properties["delivery_mode"] = 2

            ch.basic_publish(exchange=self.QUEUE,
                             routing_key=self.ROUTING_KEY,
                             msg=msg)

            self.logger.info("Message queued success ... ")
        except Exception, e:
            self.logger.error("Error attempting to publish to Rabbit: %r " % e)
            self.log_to_file(message)
            self.logger.error("Logged message to file proceeding")
            conn.close()
        else:
            ch.close()
            conn.close()

        self.logger.info("Updating withdrawal status ... ")
       

