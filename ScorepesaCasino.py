from utils import LocalConfigParser
from sqlalchemy.sql import text as sql_text
from datetime import datetime
from db.schema import Db, Transaction
import json
from SendSmsPublisher import SendSmsPublisher
from ScorepesaPoint import ScorepesaPoint
import requests
import urllib


class ScorepesaCasino(object):

    APP_NAME = 'scorepesa_mo_consumer_casino'

    def __init__(self, logger):
        self.logger=logger
        self.db_configs = LocalConfigParser.parse_configs("DB")
        self.vtdb_configs = LocalConfigParser.parse_configs("VTDB")
        self.scorepesa_configs = LocalConfigParser.parse_configs("SCOREPESA")
        self.scorepesa_points_cfgs = LocalConfigParser.parse_configs("SCOREPESAPOINT")
        self.scorepesa_freebet_cfgs = LocalConfigParser.parse_configs("FREEBET")
        self.scorepesa_bonus_cfgs = LocalConfigParser.parse_configs("SCOREPESABONUS")
        self.scorepesa_virtuals_cfgs = LocalConfigParser.parse_configs("SCOREPESAVIRTUALS")
        self.scorepesa_casino_cfgs = LocalConfigParser.parse_configs("SCOREPESACASINO")
        self.db=self.db_factory()
        self.balance, self.bonus = None, None
        self.logger.info("Scorepesa casino process init.....")
        super(ScorepesaCasino, self).__init__()

    def __del__(self):
        self.db=None
      
    def db_factory(self):
         self.db = Db(self.db_configs['host'], self.db_configs['db_name'],
            self.db_configs['username'], self.db_configs['password'],
             socket=self.db_configs['socket'], port=self.db_configs['port'])

         return self.db

    def scorepesa_profile_exists(self, profile_id):
        profile_id = message.get('user')
        profile_query = "select profile_id, msisdn, created, network from"\
            " profile where %s=:value limit 1"

        if profile_id and int(profile_id) > 0:
            profile_query = profile_query % ("profile_id")
            values = {'value':profile_id}
        else:
           return False

        profile = self.db.engine.execute(sql_text(profile_query),values)\
            .fetchone()
        self.logger.info('Query profile sql :::: %s' % (profile))

        if not profile:
           self.logger.info("no matching profile found::: %r " % profile_id)
           return False
        else:
            profile_id, msisdn, created, network = profile
            self.logger.info("found profile data:::{0}::{1}::{2}::{3}...."\
                .format(profile_id, msisdn, created, network))

        self.profile_id = profile_id
        self.operator = network

        return profile_id

    def create_seven_aggregator_request(self, request_data, request_type):
        self.logger.info("create seven aggregator req....{0}::reqtype::{1}"\
            .format(request_data, request_type))
        connection=self.db.engine.connect()       

        amount=request_data.get("amount")
        request_name=request_data.get("request_name")
        amount_small=request_data.get("amountSmall")
        currency=request_data.get("currency")
        user=request_data.get("user")
        profile_id = user
        #validate balance enough to proceed
        if profile_id and float(amount) > 0.0 and int(request_type)==0:
           balance, bonus=self.get_account_balance(profile_id)
           if float(balance) < float(amount):
              return 1,1,200,0
        else:
           if int(request_type)==0:
              return 1,1,200,-1

        #check minimum bet amount
        #if request_name=="ReserveFunds" and float(amount) < 50.0:
        #    return 1,1,200,-1

        payment_strategy=request_data.get("paymentStrategy")
        transactionType=request_data.get("transactionType")
        payment_id=request_data.get("paymentId")
        transaction_id=request_data.get("transactionId")
        source_id=request_data.get("sourceId")
        reference_id=request_data.get("referenceId")
        tp_token=request_data.get("tpToken")
        ticket_info=json.dumps(request_data.get("ticketInfo"))
        security_hash=request_data.get("securityHash")
        club_uuid=request_data.get("clubUuid") 
        status=1
        created_by=request_name
        date_created=datetime.now()
        reference=request_name

        insQ="INSERT INTO seven_aggregator_request (amount, request_name, "\
            "amount_small, currency, user, payment_strategy, transactionType,"\
            " payment_id, transaction_id, source_id, reference_id, tp_token, "\
            "ticket_info, security_hash, club_uuid, status, created_by, "\
            "date_created) VALUES (:amount, :request_name, :amount_small, "\
            ":currency, :user, :payment_strategy, :transactionType, "\
            ":payment_id, :transaction_id, :source_id, :reference_id, "\
            ":tp_token, :ticket_info, :security_hash, :club_uuid, "\
            ":status, :created_by, :date_created)"
        params={
            "amount":amount, "request_name":request_name, 
            "amount_small":amount_small, "currency":currency, "user":user, 
            "payment_strategy":payment_strategy, 
            "transactionType":transactionType, "payment_id":payment_id, 
            "transaction_id":transaction_id, "source_id":source_id, 
            "reference_id":reference_id, "tp_token":tp_token, 
            "ticket_info":ticket_info, "security_hash":security_hash, 
            "club_uuid":club_uuid, "status":status, "created_by":created_by, 
            "date_created":date_created }
      
        self.logger.info("initiating request processing..... {0}:::{1}"\
            .format(insQ, params))

        try:
            trans=connection.begin()
             #if payment id,requestname and profile query dont exist create if exists return
            if not self.check_seven_aggrigator_payment_exists(request_data):
                result=connection.execute(sql_text(insQ), params)
            else:
                balance, bonus=self.get_account_balance(profile_id)
                return 1,1,200, balance
             #if bet(a reserve) dont exists return error for any credits request
            if not self.check_seven_aggrigator_bet_exists(request_data) and \
                request_name=='CreditRequest':
                return 1,1,200,-2

            self.logger.info("seven_aggregator_request result ..... {0}".format(result))
            if result:
                aggregator_id = result.lastrowid
                #create transaction for credit/debit
                trx_res=self.create_transaction(request_type, request_data, 
                    aggregator_id, reference, connection)
                self.logger.info(
                    "created transcation .... result:: {0} ...commiting..."\
                    .format(trx_res))
            #update aggregator request status =5

            if request_name=="CreditRequest":
                #send winning message for bet won(on credit request)
                self.send_message(request_data)
            if request_name=="ReserveFunds":
                #award scorepesa points for bet placed(on reserve confirm request)
                self.award_scorepesa_points(request_data, trx_res)

            balance, bonus=self.get_account_balance(profile_id)

            trans.commit()   

            return aggregator_id, trx_res, 200, balance
        except Exception, ex:
          trans.rollback()
          self.logger.error(
            "Exception processing seven aggregator request...rolledback {0}"\
            .format(ex))
          return None, None, 500, None

    def create_transaction(self, request_type, 
        request_data, aggregator_id, reference, connection):
        try:
            profile_id = request_data.get("user")
            created_by=request_data.get("request_name")
            amount=request_data.get("amount")
            payment_id=request_data.get("paymentId")
            #amount to be crdit/debit
            bet_on_balance=float(amount)
            reference_id=request_data.get("referenceId")

            roamtech_id = self.get_roamtech_virtual_acc('ROAMTECH_VIRTUAL')
            trx_debit_dict = {
                "profile_id": profile_id,
                "account": "%s_%s" % (profile_id, 'VIRTUAL'),
                "iscredit": request_type,
                "reference": "{0}#{1}".format(payment_id, reference_id),
                "amount": bet_on_balance,
                "created_by": created_by,
                "created": datetime.now(),
                "modified": datetime.now()
            }
            trxd = connection.execute(
                Transaction.__table__.insert(), trx_debit_dict)
            trxd_id = trxd.inserted_primary_key[0]

            #update profile_balance
            bu_Q = "update profile_balance set balance=(balance-{0}) where "\
                "profile_id=:profile_id limit 1".format(bet_on_balance)
            if request_type == 1:
                #credit
                bu_Q = "update profile_balance set balance=(balance+{0}) where"\
                    " profile_id=:profile_id limit 1".format(bet_on_balance)

                connection.execute(sql_text(bu_Q),{'profile_id': profile_id}) 
            return trxd_id 
        except Exception, ex:
           self.logger.error("Exception aggregator transaction {0}".format(ex))
           raise

    def get_roamtech_virtual_acc(self, acc):
        if acc == 'ROAMTECH_MPESA':
            if 'mpesa_roamtech_profile_id' in self.scorepesa_configs:
                return self.scorepesa_configs['mpesa_roamtech_profile_id']
            return 5

        if 'virtual_roamtech_profile_id' in self.scorepesa_configs:
            return self.scorepesa_configs['virtual_roamtech_profile_id']
        return 6

    def get_account_balance(self, profile_id):
        bal = False
        if profile_id:
            bal = self.db.engine.execute(
                sql_text("select balance, bonus_balance from profile_balance"\
                    " where profile_id = :value"),
                {'value':profile_id}).fetchone()
        if bal:
            available_bonus=float(bal[1])
            self.balance, self.bonus = float(bal[0]), available_bonus
        else:
            self.balance, self.bonus = float(0), float(0)
        return self.balance, self.bonus
    
    def confirm_payment(self, message):
        msg_status='ERROR'
        bal=''
        currency=''
        msg='payment not exist.'
        
        result = self.db.engine.execute(sql_text("select id, status, user"
            " from seven_aggregator_request where payment_id=:value"),
             {'value': message.get("paymentId")}).fetchone()
        if result:
           _id, status, user=result
           bal,bonus=self.get_account_balance(user)
           if _id > 0 and status==1:
              #flag payment as confirmed(set status=2)
              flagged = self.flag_payment_as_confirmed(message.get("paymentId"))
              if flagged:
                 msg_status='OK'
                 bal=int(bal)
                 currency='KES'
                 msg=''
           if _id > 0 or status==5 or status==2:
                msg_status='OK'
                bal=int(bal)
                currency='KES'
                msg=''

        status=200
        response = {"status": msg_status, "balance": bal, 
            "currency": currency, "msg": msg}
        return response, status

    def confirm_trx(self, message):
        msg_status='ERROR'
        bal=''
        currency=''
        msg='payment not exist.'
        sQl = "select id, status, user from seven_aggregator_request where"\
            " payment_id = :value and transaction_id=:trxid"
        params = {'value': message.get("paymentId"), 
            "trxid": message.get("transactionId")}
 
        if message.get("transaction_id") is None:
            sQl="select id, status, user from seven_aggregator_request "\
                "where payment_id=:value and payment_strategy='strictSingle'"
            params = {'value': message.get("paymentId")}

        result = self.db.engine.execute(sql_text(sQl), params).fetchone()
        if result:
          _id, status, user=result
          bal,bonus=self.get_account_balance(user)
          if _id > 0 and status==1:
             #flag payment as confirmed(set status=5)
             flagged = self.flag_payment_as_confirmed(
                message.get("paymentId"), message.get("transactionId"))
             if flagged:
                msg_status='OK'
                bal=int(bal)
                currency='KES'
                msg='Successful request'
          if _id > 0 or status==5 or status==2:
               msg_status='OK'
               bal=int(bal)
               currency='KES'
               msg='Successful request'

        #prepare response
        status=200
        response={"status": msg_status, "balance": bal, 
            "currency": currency, "msg": msg}
        return response, status

    def flag_payment_as_confirmed(self, payment_id, 
        trx_id=None, status='completed'):
        connection = self.db.engine.connect()
        trans=connection.begin()
        sql = "update seven_aggregator_request set status={0}, "\
            "aggregator_status=:agstatus where payment_id = :payment"\
            .format(2, status)
        params={"payment": payment_id, "agstatus": status}
        if trx_id:
            params={"payment": payment_id, "transaction_id": trx_id, 
                "agstatus": status} 
            sql = "update seven_aggregator_request set status={0}, "\
                "aggregator_status=:agstatus where payment_id=:payment and"\
                " transaction_id=:trxid".format(2,status)
        try:
            res = connection.execute(sql_text(sql), params)
            self.logger.info("flag payment as confirmed :sql::{0}::params::{1}"\
                "::result::{2}....".format(sql, params, res))
            trans.commit()
            return True
        except Exception, e:
            trans.rollback()
            self.logger.error("Exception flag payment confirm rolledback... {0}"\
                .format(e))
            return False

    def do_payment_cancel(self, payment_id, trx_id=None):
        connection = self.db.engine.connect()
        trans=connection.begin()
        request_data=[]

        msg_status='ERROR'
        bal=''
        currency=''
        msg='Missing required parameters.'
        status=200

        extra_sql=""
        params = {"payid": payment_id}
        if trx_id:
           params = {"payid": payment_id, "trxid": trx_id}
           extra_sql= " and transaction_id=:trx_id"
        sql="select id as aggregator_id, amount, payment_id, transaction_id"\
            " from seven_aggregator_request where status=1 and "\
            "payment_id=:payid {0}".format(extra_sql)
        self.logger.info("do payment cancellation...{0}::{1}::{2}::{3}"\
            .format(payment_id, trx_id, sql, params))
        try:
            res=connection.execute(sql_text(sql), params).fetchone()
            aggregator_id, amount, payment_id, transaction_id, user=res
            if payment_id and float(amount) > 0.0:
                #cancel transaction
                request_type=1
                request_data['user']=user
                request_data['created_by']='CancelPaymentRequest'
                request_data['amount']=amount
              
                cancel_res=self.create_transaction(request_type, request_data,
                     aggregator_id, connection)
                if cancel_res:
                    self.flag_payment_as_confirmed(payment_id, trx_id, 
                        status='cancelled')
            msg_status='OK'
            bal=''
            currency=''
            msg=''
            #prepare response
            return {"status": msg_status, "balance": bal, "currency": currency,
                 "msg": msg}, status
        except Exception, ex:
            self.logger.error("Exception cancel payment/transaction.... {0}"\
                .format(ex))
            return {"status": msg_status, "balance": bal, 
                "currency": currency, "msg": msg}, status

    def do_resettle(self, payment_id, trx_id):
        connection = self.db.engine.connect()
        trans=connection.begin()
        request_data=[]
        
        msg_status='ERROR'
        bal=''
        currency=''
        msg='payment not exist.'
        status=200
   
        params = {"payid": payment_id, "trxid": trx_id}
        if not trx_id or not payment_id:
            return json.dumps(
                {"status": msg_status, "balance": bal, 
                "currency": currency, "msg": msg}), status
        sql="select id as aggregator_id, amount, payment_id, transaction_id"\
            " from seven_aggregator_request where status=1 and"\
            " payment_id=:payid {0}".format(extra_sql)
        self.logger.info("do payment resettle...{0}::{1}::{2}::{3}"\
            .format(payment_id, trx_id, sql, params))
        try:
            res=connection.execute(sql_text(sql), params).fetchone()
            aggregator_id, amount, payment_id, transaction_id, user=res
            if payment_id and float(amount) > 0.0:
                #resettle transaction
                request_type=1
                request_data['user']=user
                request_data['created_by']=''
                request_data['amount']=amount
              
                cancel_res=self.create_transaction(request_type, request_data, 
                    aggregator_id, connection)
                if cancel_res:
                    self.flag_payment_as_confirmed(payment_id, 7, trx_id)
            msg_status='OK'
            bal=''
            currency=''
            msg=''
            #prepare response
            return {"status": msg_status, "balance": bal, 
                "currency": currency, "msg": msg}, status
        except Exception, ex:
            return {"status": msg_status, "balance": bal, 
                "currency": currency, "msg": msg}, status
            self.logger.error("Exception resettle payment.... {0}".format(ex))


    def get_user_funds(self, profile_id):
        status=200
        msg_status='ERROR'
        currency=''
        msg='User not exist.'
        if profile_id and int(profile_id) > 0:
           bal, bonus = self.get_account_balance(profile_id)
           msg_status='OK'
           currency='KES'
           msg=''
        return {"status": msg_status, "balance": bal,
             "currency": currency, "msg": msg}, status

    def get_player_detail(self, profile_id):
        player_detail = False
        if profile_id:
            player_detail = self.db.engine.execute(
                sql_text("select name, p.msisdn from profile p left join"\
                    " profile_settings s on (p.profile_id=s.profile_id) where"\
                    " p.profile_id = :profile"), {'profile': profile_id})\
                    .fetchone()
        if player_detail:
            name = player_detail[0]
            msisdn = player_detail[1]  
        else:
            name=''
            msisdn=''
        response = {
          "id": profile_id,
          "username": "player{0}".format(profile_id),
          "email": "player_{0}@scorepesa.com".format(profile_id),
          "firstName": "player{0}".format(profile_id),
          "lastName": "player{0}".format(profile_id)
        }          
        return response, 200

    def check_seven_aggrigator_payment_exists(self, message):
        result = self.db.engine.execute(sql_text("select id, payment_strategy,"\
            " amount_small, request_name, amount, reference_id, "\
            "aggregator_status, payment_id, status, user from "\
            "seven_aggregator_request where user=:pid and payment_id=:id "\
            "and request_name=:req_type"), {'id': message.get("paymentId"), 
                "pid":message.get("user"), 
                "req_type":message.get("request_name")})\
            .fetchone()
        if result:
           sid, payment_strategy, amount_small, request_name, \
           amount, reference_id, aggregator_status, payment_id, \
           status, user=result
           bal,bonus=self.get_account_balance(user)
           if sid and sid > 0:
              return True
        return False

    def check_seven_aggrigator_bet_exists(self, message):
        result = self.db.engine.execute(sql_text("select id, payment_strategy,"\
            " amount_small, request_name, amount, reference_id, "\
            "aggregator_status, payment_id, status, user from "\
            "seven_aggregator_request where user=:pid and payment_id=:id"),
             {'id': message.get("paymentId"), "pid":message.get("user")})\
             .fetchone()
        if result:
           sid, payment_strategy, amount_small, request_name, \
           amount, reference_id, aggregator_status, \
           payment_id, status, user=result
           bal,bonus=self.get_account_balance(user)
           if sid and sid > 0:
              return True
        return False

    def send_message(self, message_body):
         msisdn = self.get_msisdn_for_profile(message_body.get('user'))
         message = "CONGRATULATIONS! you have won KES.{0} on Lucky six game"\
            " ReferenceId {1}.".format(message_body.get('amount'), 
            message_body.get('referenceId'))
         message_type='BULK'
         short_code=101010
         correlator=''
         link_id=''
         payload = urllib.urlencode({"message": message, 
            "msisdn":msisdn, "message_type":message_type, 
            "short_code":short_code, "correlator":correlator, 
            "link_id":link_id})
         self.send_notification(payload)

    def award_scorepesa_points(self, message_body, tranx_id):
         testing = True
         if int(self.scorepesa_points_cfgs['enable_scorepesa_points_testing']) == 1:
             testing = None
             if str(profile_id) in \
                 self.scorepesa_points_cfgs["tests_whitelist"].split(','):
                  testing = True
         if testing is not None:
             connection=self.db.engine.connect()
             profile_id=message_body.get('user')
             bpoint = ScorepesaPoint(self.logger, profile_id, connection)
                          
             scorepesa_points = \
                float(message_body.get('amount'))/float(
                    self.scorepesa_casino_cfgs['lucky6_points_divider'])
             if scorepesa_points > float(
                    self.scorepesa_casino_cfgs['lucky6_points_per_bet_limit']):
                 scorepesa_points=float(
                    self.scorepesa_casino_cfgs['lucky6_points_per_bet_limit'])

             daily_points = bpoint.check_daily_max_points()
             self.logger.info("Scorepesa points award lucky6 :: bpoints:: {0}"
                " :: dailypoints {1} ....".format(scorepesa_points, daily_points))

             if (daily_points+scorepesa_points) >= float(
                 self.scorepesa_casino_cfgs['lucky6_points_daily_limit']):
                 remainder_today = float(
                    self.scorepesa_casino_cfgs['lucky6_points_daily_limit'])\
                    - daily_points
                 if remainder_today > 0.0:
                     scorepesa_points = remainder_today
             else:
                 return None
      
             if scorepesa_points:
                 #push to scorepesa points queue
                 message={
                  "profile_id":message_body.get('user'),
                  "points":scorepesa_points,
                  "bet_slips": message_body.get('ticketInfo'),
                  "bet_total_odd":bet_total_odd,
                  "stake_amount":float(message_body.get('amount')),
                  "bet_type":'Luckysix',
                  "transaction_id":tranx_id,
                  "reqtype": "Luckysix",
                  "bet_id": message_body.get('referenceId')
                 }
                 queue_name="SCOREPESA_POINTS_AWARD_Q"
                 exchange_name="SCOREPESA_POINTS_AWARD_X"
                 dead_letter_x = "SCOREPESA_POINTS_DEAD_X"
                 dead_letter_k = "SCOREPESA_POINTS_DEAD_R"
                 routing_key=""
                 pub = SendSmsPublisher(queue_name, exchange_name)
                 result = pub.publishBpoint(message, routing_key, 
                    dead_letter_x, dead_letter_k)
                 #betpoint.process_scorepesa_points(trxd_id, points=scorepesa_points)
         return True

    def get_msisdn_for_profile(self, profile_id):
        msisdn = None
        sqlQ = "select msisdn from profile where profile_id=:profile"
        result = self.db.engine.execute(sql_text(sqlQ), 
            {'profile': profile_id}).fetchone()
        if result and result[0]:
            msisdn = result[0]
            self.logger.info("got profile {0} for msisdn {1} :: sql {2}"
                " :: result :: {3}".format(profile_id, msisdn, sqlQ, result))
            return msisdn

    def send_notification(self, payload):
        url = "http://127.0.0.1:8008/sendsms"

        headers = {
          'content-type': "application/x-www-form-urlencoded",
          'cache-control': "no-cache",
        }
        self.logger.info(
            "referals notifications payload:: {0}".format(payload))
        response = requests.request("POST", url, data=payload, headers=headers)
        self.logger.info(
            "referral send notification response :::: {0}".format(response))
        return response
