from datetime import datetime
from sqlalchemy.sql import text as sql_text
from utils import LocalConfigParser
from db.schema import Db, ScorepesaPointBet, JackpotBet, JackpotTrx, Transaction, BetSlip, Bet
from decimal import Decimal
from SendSmsPublisher import SendSmsPublisher
import requests


class ScorepesaPoint(object):

    APP_NAME = 'scorepesa_mo_consumer'

    def __del__(self):
        self.logger.info("Destroying ScorepesaPoints object.../")
        if self.db:
            self.db.close()
        if self.connection:
            self.connection=None
        if self.profile_id:
            self.profile_id=None
        if self.scorepesa_point_trx_id:
            self.scorepesa_point_trx_id=None

    def __init__(self, logger, profile_id, connection=None):
        self.logger = logger
        self.profile_id = profile_id
        self.points = None
        self.scorepesa_points_cfgs = LocalConfigParser.parse_configs("SCOREPESAPOINT")
        self.db_configs = LocalConfigParser.parse_configs("DB")
        self.scorepesa_configs = LocalConfigParser.parse_configs("SCOREPESA")
        self.scorepesa_point_trx_id = None
        self.app_name = "scorepesa_mo_consumer"
        self.db = None
        if connection is None:
           self.db = self.get_scorepesa_db()
           self.connection = self.db.engine.connect()
        else:
           self.connection = connection
        self.weekly_redeemed_points=self.check_weekly_max_points()
        self.scorepesa_points_bal, self.jp_bet_status = self.get_balance(), None
        super(ScorepesaPoint, self).__init__()

    def get_scorepesa_db(self):
        self.db = Db(self.db_configs['host'], self.db_configs['db_name'],
            self.db_configs['username'],
            self.db_configs['password'], socket=self.db_configs['socket'], port=self.db_configs['port'])
        return self.db

    def bet_points_award(self, trans_id, livebetting, profile_id, bet_slips, bet_total_odd, amount, bet_id, bet_type='prematch'):
         testing = True
         if int(self.scorepesa_points_cfgs['enable_scorepesa_points_testing']) == 1:
             testing = None
             if str(profile_id) in self.scorepesa_points_cfgs["tests_whitelist"].split(','):
                  testing = True
         #no scorepesa points for live betting 
         #enable livebetting set it False
         livebetting=False
         if testing is not None and not livebetting:
             scorepesa_points = self.scorepesa_points_to_award(bet_slips, bet_total_odd, amount, bet_type=bet_type)
             if scorepesa_points:
                 #push to scorepesa points queue
                 message={
                  "profile_id":profile_id,
                  "points":scorepesa_points,
                  "bet_slips": bet_slips,
                  "bet_total_odd":bet_total_odd,
                  "stake_amount":float(amount),
                  "bet_type":bet_type,
                  "transaction_id":trans_id,
                  "reqtype": bet_type,
                  "bet_id": bet_id
                 }
                 queue_name="SCOREPESA_POINTS_AWARD_Q"
                 exchange_name="SCOREPESA_POINTS_AWARD_X"
                 dead_letter_x = "SCOREPESA_POINTS_DEAD_X"
                 dead_letter_k = "SCOREPESA_POINTS_DEAD_R"
                 routing_key=""
                 pub = SendSmsPublisher(queue_name, exchange_name)
                 result = pub.publishBpoint(message, routing_key, dead_letter_x, dead_letter_k)
                 #betpoint.process_scorepesa_points(trxd_id, points=scorepesa_points)
         return True

    def get_balance(self):
        bal = None
        if self.profile_id:
            bal = self.connection.execute(
                sql_text("select points from scorepesa_point where profile_id = :value"),
                {'value':self.profile_id}).fetchone()
        if bal:
            self.points = float(bal[0])
        else:
            self.points =  0.0
        self.logger.info("getting points balance query......{0}::{1}::bal::{2}".format(bal,self.profile_id, self.points))
        return self.points
        

    def add_points(self, points, transaction_id):
        trans = self.connection.begin() 
        daily_points = self.check_daily_max_points()

        self.logger.info("scorepesa points award check max profileId::{2} :: bpoints:: {0} :: dailypoints {1} ....".format(points, daily_points, self.profile_id))

        if (daily_points+float(points)) >= float(self.scorepesa_points_cfgs['scorepesa_points_daily_max']):
            remainder_today = float(self.scorepesa_points_cfgs['scorepesa_points_daily_max'])-daily_points
            if remainder_today > 0.0:
               points = remainder_today
            else:
               return None
        try:
           updateQ = "insert into scorepesa_point (profile_id,points,redeemed_amount,created_by,status,created) values(:profile_id,:points,0,'scorepesa_mo_consumer','ACTIVE',now()) on duplicate key update points=points+:points"
           self.logger.info("POINTS SQL: %s, %s, %s " % (self.profile_id, points, updateQ))
           scorepesa_trx = "insert into scorepesa_point_trx (trx_id,points,trx_type,status,created,modified) values(:trx_id,:points,:trx_type,:status,:created,:modified)"
           self.connection.execute(sql_text(updateQ), {'profile_id':self.profile_id, 'points':float(points)})

           self.logger.info("Transaction acquire scorepesa points :: profile_id::{1} :: {0}".format(transaction_id, self.profile_id))
           points_trx = {"trx_id":transaction_id, "points":float(points), "trx_type":'CREDIT', "status":'GAIN', "created":datetime.now(), "modified":datetime.now()}
           trx_res = self.connection.execute(sql_text(scorepesa_trx), points_trx)
           trans.commit()
           return True
        except Exception, e:
           trans.rollback()
           raise
        
    def get_amount_on_redeemed_amount(self, points):
         rate = int(self.point_cfgs['points_redeem_rate']) or 50
         return float(points*(rate/100))

    '''
     Pass connection to ensure transaction consistent
    '''
    def redeem(self, points, connection, reference, ref_desc, trx_id, redeemed_amount=None):
        try:
           #redeemed_amount = self.get_amount_on_redeemed_amount(points)
           updateQ = "update scorepesa_point set points=points-{0}, redeemed_amount=redeemed_amount+{1} where  profile_id = :value".format(points, redeemed_amount)
           #trxQ = "insert into transaction (profile_id ,account,iscredit ,reference ,amount ,running_balance ,created_by ,created ,modified) values(:profile_id ,:account,:iscredit ,:reference ,:amount ,:running_balance ,:created_by ,:created ,:modified)"
           scorepesa_trx = "insert into scorepesa_point_trx (trx_id,points,trx_type,status,created,modified) values(:trx_id,:points,:trx_type,:status,:created,:modified)"

           connection.execute(sql_text(updateQ), {'value':self.profile_id})
           '''
           trx_dict = {"profile_id": self.profile_id,
                       "account": "{0}-{1}".format(self.profile_id, 'VIRTUAL'),
                       "iscredit": 0, 
                       "reference": "{0}-{1}".format(reference, ref_desc),
                       "amount": 0,
                       "running_balance": 0, 
                       "created_by": 'Scorepesa_Point', 
                       "created": datetime.now(), 
                       "modified":datetime.now()
           }
           self.logger.info("in redeem create transaction map......")
           trx_res = connection.execute(sql_text(trxQ), trx_dict)
           
           trx_id = trx_res.lastrowid
           '''

           if not trx_id:
              return False

           self.logger.info("Transaction redeem result id:: {0}".format(trx_id))
           trxx_dict = {"trx_id":trx_id, 
                       "points":points, 
                       "trx_type":'DEBIT', 
                       "status":'REDEEM', 
                       "created":datetime.now(), 
                       "modified":datetime.now()
           }
           self.logger.info("scorepesa trx now .... {0}:: dict ::{1}.....".format(scorepesa_trx, trxx_dict))
           betpointtrx_res = connection.execute(sql_text(scorepesa_trx), trxx_dict)
           self.scorepesa_point_trx_id = betpointtrx_res.lastrowid
  
           return True
        except Exception, e:
           raise

    def check_bet_cancelled(self, bet_id):
         sQ = "select b.status, b.profile_id, b.bet_id from bet b where b.profile_id=:pf and b.status=24 and b.bet_id=:bet"
         bcancels = self.connection.execute(sql_text(sQ), {'pf': self.profile_id, 'bet': bet_id}).fetchone()
         self.logger.info("get bet cancellations :: {0}".format(bcancels))
         if bcancels and int(bcancels[0]) == 24:
            return True
         return False

    '''
      Normal bets formula = bet_amount*no.of matches/10
      Early bird bets formula = (bet_amount*no.of matches/10)*2
    '''
    def scorepesa_points_to_award(self, bet_slips, bet_total_odd, amount, bet_type='prematch'):
        self.logger.info("Awarding points now.... {0} :: oddtotal {1} :: profile_id :: {2}".format(len(bet_slips), bet_total_odd, self.profile_id))
        noww = datetime.now()
        multiply_factor = float(bet_total_odd)
        #float(len(bet_slips))#float(self.scorepesa_points_cfgs['points_multiply_factor'])
        divide_factor = float(self.scorepesa_points_cfgs['points_divide_factor'])
        divide_factor_early_bird = float(self.scorepesa_points_cfgs['early_bird_points_divide_factor'])
        triple_divide_factor = float(self.scorepesa_points_cfgs['triple_divide_factor'])
        points_per_bet = float(self.scorepesa_points_cfgs['scorepesa_points_per_bet'])

        if float(bet_total_odd) < float(self.scorepesa_points_cfgs['bet_scorepesa_point_min_odd']):
           return None
        if str(noww.hour) in self.scorepesa_points_cfgs['scorepesa_point_early_bet_hours'].split(','):
           points = (float(amount) * multiply_factor)/divide_factor_early_bird
           #set max per bet points
           bpoints = points_per_bet if float(points) >= points_per_bet else float(points)
        else:
           points = (float(amount) * multiply_factor)/divide_factor
           #overide with tripple divide factor if set
           if triple_divide_factor != 0.0:
              points = float(amount) * multiply_factor/triple_divide_factor
           #set max per bet points
           bpoints = points_per_bet if float(points) >= points_per_bet else float(points)
      
        daily_points = self.check_daily_max_points() 

        self.logger.info("Scorepesa points award :: bpoints:: {0} :: dailypoints {1} ....".format(bpoints, daily_points))
 
        if (daily_points+bpoints) >= float(self.scorepesa_points_cfgs['scorepesa_points_daily_max']):
            remainder_today = float(self.scorepesa_points_cfgs['scorepesa_points_daily_max'])-daily_points
            if remainder_today > 0.0:
               bpoints = remainder_today
            else:
               return None        
        return bpoints   

    def process_scorepesa_points(self, transaction_id, points=1, reqtype='BET'):
        #BET #Deposit #Referral
        #if reqtype.lower() == 'bet':
        return self.add_points(points, transaction_id)
        #else:
        #   return "Earn scorepesa loyalty points by placing a Bet, Depositing, Referring your friends to Scorepesa. T&Cs Apply."

    def check_daily_max_points(self):
        result = self.connection.execute(
                sql_text("select sum(bt.points) as points from scorepesa_point_trx bt inner join transaction t on bt.trx_id=t.id where t.profile_id=:pid and date(bt.created)=curdate()"), {'pid':self.profile_id}).fetchone()
        self.logger.info("check daily points result:: {0}".format(result))
        if result:
           points, = result
           if points is None:
              points=0               
           self.logger.info("daily redeemed scorepesa points :: {0}".format(points))
        return float(points)         

    def check_weekly_max_points(self):
        weekly_redeemed_points=0.0
        result = self.connection.execute(
                sql_text("select sum(bt.points) as points from scorepesa_point_trx bt inner join transaction t on bt.trx_id=t.id where t.profile_id=:pid and WEEKOFYEAR(bt.created) >= WEEKOFYEAR(NOW()) and bt.status='REDEEM'"), {'pid':self.profile_id}).fetchone()
        self.logger.info("check weekly points result :: {0}".format(result))
        if result:
           weekly_redeemed_points, = result
           if weekly_redeemed_points is None:
              weekly_redeemed_points=0.0
           self.logger.info("current week redeemed scorepesa points :: {0}".format(weekly_redeemed_points,))
        return weekly_redeemed_points

    def bet_is_free_jp(self, bet_id):
        bet_id = None
        result = self.connection.execute(
                sql_text("select bet_id from scorepesa_point_bet where bet_id = :betid"), {'betid':bet_id}).fetchone()
        self.logger.info("got free jp bet result :: {0}".format(result))
        if result:
           bet_id, = result
        self.logger.info("bet id got was......{0}".format(bet_id))    
        return bet_id

     #bet_message, redeem_amount, bet_slips, bet_on_balance, app=app_name, jp=jpEventId
    def place_points_bet(self, bet_string, redeem_amount, slips, bet_on_balance, totalOdd=None, possibleWin=None, app=None, jp=None):        
        connection = self.connection
        app_name=app
        game_count=len(slips)
        profile_id=self.profile_id

        self.scorepesa_points_bal = self.get_balance()
        bet_status = 1
        if totalOdd is None:
           totalOdd = 1 
        self.logger.info("in place scorepesa point bet .....")
        bet_slips = slips

        weekly_points_limit = self.scorepesa_points_cfgs['weekly_points_redeem_limit']
        if float(self.weekly_redeemed_points) > float(weekly_points_limit):
            self.jp_bet_status = 421
            return False
        
        trans = connection.begin()
        try:
            bet_dict = {
                "profile_id": self.profile_id,
                "bet_message": bet_string,
                "bet_amount": bet_on_balance,
                "total_odd": Decimal(totalOdd),
                "possible_win": possibleWin if not jp else redeem_amount,
                "status": bet_status if not jp else 9,
                "reference": 'HYBRID_JACKPOT',
                "win": 0,
                "created_by": app_name,
                "created": datetime.now(),
                "modified": datetime.now()
            }
            bet = connection.execute(Bet.__table__.insert(), bet_dict)
            trace_id = bet_id = bet.inserted_primary_key[0]
            self.logger.info("created scorepesa point bet .....betId {0}".format(bet_id))
            slip_data = []
            for slip in bet_slips:
                bet_slip_dict = {
                    "parent_match_id": slip.get("parent_match_id"),
                    "bet_id": trace_id,
                    "bet_pick": slip.get("pick"),
                    "special_bet_value": slip.get("special_bet_value") if slip.get("special_bet_value") else '',
                    "total_games": game_count,
                    "odd_value": slip.get("odd_value"),
                    "win": 0,
                    "live_bet": 0,#slip.get("bet_type"),
                    "created": datetime.now(),
                    "status": 1,
                    "sub_type_id": slip.get("sub_type_id")
                }
                slip_data.append(bet_slip_dict)

            connection.execute(BetSlip.__table__.insert(), slip_data)

            self.logger.info("Created scorepesa point betslip .....")

            #roamtech_id = self.get_roamtech_virtual_acc('ROAMTECH_VIRTUAL')
            trx_debit_dict = {
                "profile_id": profile_id,
                "account": "%s_%s" % (profile_id, 'VIRTUAL'),
                "iscredit": 0,
                "reference": "{0}_{1}".format(bet_id, 'HybridJPBet'),
                "amount": bet_on_balance,
                "created_by": app_name,
                "created": datetime.now(),
                "modified": datetime.now()
            }
            trxd = connection.execute(Transaction.__table__.insert(), trx_debit_dict)
            trxd_id = trxd.inserted_primary_key[0]

            self.logger.info("Created scorepesa point transaction .....TrxId {0}".format(trxd_id))

            #since in future we could have bets on non jackpot
            if jp:
                self.update_jackpot_bet(bet_id, jp, trxd_id, connection)
            
            ref_desc="HybridJPBetRedeem"

            self.logger.info("Scorepesa point bet updated Jackpot bet transaction.....")
            self.logger.info("scorepesa points balance before :: {0}".format(self.scorepesa_points_bal))
            points = float(self.scorepesa_points_cfgs['free_jp_points_deduct'])
            if redeem_amount > 50.0:
               points = float((redeem_amount/50.0)*float(self.scorepesa_points_cfgs['free_jp_points_deduct']))

            #insufficient
            if float(self.scorepesa_points_bal) < float(points):
                self.jp_bet_status = 424            
                return False

            self.logger.info("Scorepesa point bet/ point 2 redeem {0}/amount redeeemed {1}/".format(points, redeem_amount))
            if not self.redeem(points, connection, bet_id, ref_desc, trxd_id, redeem_amount):
                self.jp_bet_status = 423
                return False

            #deduct cash if cash and points hybrid bet
            if jp:
                self.deduct_bet_stake_amount(profile_id, bet_on_balance, connection)

            #Create ScorepesaPointBet
            self.scorepesa_point_bet(bet_id, trxd_id, points, app_name, redeem_amount, connection)

            bpoints_bal = float(self.scorepesa_points_bal)-float(points)
            self.logger.info("scorepesa points balance before :: {0} ::: after :: {1}".format(self.scorepesa_points_bal, bpoints_bal))
            self.scorepesa_points_bal = bpoints_bal

            self.logger.info("Transaction saved success betID::{0}::redeemed points::{1} ...redeemAmount:: {2}".format(trace_id, points, redeem_amount))
            trans.commit()
            return bet_id
        except Exception as e:
            trans.rollback()
            self.logger.error("Transaction creating scorepesa points bet, rolled back :: {0} ...".format(e))
            return False

    def update_jackpot_bet(self, bet_id, jackpot_event_id, trx_id, connection):
        try:
            jp_bet_dict = {
               "bet_id": bet_id,
               "jackpot_event_id": jackpot_event_id,
               "status": 'ACTIVE',
               "created": datetime.now(),
               "modified": datetime.now()
            }
            jp_trx_dict = {
               "trx_id": trx_id,
               "jackpot_event_id": jackpot_event_id,
               "created": datetime.now(),
               "modified": datetime.now(),
            }
            connection.execute(JackpotBet.__table__.insert(), jp_bet_dict)
            connection.execute(JackpotTrx.__table__.insert(), jp_trx_dict)
        except Exception, e:
            raise

    def deduct_bet_stake_amount(self, profile_id, bet_on_balance, connection):
        try:
           sql="UPDATE profile_balance SET balance=balance-{0} WHERE profile_id=:prf LIMIT 2".format(bet_on_balance)
           pars = {"prf": profile_id}
           connection.execute(sql_text(sql), pars)
        except Exception, e:
           raise


    #bet_id, trxd_id, points, app_name, redeem_amount, connection
    def scorepesa_point_bet(self, bet_id, trxd_id, points, app_name, redeemed_amount, connection):
        try:
            bet_dict = {
               "bet_id": bet_id,
               "scorepesa_point_trx_id": self.scorepesa_point_trx_id,
               "points": points,
               "amount": redeemed_amount,
               "created_by": app_name,
               "created": datetime.now(),
               "modified": datetime.now()
            }
            connection.execute(ScorepesaPointBet.__table__.insert(), bet_dict)
        except Exception,e:
            raise

    def get_roamtech_virtual_acc(self, acc):
        if acc == 'ROAMTECH_MPESA':
            if 'mpesa_roamtech_profile_id' in self.scorepesa_configs:
                return self.scorepesa_configs['mpesa_roamtech_profile_id']
            return 5

        if 'virtual_roamtech_profile_id' in self.scorepesa_configs:
            return self.scorepesa_configs['virtual_roamtech_profile_id']
        return 6


    def transaction_for_bonus_scorepesa_point_award(self, profile_id, req_type='scorepesa_points_bonus'):
         try:
            trxQ = "insert into transaction (profile_id ,account,iscredit ,reference ,amount ,running_balance ,created_by ,created ,modified) values(:profile_id ,:account,:iscredit ,:reference ,:amount ,:running_balance ,:created_by ,:created ,:modified)"

            trx_dict = {
               "profile_id": profile_id,
               "account": "{0}-{1}".format(profile_id, 'VIRTUAL'),
               "iscredit": 1,
               "reference": "{0}-{1}".format(profile_id, req_type),
               "amount": 0,
               "running_balance": 0,
               "created_by": req_type,
               "created": datetime.now(),
               "modified":datetime.now()
            }
            self.logger.info("in transaction for use in bonus points award......")
            trx_res = self.connection.execute(sql_text(trxQ), trx_dict)
            trx_id = trx_res.lastrowid
            return trx_id
         except Exception, e:
            self.logger.error("Exception transaction scorepesa point bonus...{0}".format(e))
            return False


    def send_notification(self, payload):
        url = "http://127.0.0.1:8008/sendsms"

        headers = {
          'content-type': "application/x-www-form-urlencoded",
          'cache-control': "no-cache",
        }
        self.logger.info("referals notifications payload:: {0}".format(payload))
        response = requests.request("POST", url, data=payload, headers=headers)
        self.logger.info("referral send notification response :::: {0}".format(response))
        return response

