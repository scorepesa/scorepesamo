import time
from datetime import datetime
from sqlalchemy.sql import text as sql_text
from utils import LocalConfigParser
from db.schema import Db, BonusTrx, ProfileBonu
from datetime import datetime, timedelta
import requests
import urllib

class ReferralBonus(object):

    APP_NAME = 'scorepesa_mo_consumer'

    def __del__(self):
        self.logger.info("Destroying ScorepesaBonus object.../")
        if self.db:
            self.db.close()
        if self.connection:
            self.connection=None            

    def __init__(self, logger, profile_id, connection=None):
        self.scorepesa_bonus_cfgs = LocalConfigParser.parse_configs("SCOREPESABONUS")
        self.db_configs = LocalConfigParser.parse_configs("DB")
        self.logger = logger
        self.profile_id = profile_id
        self.referal_bonus_expired = False
        self.no_referal_bonus = True
        self.referal_bonus_extra = ''
        self.bonus_adjust_to_stake = False
        self.new_bonus_balance_amount = 0.0
        self.current_bonus = None
        self.daily_bonus_claimed_notify=False
        self.daily_bonus_claimed = 0.0

        if connection is None:
           self.db = self.get_scorepesa_db()
           self.connection = self.db.engine.connect()
        else:
           self.connection = connection
        super(ReferralBonus, self).__init__()

    def get_scorepesa_db(self):
        self.db = Db(self.db_configs['host'], self.db_configs['db_name'],
            self.db_configs['username'],
            self.db_configs['password'], socket=self.db_configs['socket'], port=self.db_configs['port'])
        return self.db
    
    def profile_bonus_adjust_referal_bonus(self, profile_id, msisdn, profile_bonus_id, amount, referred_msisdn, created_by='referral_message_re_award'):
        #check limits b4 proceed to award bonus to profile
        if self.check_bonus_daily_limits(msisdn, profile_id, referred_msisdn):
            return

        trxx = self.connection.begin()
        try:
           pb_status = 'CLAIMED'
           pbQ = """update profile_bonus set bet_on_status=10, bonus_amount={0}, status=:st, updated=:ddate, created_by=:cb where profile_id=:profile_id and profile_bonus_id=:id""".format(amount)
           params = {"profile_id": profile_id, "id": profile_bonus_id, "st": pb_status, "ddate": datetime.now(), "cb": created_by}
           self.logger.info("update referal bonus re-award query {0} :: params :: {1}: new bonus bal: {2}".format(pbQ, params, amount))
           self.connection.execute(sql_text(pbQ), params)

           bonus_trx_dict = {
                "profile_id":profile_id,
                "profile_bonus_id":profile_bonus_id,
                "account":"%s_%s" % (profile_id, 'VIRTUAL'),
                "iscredit":1,
                "reference":'{0}_{1}'.format(profile_bonus_id, time.strftime('%Y%m%d%H%M%S')),
                "amount":amount,
                "created_by": created_by,
                "created":datetime.now(),
                "modified":datetime.now()
           }
           #update bonus balance to the provided amount
           self.connection.execute(BonusTrx.__table__.insert(), bonus_trx_dict)
           balUpdate = """INSERT INTO profile_balance(profile_id, balance, transaction_id, created, bonus_balance)
            VALUES (:pf, :amount, :trx_id, NOW(), :bonus_bal) ON DUPLICATE KEY UPDATE  bonus_balance = (bonus_balance+%0.2f)""" % (amount)
           prm = {'pf':profile_id, 'amount':0.0, 'trx_id':-1, 'bonus_bal': amount}
           self.connection.execute(sql_text(balUpdate), prm)
           self.logger.info("update profile balance referral bonus bal award query {0} :: params :: {1}".format(balUpdate, prm))
           trxx.commit()
           return True
        except Exception as e:
           trxx.rollback()
           self.logger.error("Referral bonus award exception :: {0} ::".format(e))
           return False

    def check_bonus_daily_limits(self, msisdn, profile_id, referred_msisdn):
         try:
             #check referrals count limit
             referrals_count = self.check_daily_referrals_count(profile_id)
             self.logger.info("Referral bonus referrals count {0}::recipient profile_id::{1}::referred msisdn::{2}[][]".format(referrals_count, profile_id, referred_msisdn))
             message_type='BULK'
             short_code=101010
             correlator=''
             link_id=''

             if int(referrals_count) > int(self.scorepesa_bonus_cfgs['referral_bonus_daily_limit_count']):
                    message = 'Sorry you have exhausted your daily referrals limit, please try again tomorrow.'
                    #send notification
                    payload = urllib.urlencode({"message": message, "msisdn":msisdn, "message_type":message_type, "short_code":short_code, "correlator":correlator, "link_id":link_id})
                    self.send_notification(payload)
                    return True
             return False
         except Exception, e:
             self.logger.error("Exception on check referral bonus limits() {0}".format(e))
             return False
       
    '''
Conditions:
 1.bet_on_bonus > 0
 2.Has profile_bonus referal bonus in CLAIMED status
 NOTE: 
   Pre-looping condition:
    Check if daily bonus limit exeeded to avoid over awarding.
 3.If 2 is true fetch all and loop through re-awarding bonus 
   based on configured max re-award per referal based on referal bet amount.  
 4.Incase of en Exception in 3 the profile_bonus status is updated to CANCELLED status and profile bonus revoked respectively.
 5.If it 3 succeed then the profile_bonus is re-awarded via update bonus_amount=re-awarded-bonus 
   and profile_balance bonus_balance=(bonus_balance+re-awarded-bonus).
    '''
    def apply_referal_bonus_on_bet(self, profile_id, amount, total_odd):
        stake = float(amount)
        if float(total_odd) < float(self.scorepesa_bonus_cfgs['referral_bet_min_odd']):
            return
        msisdn = self.get_msisdn_for_profile(profile_id)
        if msisdn is None:
           return
        self.logger.info("in re-awarding referal bonus.....msisdn:{0}::profile_id::{1}:stake:{2}::totalodd::{3}".format(msisdn, profile_id, stake, total_odd))
        try:
            result = self.connection.execute(sql_text("""
            select profile_bonus_id, referred_msisdn, bonus_amount, profile_id, expiry_date, date_created from profile_bonus where referred_msisdn=:rfmsisdn and status in ('CLAIMED','EXPIRED') and created_by=:cb and date_created > DATE_SUB(NOW(), INTERVAL 3 DAY)"""), {'rfmsisdn': msisdn, 'cb': 'referral_message'}).fetchone()
            if result:
               self.logger.info("got a referal bonus.....profile:{0}::stake:{1}::total_odd::{2}::msisdn::{3}".format(profile_id, stake, total_odd, msisdn))
               #unset to avoid confusion incase of multiple bonus claim adjustment
               profile_bonus_id = result[0]
               referred_msisdn = result[1]
               claimer_profile_id = result[3]
               bamount = result[2]
               created_date = result[5]
               #check referrals count limit
               referrals_count = self.check_daily_referrals_count(claimer_profile_id)
               self.logger.info("Referral bonus referrals count {0}::recipient profile_id::{1}..".format(referrals_count, claimer_profile_id))
               if int(referrals_count) > int(self.scorepesa_bonus_cfgs['referral_bonus_daily_limit_count']):
                   msisdn = self.get_msisdn_for_profile(claimer_profile_id)
                   message = 'Sorry you have reached the daily referrals limit, please try again tomorrow.' 
                   message_type='BULK'
                   short_code=101010
                   correlator=''
                   link_id=''
                   payload = urllib.urlencode({"message": message, "msisdn":msisdn, "message_type":message_type, "short_code":short_code, "correlator":correlator, "link_id":link_id})
                   self.send_notification(payload)
                   return

               #if expiry_date > 72 hours then cancel the bonus award
               if created_date < datetime.now()-timedelta(days=3):
                     self.logger.info("bonus expired and have 2 flag as cancel and return to proceed with bet::{0} ...".format(profile_bonus_id))
                     self.profile_bonus_flag_created_by(claimer_profile_id, profile_bonus_id, status='CANCELLED')
                     self.referal_bonus_expired = True
                     self.logger.info("bonus_amount::{4}::profile:{0}::stake:{1}::total_odd::{2}::msisdn::{3}::".format(profile_id, stake, total_odd, msisdn, bamount))
                     return

               new_bamount = self.check_if_profile_has_bet(msisdn)
               if new_bamount is None:
                  new_bamount = stake

               if float(new_bamount) > float(self.scorepesa_bonus_cfgs['max_referal_bonus_re_award']):
                   new_bamount = float(self.scorepesa_bonus_cfgs['max_referal_bonus_re_award'])

               try:
                   if self.check_bonus_daily_award_limit(claimer_profile_id, new_bamount):
                      #update profile_bonus to new bonus based on bet_amount
                      self.current_bonus,bal = self.get_profile_account_balance(claimer_profile_id)
                      self.logger.info("claimer :: {2} :: current bonus bal :: {0} :: account bal :: {1}".format(self.current_bonus, bal, claimer_profile_id))
                      self.logger.info("bonus adjustment ....... {0}".format(new_bamount))
                      self.profile_bonus_adjust_referal_bonus(claimer_profile_id, profile_bonus_id, new_bamount)
                      self.logger.info("referral bonus adjust claimer::{0}::bonus bal::{1}::newbamount: {2}".format(claimer_profile_id, self.current_bonus, new_bamount))
                   else:
                      self.logger.info("limit reached time to notify.....")
                      #flag profile_bonus to avoid reprocessing of same bonus re-award
                      self.daily_bonus_claimed_notify = True
                      msisdn = self.get_msisdn_for_profile(claimer_profile_id)
                      message = 'Sorry could not award bonus since you have reached the daily referrals limit, please try again tomorrow.'
                      message_type='BULK'
                      correlator=''
                      link_id=''
                      short_code=101010
                      payload = urllib.urlencode({"message": message, "msisdn":msisdn, "message_type":message_type, "short_code":short_code, "correlator":correlator, "link_id":link_id})
                      notify_result=self.send_notification(payload)
                      self.logger.info("ignore due to daily limit re-processing anotherday if not expire ::bonus:: {0} ::daila claimed::{1}::{2}".format(bamount, self.daily_bonus_claimed, notify_result))
               except Exception, e:
                   self.logger.error("Re-award failed proceed with bet... exception :: {0} ::".format(e))
            self.logger.info("...its a wrap ..... proceeding with bet. Dailybonus::{0}::expired::{1}".format(self.daily_bonus_claimed_notify, self.referal_bonus_expired))
            return
        except Exception, ex:
            self.logger.error("Exception.... proceed with bet normally:: {0} ::".format(ex))

    def award_referral_bonus_after_bet(self, profile_id, amount, total_odd):
        msisdn = self.get_msisdn_for_profile(profile_id)
        if msisdn is None:
           return
        self.logger.info("in award referal bonus.....msisdn:{0}::profile_id::{1}:stake:{2}::totalodd::{3}".format(msisdn, profile_id, amount, total_odd))
        try:
            result = self.connection.execute(sql_text("""
            select profile_bonus_id, referred_msisdn, bonus_amount, profile_id, expiry_date, date_created, bet_on_status from profile_bonus where referred_msisdn=:rfmsisdn and status in ('NEW','CLAIMED') and created_by=:cb"""), {'rfmsisdn': msisdn, 'cb': 'referral_message'}).fetchone()
            #date_created > DATE_SUB(NOW(), INTERVAL 3 DAY)
            if result:
               self.logger.info("got a referal bonus...profile:{0}::stake:{1}::total_odd::{2}::msisdn::{3}".format(profile_id, amount, total_odd, msisdn))
               profile_bonus_id = result[0]
               referred_msisdn = result[1]
               claimer_profile_id = result[3]
               bamount = result[2]
               created_date = result[5]
               bet_on_status=result[6]
               claimer_msisdn = self.get_msisdn_for_profile(claimer_profile_id)

               if bet_on_status==10:
                   self.logger.info("bonus already awarded ignoring award claimer[][]{0}[]referred[]{1}[][]".format(claimer_profile_id, referred_msisdn))
                   return

               self.logger.info("profileclaimer[]{0}[]referredmsisdn[]{1}[]profilebonusid[]{2}[]betamount[]{3}[]created[]{4}[][]".format(claimer_profile_id,referred_msisdn,profile_bonus_id,bamount,created_date))

               #if expiry_date > 72 hours then cancel the bonus award and notify
               if created_date < datetime.now()-timedelta(days=3):
                   self.logger.info("bonus expired and have 2 flag as cancel and return to proceed with bet::{0} ...".format(profile_bonus_id))
                   self.profile_bonus_flag_created_by(claimer_profile_id, profile_bonus_id, status='CANCELLED')
                   #self.referal_bonus_expired = True
                   self.logger.info("referals daily limit claimer profile:{0}::claimer msisdn::{1}::".format(claimer_profile_id, claimer_msisdn))
                   message = 'Sorry your referral bonus for {0} is expired, expiry is after 72 hours. Scorepesa T&Cs apply.'.format(referred_msisdn)
                   message_type='BULK'
                   correlator=''
                   link_id=''
                   short_code=101010
                   payload = urllib.urlencode({"message": message, "msisdn":claimer_msisdn, "message_type":message_type, "short_code":short_code, "correlator":correlator, "link_id":link_id})
                   self.send_notification(payload)
                   return

               #check if referred placed a bet b4 proceed to award bonus
               bet = self.check_profile_has_bet(msisdn, profile_id)
               if bet:
                   #call award of bonus
                   return self.profile_bonus_adjust_referal_bonus(claimer_profile_id, claimer_msisdn, profile_bonus_id, 49, referred_msisdn, 'referral_message')
               else:
                   return
            else:
               #no referal bonus to award
               return   
        except Exception, ex:
            self.logger.error("Exception.... proceed with bet normally:: {0} ::".format(ex))
 
    def get_profile_account_balance(self, profile_id):
        bal = self.db.engine.execute(
                sql_text("select balance, bonus_balance from profile_balance where profile_id = :value"),
                {'value':profile_id}).fetchone()
        if bal and bal[0] is not None:
            available_bonus = float(bal[1]) if bal[1] is not None else float(0)
            balance, bonus = float(bal[0]), available_bonus
        else:
            balance, bonus = 0, 0
        self.logger.info("Available balance/bonus... :bal: {0} :bonus: {1}".format(balance, bonus))
        return bonus, balance
    
    def profile_bonus_cancel(self, profile_id, profile_bonus_id, amount):
        #connection = self.db.engine.connect()
        trxx = self.connection.begin()
        try:
           pbQ = """update profile_bonus set status=:bstatus, bet_on_status=:betstatus where profile_id=:profile_id and profile_bonus_id=:id"""
           params = {"profile_id": profile_id, "id": profile_bonus_id, "bstatus": "CANCELLED", "betstatus": 1}
           self.logger.info("update referal bonus re-award cancellation query {0} :: params :: {1}".format(pbQ, params))
           self.connection.execute(sql_text(pbQ), params)
           #to avoid mysql error on lessing
           if float(self.current_bonus) < float(amount):
               amount = self.current_bonus
           if float(amount) < 0.0:
               amount = 0.0

           balUpdate = """INSERT INTO profile_balance(profile_id, balance, transaction_id, created, bonus_balance)
            VALUES (:pf, :amount, :trx_id, NOW(), :bonus_bal)
            ON DUPLICATE KEY UPDATE  bonus_balance = (bonus_balance-%0.2f)""" % (amount)
           prm = {'pf':profile_id, 'amount':0.0, 'trx_id':-1, 'bonus_bal': 0.0}
           self.connection.execute(sql_text(balUpdate), prm)

           self.logger.info("update profile balance bonus bal cancellation query {0} :: params :: {1}".format(balUpdate, prm))
           trxx.commit()
           return True
        except Exception as e:
           trxx.rollback()
           self.logger.error("Re-award bonus cancellation exception :: {0} ::".format(e))
           return False

    def profile_bonus_flag_created_by(self, profile_id, profile_bonus_id, status=None):
        trxx = self.connection.begin()
        try:
           created_by = "referral_message_re_award"
           pbQ = """update profile_bonus set created_by=:created_by where profile_id=:profile_id and profile_bonus_id=:id"""
           params = {"profile_id": profile_id, "id": profile_bonus_id, "created_by": created_by}
           if status is not None:
              pbQ = """update profile_bonus set created_by=:created_by, status=:status where profile_id=:profile_id and profile_bonus_id=:id"""
              created_by = "referral_message"
              params = {"profile_id": profile_id, "id": profile_bonus_id, "created_by": created_by, "status": status}
           
           self.logger.info("update referal bonus re-award flag created by query {0} :: params :: {1}".format(pbQ, params))
           self.connection.execute(sql_text(pbQ), params)
           trxx.commit()
        except Exception as e:
           trxx.rollback()
           self.logger.error("Re-award bonus flag exception :: {0} ::".format(e))

    def check_profile_has_bet(self, msisdn, profile_id=None):
         try:
             profile_id = self.get_profile_id_for_msisdn(msisdn) if profile_id is None else profile_id
             self.logger.info("check if profile has bet [][] {0}".format(profile_id))
             sql = "select bet_id from bet where profile_id=:pfid order by bet_id asc limit 1"
             results = self.connection.execute(sql_text(sql), {'pfid': profile_id}).fetchone()
             self.logger.info("check if bet has bet results [][] {0} [][][]...".format(results))
             if results:
                self.logger.info("extract check if bet bet_id[][] {0}".format(results[0]))
                if results[0]:
                    self.logger.info("confirmed have bet proceed and award bonus....")
                    return True
             return False
         except Exception, ex:
             self.logger.error("check if profile has bet exception:: {0} ::".format(ex))
             return False

    def check_if_profile_has_bet(self, msisdn, profile_id=None):
         try:
             profile_id = self.get_profile_id_for_msisdn(msisdn) if profile_id is None else profile_id
             self.logger.info("got profile id {0}".format(profile_id))
             sql = "select bet_amount, bet_id from bet where profile_id=:pfid order by bet_id asc limit 10"
             results = self.connection.execute(sql_text(sql), {'pfid': profile_id}).fetchall()
             bet_amount=None
             levels =0 
             if results:
                for result in results:
                    self.logger.info("got bet detail :: {0}:level:{1}".format(result, levels))
                    sqll = "select ratio from bonus_bet where bet_id = :bet_id"
                    bonus_bet = self.connection.execute(sql_text(sqll), {'bet_id': result[1]}).fetchone()
                    self.logger.info("profile ::{0}::bet sql::{1}::bonusbet sql::{2}::ratio::{3}".format(profile_id, sql, sqll, bonus_bet))
                    bet_bonus_amount=None
                    if bonus_bet:
                       ratio, = bonus_bet
                       bet_bonus_amount = float(ratio)*float(result[0])  
                    if result and result[0] and bet_bonus_amount is None:
                       bet_amount = float(result[0])
                       break
                    else:
                       bet_amount = float(result[0]) - float(bet_bonus_amount)
                       if bet_amount > 0.0:
                          break                   
                    levels += 1
                self.logger.info("returning bet amount to award based :: {0}:levels:: {1}".format(bet_amount, levels))
                return bet_amount
         except Exception, ex:
             self.logger.error("Get profile bet history exception:: {0} ::".format(ex))
             return None

    def get_profile_id_for_msisdn(self, msisdn):
          profile_id = None
          sqlQ = "select profile_id from profile where msisdn=:msisdn"
          result = self.connection.execute(sql_text(sqlQ), {'msisdn': msisdn}).fetchone()
          if result and result[0]:
             profile_id = result[0]
          self.logger.info("got profile {0} for msisdn {1} :: sql {2} :: result :: {3}".format(profile_id, msisdn, sqlQ, result))
          return profile_id

    def get_msisdn_for_profile(self, profile_id):
          msisdn = None
          sqlQ = "select msisdn from profile where profile_id=:profile"
          result = self.connection.execute(sql_text(sqlQ), {'profile': profile_id}).fetchone()
          if result and result[0]:
             msisdn = result[0]
          self.logger.info("got profile {0} for msisdn {1} :: sql {2} :: result :: {3}".format(profile_id, msisdn, sqlQ, result))
          return msisdn

    def check_bonus_daily_award_limit(self, profile_id, re_award_amount):
         houurz = int(self.scorepesa_bonus_cfgs['limit_check_no_of_hours'])
         t_q = "select sum(bonus_amount) from profile_bonus where updated > DATE_SUB(NOW(), INTERVAL {0} HOUR) and status in ('CLAIMED','USED') and profile_id = :pf and created_by in ('referral_message', 'referral_message_re_award')".format(houurz)
         awarded_bonus = self.connection.execute(sql_text(t_q), {'pf': profile_id}).fetchone()
         self.logger.info("checking daily bonus awarded so far ..../::: {0} :sql: {1} :profile: {2}".format(awarded_bonus, t_q, profile_id))
         if awarded_bonus and awarded_bonus[0]:
            amount = float(re_award_amount)
            self.daily_bonus_claimed = amount
            if awarded_bonus[0] is not None:
               amount = float(awarded_bonus[0]) + float(re_award_amount)
               self.daily_bonus_claimed = amount
            if float(amount) <= float(self.scorepesa_bonus_cfgs['scorepesa_bonus_re_award_daily_limit']):
               return True
            return False
         return True

    def check_daily_referrals_count(self, profile_id):
        referrals=0
        hourz_limit = int(self.scorepesa_bonus_cfgs['limit_time_check_no_of_referred'])
        result = self.connection.execute(
                sql_text("select count(profile_bonus_id) as referrals from profile_bonus where profile_id=:pid and created_by='referral_message' and date_created > DATE_SUB(NOW(), INTERVAL {0} HOUR)".format(hourz_limit)), {'pid': profile_id}).fetchone()
        self.logger.info("daily referrals result:: {0}".format(result))
        if result:
           referrals, = result
           if referrals is None:
              referrals = 0
           self.logger.info("got daily referrals count :: {0}".format(referrals))
        return referrals

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


    def award_bonus_on_request(self, profile_id, msisdn, amount, bonus_type='customer_engage_bonus'):
        try:
            self.logger.info("award bonus {0}::{1}::{2}::{3}".format(profile_id, msisdn, amount, bonus_type))
            profile_bonus_dict = {
                "profile_id": profile_id,
                "referred_msisdn": msisdn,
                "bonus_amount": float(amount),
                "status":'CLAIMED',
                "expiry_date": datetime.now()+timedelta(days =1),
                "created_by": bonus_type,
                "bet_on_status": 1,
                "date_created": datetime.now(),
                "updated": datetime.now()
            }
            result_proxy = self.connection.execute(ProfileBonu.__table__.insert(), profile_bonus_dict)
            profile_bonus_id = result_proxy.inserted_primary_key
            self.logger.info("Profile bonus insert proxy.... {0}".format(profile_bonus_id))

            bonus_trx_dict = {
                "profile_id":profile_id,
                "profile_bonus_id": profile_bonus_id,
                "account":"%s_%s" % (profile_id, 'VIRTUAL'),
                "iscredit":1,
                "reference": profile_bonus_id,
                "amount": float(amount),
                "created_by": bonus_type,
                "created":datetime.now(),
                "modified":datetime.now()
            }
            #update bonus balance to the re-adjusted bonus balance based on bet stake amount of referrred friend
            self.connection.execute(BonusTrx.__table__.insert(), bonus_trx_dict)
            self.logger.info('Profile bonus created  for ..%s ::id:: %s ' % (msisdn, profile_bonus_id))

            #update profile for this dude to get bonus
            profileUpdate = """INSERT INTO profile_balance(profile_id, balance, bonus_balance, transaction_id, created) VALUES (:pf, 0, :amount, :trx_id, NOW()) ON DUPLICATE KEY UPDATE  bonus_balance = (bonus_balance+%0.2f)""" % (float(amount), )
            self.connection.execute(sql_text(profileUpdate), {'pf': profile_id, 'amount': float(amount), 'trx_id': -1})
            self.logger.info('Bonus amount kshs.%s awarded for.... %s ' % (amount, msisdn))
            #msisdn = self.get_msisdn_for_profile(claimer_profile_id)
            message = 'CONGRATULATIONS! You have been awarded a bonus of Kshs. %0.2f. www.scorepesasports.com.' % float(amount)
            message_type = 'BULK'
            short_code = 101010
            correlator = ''
            link_id = ''
            payload = urllib.urlencode({"message": message, "msisdn":msisdn, "message_type":message_type, "short_code":short_code, "correlator":correlator, "link_id":link_id})
            self.send_notification(payload)
        except Exception, ex:
            self.logger.info('Failed to award bonus %s::%r '% (msisdn, ex))
