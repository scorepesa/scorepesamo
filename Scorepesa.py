from utils import LocalConfigParser
from db.schema import Db, Profile, Bet, EventOdd, Match, \
GameRequest, Transaction, BetSlip, Outbox, Inbox, Withdrawal, \
MpesaRate, Competition, Sport, AccountFreeze, ProfileSetting, \
ProfileBalance, OddKeyAlia, ProfileBonu, BonusTrx, BonusBet,\
BonusBetCount, JackpotBet, JackpotEvent, JackpotMatch, JackpotTrx, \
JackpotType, JackpotWinner, Outcome, LiveOddsMeta, LiveOdds, MtsTicketSubmit,\
LiveMatch, BetDiscount
from datetime import datetime, timedelta
from Publisher import Publisher
from decimal import Decimal
from sqlalchemy.exc import IntegrityError
import re
import time
from sqlalchemy import func, asc
from sqlalchemy.orm.exc import NoResultFound
import os
from sqlalchemy.sql import text as sql_text
from SendSmsPublisher import SendSmsPublisher
from ScorepesaPoint import ScorepesaPoint
from ReferralBonus import ReferralBonus
from sqlalchemy import desc
from random import randint
import json


class Scorepesa(object):

    APP_NAME = 'scorepesa_api'

    def __del__(self):
        self.logger.info("Destroying Scorepesa object ... will attempt close db ")
        if self.db_session:
            self.db_session.remove()
        if self.db:
            self.db.close()
        self.inbox_id = None
        self.outbox_id = None
        if self.bet_transaction_id:
            self.bet_transaction_id=None
        if self.stake_on_cash:
            self.stake_on_cash=None
        if self.bet_transaction_id:
            self.bet_transactioni_id=None

    def __init__(self, logger):
        self.message_type = 'UNKNOWN'
        self.logger = logger
        self.jp_bet_status = 500
        self.bonus_balance_amount = None
        self.db_configs = LocalConfigParser.parse_configs("DB")
        self.scorepesa_configs = LocalConfigParser.parse_configs("SCOREPESA")
        self.withdrawal_configs = LocalConfigParser.parse_configs("WITHDRAWAL")
        self.scorepesa_points_cfgs = LocalConfigParser.parse_configs("SCOREPESAPOINT")
        self.scorepesa_freebet_cfgs = LocalConfigParser.parse_configs("FREEBET")
        self.scorepesa_bonus_cfgs = LocalConfigParser.parse_configs("SCOREPESABONUS")
        self.redis_configs = LocalConfigParser.parse_configs("SCOREPESAREDIS")
        self.logger.info("Reading Scorepesa process called ")
        self.default_sub_type_id = 1
        self.db, self.db_session = self.get_db_session()
        self.profile_id = None
        self.outright = None
        self.outright_bet = None
        self.outright_default_sub_type_id = 30
        self.balance, self.bonus = None, None
        self.stake_on_cash=None
        self.bet_transaction_id=None
        self.multibet_bonus_message = ""
        self.freebet_notification = ''
        self.free_bonus_advise=''
        self.free_bet_bonus_tnc=''
        self.bonus_bet_low_odd_msg=''
        self.bonusWin = float(0)
        self.possible_win = float(0)
        self.multibet_possible_win = None
        self.punter_single_limit = None
        self.punter_multibet_limit = None
        self.daila_pwin_limit = None
        self.referal_bonus_fail_notify=''
        self.referal_bonus_extra = ''
        self.livebetting = False
        self.betslipLen = 0
        self.referral_bonus_advise=False
        self.referral_bonus_advise_notification=''
        self.operator = None
        self.profile_no_network=False
        self.is_paid = False
        self.no_games_found=False
        self.custom_matches=False
        self.name = ''
        self.beshte_bet_id = None 
        super(Scorepesa, self).__init__()

    def get_db_session(self):
        self.db = Db(self.db_configs['host'], self.db_configs['db_name'],
            self.db_configs['username'], self.db_configs['password'], 
            socket=self.db_configs['socket'], port=self.db_configs['port'])

        self.db_session = self.db.get_db_session()

        return self.db, self.db_session

    def inbox_message(self, message):
        t0 = time.time()
        inbox = None
        inbox_dict = {
                "network":message.get("network"),
                "shortcode":message.get("short_code"),
                "msisdn":message.get("msisdn"),
                "message":message.get("message"),
                "linkid":message.get("link_id"),
                "created":datetime.now(),
                "modified":datetime.now(),
                "created_by":self.APP_NAME
        }
        
        try:
            result_proxy = self.db.engine.execute(
                Inbox.__table__.insert(),inbox_dict)

            self.inbox_id = result_proxy.inserted_primary_key

        except Exception, e:
            self.error("Problem creating inbox message ...%r :: %r" % \
            (e, message))
            self.inbox_id = -1

        t1 = time.time()
        self.info("Time taken to create inbox message => %r seconds" % (t1-t0))


    def outbox_message(self, message, response):
        t0 = time.time()
        outbox = None
        self.logger.info("RESPONSE TEXT: %r " % response)
        outbox_dict = {
            "shortcode":message.get("short_code") or message.get('access_code') or None,
            "network":message.get("network") or None,
            "profile_id":self.profile_id or None,
            "linkid":message.get("link_id") or None,
            "retry_status":"0",
            "modified":datetime.now(),
            "text":str(response),
            "sdp_id":message.get("sdp_id") or '6013852000120687',
            "msisdn":message.get("msisdn"),
            "date_created":datetime.now(),
            "date_sent":datetime.now(),

        }
        self.logger.info("Outbox D %r" % outbox_dict)
        try:
            connection = self.db.engine.connect()
            trans = connection.begin()

            result_proxy = connection.execute(Outbox.__table__.insert(), 
                outbox_dict)

            self.outbox_id = result_proxy.inserted_primary_key[0]
            self.logger.info("Logging self.outbox_id: %r" % self.outbox_id)
            trans.commit()
        except Exception, e:
            trans.rollback()
            self.error("Problem creating outbox message ...%r, %r" % \
            (outbox_dict, e))
            self.outbox_id = -1
        t1 = time.time()
        self.info("Time taken to insert outbox ...%r seconds" % (t1-t0))


    def invalid_balance_for_bet_response(self, bet_amount, message_dict):
        balance, bonus = self.get_account_balance(message_dict)
        #bonus = self.get_bonus_balance(message_dict)
        self.info("Found Balance For BET bal::%r::Bet amount::%r" % \
        (balance, bet_amount))
        if bet_amount < Decimal(self.scorepesa_configs["min_amount"]):
            self.jp_bet_status = 421
            response = 'Sorry but your bet amount KES %0.2f is less that' \
                'minimum allowed of KES %s. Please try again. ' \
                'scorepesa.co.ke' \
                % (bet_amount, self.scorepesa_configs["min_amount"])
        elif (balance+bonus) < bet_amount:
            self.jp_bet_status = 421
            response = 'Sorry but your current balance is KES %0.2f, ' \
                'To place your bet of KES %0.2f, please top up your ' \
                'account.%s' % (balance, bet_amount, self.free_bonus_advise)
        elif (balance+bonus) < Decimal(self.scorepesa_configs["min_amount"]):
            self.jp_bet_status = 421
            response = 'Sorry but your current balance is KES %0.2f, ' \
                'To place your bet of KES %0.2f, please top up your ' \
                'account.%s' % (balance, bet_amount, self.free_bonus_advise)
        else:
            response = None

        return response
    """
    Returns bet_id, amount, is_valid. Valid =-1 duplicate trx in mpesa_trx
    """
    def get_bet_details(self, msg):
        duplicatePaymentSQL = "select mpesa_amt from mpesa_transaction "\
            "where mpesa_code = :receipt_number"

        res1 = self.db.engine.execute(sql_text(duplicatePaymentSQL), msg).fetchone()
	if res1:
            return 0, 0, -1
        #if not msg.get('reference_no', None):
        #    return 0, 0, 1
        bet_id = msg.get("bet_id")
	q = "select bet_id, bet_amount from bet "\
            "inner join profile using(profile_id) "\
	    "where bet_id = :bet_id and bet.created >  now() - interval 15 minute "
        res = self.db.engine.execute(sql_text(q),{'bet_id':bet_id}).fetchone()
        b_id, bet_amount = (0, 0)
        if res:
            b_id, bet_amount = res
        self.info("Found valid bet  :%r, %r" %( b_id, bet_amount))
        if b_id:
            q2 = "select * from bet_slip inner join"\
		 " `match` m using(parent_match_id) where bet_id = :bet_id "\
                 " and m.bet_closure < now() "
            res2 = self.db.engine.execute(sql_text(q2), {'bet_id':bet_id}).fetchone()
            if res2:
                return str(b_id), str(bet_amount), 0
            return str(b_id), str(bet_amount), 1
        return 0, 0, 0 
        
        

    def process_request(self, message_dict):
        new = False
        msisdn = self.clean_msisdn(message_dict.get('msisdn'))
        self.info("Found msisdn and clean :%r" % msisdn)
        if msisdn is None:
            return 0, None, new
        message_dict['msisdn'] = msisdn

        text_dict = self.parse_message(message_dict.get("message", ''))
        if not text_dict:
            profile_id, new = self.create_profile(message_dict, 0)
            response = self.process_unknown(message_dict)
            self.outbox_message(message_dict, response)
            return self.outbox_id, response, new
       
        self.inbox_message(message_dict)
        if self.message_type == 'GAMES':
            self.info("Found request GAMES")
            response, new = self.process_games(message_dict)
        elif self.message_type == 'JP_MATCH':
            self.info("JP Games TEXT DICT %r" % text_dict)
            try:
                jp_type = (text_dict[-1:][0]).lower()
            except:
                jp_type = 'jp'
            self.info("Games request Jackpot type %r" % jp_type)
            response, new = self.process_jackpot_games(message_dict, jp_type)
        elif self.message_type == 'JP_RESULT':
            self.info("JP match results TEXT DICT %r" % text_dict)
            response, new = self.process_jackpot_results(message_dict)
        elif self.message_type == 'SCOREPESAPOINT':
            profile_id, new = self.create_profile(message_dict, 0)
            betpoint = ScorepesaPoint(self.logger, profile_id,  
                self.db.engine.connect())
            self.info("Scorepesa points TEXT DICT {0}".format(text_dict))
            scorepesa_points_balance = betpoint.get_balance() 
            response = "Your current Scorepesa Points balance is %0.2f. Bet daily' \
                'to earn more points on Scorepesa" % (scorepesa_points_balance,)

            self.info("Scorepesa points process result {0}".format(response))
        elif self.message_type == 'BINGWA_RESULT':
            self.info("Bingwa match results TEXT DICT %r" % text_dict)
            response, new = self.process_jackpot_results(message_dict,
                  jp_key='bingwa5', sub_type_id=2)
        elif self.message_type == 'HELP':
            response = self.process_help()
        elif self.message_type == 'BET':
            self.info("TEXT DICT %r" % text_dict)
            try:
                bet_amount = Decimal(text_dict[-1:][0])
            except:
                bet_amount = 0
            self.info("Bet Amount %r" % bet_amount)

            self.info("Found request BET")
            invalid_amount_response =\
             self.invalid_balance_for_bet_response(bet_amount, message_dict)
            if invalid_amount_response:
                response = invalid_amount_response
            else:
                response = self.bet(message_dict, text_dict)

        elif self.message_type == 'BALANCE':
            self.info("Found request BALANCE")
            response = self.process_balance(message_dict)
        elif self.message_type == 'WITHDRAW' or self.message_type =='W':
            self.info("Found request WITHDRAW")
            response, status_code = self.process_withdrawal(message_dict, 
                text_dict)
            #return 0, "WITHDRAW", 0
        elif self.message_type == 'HELP':
            response = self.process_help()
        elif self.message_type == 'BONUS':
            response, new =\
             self.process_bonus_request(message_dict, text_dict)
        elif self.message_type == 'STOP':
            profile_id, new = self.create_profile(message_dict, 0)
            response = self.process_stop(message_dict)
        elif self.message_type == 'BONUS_BALANCE':
            response = self.bonus_balance(message_dict)
        elif self.message_type == 'FREEJACKPOT':
            response = self.scorepesapoint_jackpot_bet(message_dict, text_dict)
        elif self.message_type == 'JACKPOT':
            message = message_dict.get('message', '')
            if 'auto' in message.lower():
               text_dict = ['auto','#'.join(['1X2'[randint(0,2)] for y in range(0,6)])]
            response = self.jackpot_bet(message_dict, text_dict)
        elif self.message_type == 'CANCEL_BET':
            self.info("Bet cancel text dict %r" % text_dict)
            try:
                cancel_bet_id = int(text_dict[-1:][0])
            except:
                cancel_bet_id = 0
            self.info("Bet Id to cancel %d :: msisdn:: %s"
             % (cancel_bet_id, msisdn))
            
            if int(self.scorepesa_configs['enable_bet_cancel_testing']) == 1:    
                if msisdn in self.scorepesa_configs["tests_whitelist"].split(','):
                    testing=True
                else:
                    testing=False
            else:
                testing=True
            if cancel_bet_id != 0 and testing and \
                int(self.scorepesa_configs["enable_bet_cancel"]) == 1:
                reason = 101
                #if(self.betrader_bet_cancel_submit(cancel_bet_id, 
                #    reason, message_dict, msisdn, None)):
                #    response =\
                #        "Request received you will receive a notification" \
                #        "shortly."
                #else:
                #    self.APP_NAME = "SMS"
                response = self.cancel_bet_trx(cancel_bet_id, True, 
                        None, message_dict)
            else:
                response =\
                 "For assistance contact customer care." \
                 "Helplines 0101 290080."
        else:
            profile_id, new = self.create_profile(message_dict, 0)
            response = self.process_unknown(message_dict, new)

        self.outbox_message(message_dict, response)
        self.info("Returning response - process request (%r, %r, %r) "
         % (self.outbox_id, response, new))
        return self.outbox_id, response, new

    def process_stop(self, message):
        try:
            self.db.engine.execute(
                sql_text("update profile set status=0 where msisdn=:value"\
                " limit 1"), {'value': message.get("msisdn")})
        except:
            pass
        return "SCOREPESA we have deactivated your account as requested. "\
            "KARIBU tena via SMS 29008 or www.scorepesa.co.ke. Thank you"

    def process_bonus_request(self, message, text_dict):
        #create this profile if not exists
        profile_id, new = self.create_profile(message, 1)
        self.logger.info("referal bonus request from profileId %r" 
        % profile_id)
        award_referral_bonus = self.scorepesa_configs['award_referral_bonus']
        if award_referral_bonus == '0':
            response = "Sorry, Bonus on reference has been temporarily suspended"
            return response, new
        self.logger.info("Referal bonus award message [] {0} [] text dict" \
            " [] {1} []".format(message, text_dict))
        if len(text_dict) < 2:
            response = "Your reference number was not recognized. " \
                "Please send ACCEPT#NUMBER to 29008 to refer a friend"
        else:
            bonus_msisdn = None
            good_message = re.sub(r"\s+", '', message.get("message"))
            msisdn_match = re.match('.*(?:\+?(?:[254]{3})|0)?7([0-9]{8})', 
                good_message)

            if msisdn_match:
                bonus_msisdn = "%s%s" %( '2547', msisdn_match.group(1))
            self.logger.info("Referral bonus found MSISDN, %r" % bonus_msisdn)

            '''
            Filter out and block airtel referrals based on operator checks
            '''
            referer_msisdn = self.get_profile_msisdn(profile_id)
            
            referer_operator = self.check_msisdn_operator(referer_msisdn, 
                referred=False)

            refered_operator = self.check_msisdn_operator(bonus_msisdn, 
                referred=True)

            self.logger.info("got referer operator {0} for referer msisdn"
                " []{1} [] refered operator {2} [] refered msisdn [] {3} []"\
                .format(refered_operator, referer_msisdn, 
                    refered_operator, bonus_msisdn))

            if refered_operator not in \
                self.scorepesa_bonus_cfgs['referals_allowed_operators'].split(','):
                return "Referral bonus on airtel is currently" \
                    " unavailable. Kindly contact our customer care 0101 290080 for" \
                    " further details. T&Cs apply.", new

            if referer_operator not in \
                self.scorepesa_bonus_cfgs['referals_allowed_operators'].split(','):
                return "Dear Customer, Airtel referral bonus is temporarily" \
                    " suspended. Kindly contact our customer care desk" \
                    " for further details. T&Cs apply.", new

            if not bonus_msisdn:
                self.logger.info("Regex match Failed, returnig fail "
                    "message [] {0}".format(message.get("message")))
                response = "We are unable to find your reference"\
                    " from your message. Please send ACCEPT#NUMBER to 29008"\
                    " to refer friend "
            elif self.db.engine.execute(
                sql_text("select profile_id from profile where msisdn=:value"),
                {'value':bonus_msisdn}).fetchone():
                self.logger.info("Found duplicate profile for referral returing")
                response = "This reference is not valid. The referred number already exist  %s" \
                    "on  scorepesa.co.ke" % (bonus_msisdn, )
            elif self.db.engine.execute(
                sql_text("select profile_bonus_id from profile_bonus where" \
                    " referred_msisdn=:value"), {'value':bonus_msisdn})\
                    .fetchone():
                self.logger.info("Duplicate referral number found, returning")
                response = "Thank you but subscriber %s was already referred" \
                "to scorepesa." % (bonus_msisdn, )
            else:
                self.logger.info("Valid referral, will create bonus trx")
                #createprofile bonus
                try:
                    profile_bonus_dict={
                        "profile_id": profile_id,
                        "referred_msisdn":bonus_msisdn, #bonus on same number
                        "bonus_amount":float(
                            self.scorepesa_configs['referral_bunus_amount']),
                        "status":'NEW',
                        "expiry_date": datetime.now()+timedelta(days =3),
                        "bet_on_status": 1,
                        "date_created": datetime.now(),
                        "updated":datetime.now(),
                        "created_by":'referral_message'
                    }
                    self.db.engine.execute(ProfileBonu.__table__.insert(), 
                        profile_bonus_dict)
                    self.logger.info("profile bonus saved success : %r" \
                        % (message.get('msisdn', None),))
                    response = "Thank you for referring %s. Your KES. %0.2f" \
                        " bonus will be activated once your friend deposits" \
                        " a minimum of KES. %0.2f to SCOREPESA via Paybill" \
                        " 290080." % (bonus_msisdn, 
                            float(self.scorepesa_configs['referral_bunus_amount']),
                            float(self.scorepesa_configs['referral_bunus_amount']))

                    if self.scorepesa_configs['bonus_on_referal_bet'] == '1':
                       bonus_award_limit = \
                       float(self.scorepesa_bonus_cfgs['max_referal_bonus_re_award'])
                       
                       response = "Thank you for referring %s. Your Bonus" \
                           " will be activated once your friend places their" \
                           " first bet within 72 hours. Bonus awarded will" \
                           " match bet amount placed up to a maximum of KESs" \
                           " %0.2f." % (bonus_msisdn, bonus_award_limit)
                       response = "Thank you for referring %s. Your KESs." \
                           " %0.2f bonus will be activated once your friend "\
                           "deposits a minimum of KES. %0.2f to SCOREPESA via"\
                           " Paybill 290080 and place a bet." \
                           % (bonus_msisdn, 
                           float(self.scorepesa_configs['referral_bunus_amount']), 
                           float(self.scorepesa_configs['referral_bunus_amount']))

                except Exception as e:
                    self.logger.error("Exception creating profile referral "\
                        "bonus ignored, rolled back : %r " % e)
                    response = 'Sorry, we could not process your referral' \
                        ' request right now. Please try again later.'
        return response, new

    def check_msisdn_operator(self, msisdn, referred=False):
         if not referred:
             operator = self.db.engine.execute(
                sql_text("select network from profile where msisdn = :value"),
                {'value':msisdn}).fetchone()
             if operator:
                network = operator[0]
                if network:
                    return network.upper()
         #check prefix instead
         return self.get_network_from_msisdn_prefix(msisdn)

    """
    TODO: Check response on wrong result
    """
    def get_account_balance(self, message):
        profile_id, new = self.create_profile(message, 1)
        self.profile_id = profile_id
        bal = False
        if profile_id:
            bal = self.db.engine.execute(
                sql_text("select balance, bonus_balance from profile" \
                    "_balance where profile_id = :value"),
                {'value':profile_id}).fetchone()
        if bal:
            available_bonus=float(bal[1])
            self.balance, self.bonus = float(bal[0]), available_bonus
        else:
            self.balance, self.bonus = 0, 0
        return self.balance, self.bonus

    def get_claimed_free_bet_bonus(self, profile_id):
        bn = False
        if profile_id:
            bn = self.db.engine.execute(
                sql_text("select bonus_amount, to_award_on from profile" \
                    "_bonus b inner join free_bet f on f.profile_id = " \
                    "b.profile_id where b.profile_id=:pf and to_award_on" \
                    " < now() and b.created_by='free_bet_bonus' and " \
                    "b.status='CLAIMED'"),{'pf':profile_id}).fetchone()
        if bn:
            #check if cancelled bet cancel bonus b4 allow to use
            sQ = "select b.status, b.profile_id, fb.free_bet_id from " \
                "free_bet_transactions ft inner join bet b on b.bet_id = " \
                "ft.bet_id inner join free_bet fb on fb.free_bet_id = " \
                "ft.free_bet_id  where ft.profile_id=:pf and fb.status = " \
                "'AWARDED' and b.status=24"
            bcancels = self.db.engine.execute(sql_text(sQ), 
                {'pf':profile_id}).fetchone()
            self.logger.info("get_claimed_free_bet_bonus :: {0}"\
                .format(bcancels))
            if bcancels and int(bcancels[0]) == 24:
               self.logger.info("Got a victim of T&Cs violation :: {0} :: " \
                "status got:: {1}".format(bcancels, bcancels[0]))
               #cancel free bet bonus
               pbcQ = "update profile_balance set bonus_balance = " \
                "(bonus_balance-{0}) where profile_id=:profile_id limit 1"\
                .format(self.scorepesa_freebet_cfgs['free_bet_amount'])
               fbcQ = "update free_bet set status='CANCELLED' where " \
                "free_bet_id=:fbid and profile_id=:pf"""

               self.db.engine.execute(sql_text(pbcQ), 
                   {'profile_id': profile_id})
               self.db.engine.execute(sql_text(fbcQ), {'pf': profile_id, 
                   'fbid': bcancels[2]})
               self.db.engine.execute(sql_text("update profile_bonus set" \
                   " status=:new_status,bet_on_status=:sta2 where " \
                   "profile_id = :pfid and status = :oldst """),
                    {'new_status':'USED', 'sta2':2, 'pfid':profile_id, 
                    "oldst":'CLAIMED'})
               self.free_bet_bonus_tnc = \
                   self.scorepesa_freebet_cfgs['free_bonus_tnc_violation']
               return 0, None
            return bn[0], bn[1]            
        return 0, (datetime.now()+timedelta(minutes=10))\
            .strftime('%Y-%m-%d %H:%M:%S')

    def get_bonus_balance(self, message):
        profile_id, new = self.create_profile(message, 1)
        bal = False
        if profile_id:
            bal = self.db.engine.execute(
                sql_text("select balance, bonus_balance from profile_balance" \
                    " where profile_id = :value"),{'value':profile_id})\
                    .fetchone()
        if not bal:
            return 0
        return bal[1]

    def process_help(self):
        return "Sms GAMES to 29008. Paybill 290080, Acc number: SCOREPESA," \
            "BALANCE to 29008, WITHDRAW#AMOUNT to 29008. Cs 0101 290080" \
            "visit scorepesa.co.ke T&C apply"

    def process_unknown(self, message, new=False):
        if new:
            response, new = self.process_games(message)
            return response
        return "Sorry your message is incorrect, Sms GAMES to 29008 to " \
            "place a bet, SMS Help to 29008 for further assistance. " \
            " scorepesa.co.ke T&C apply"

    def process_balance(self, message):
        balance, bonus = self.get_account_balance(message)
        return "Your current SCOREPESA Account balance is KES %0.2f, " \
            "Bonus %0.2f. %s" % (balance, bonus, 
            self.scorepesa_configs['scorepesa_fb_page_msg'])

    def bonus_balance(self, message):
        balance = self.get_bonus_balance(message)
        return "Your current SCOREPESA Bonus balance is KES %0.2f to refer" \
            "more friends send ACCEPT#NUMBER to 29008 " % balance

    def betting_disabled(self):
        d = "select id, disabled from betting_control order by 1 desc limit 1";
        dis = self.db.engine.execute(sql_text(d)).fetchone()
        return dis and dis[1] == 1

    def live_betting_disabled(self):
        d = "select id, disabled from live_betting_control order by 1 desc limit 1";
        dis = self.db.engine.execute(sql_text(d)).fetchone()
        return dis and dis[1] == 1


    def bet(self, message, options):
        self.logger.info("SMS bet options[][] {0} []message[] {1}"\
        .format(options, message))
        if self.betting_disabled():
            return "Betting has been temporarily disbaled. Kindly try again after a few minutes "
        #options looks like [GAME,190, 1, 250]
        #game id#pic#gameid#pick ....#amount
        profile_id, new = self.create_profile(message, 1)
        if self.check_account_freeze(message, profile_id=None):
            return self.scorepesa_configs['account_bet_block_msg']

        #all even options minus keywork games
        bet_pick = options[::2][1:]
        #all even options skipping first and last
        game_ids = options[1:][::2][:-1]

        if len(game_ids) > int(self.scorepesa_configs['max_match_bet']):
            return "The number of teams in our bet exceeds the maximun " \
                "allowed for multibet. You can only bet for %s teams" \
                % float(self.scorepesa_configs['max_match_bet'])
        self.betslipLen = len(game_ids)
        #last option
        try:
            amount = options[-1:][0]
        except:
            amount = 0

        if len(game_ids) != len(bet_pick):
            return "Incorrect bet, your GAMEIDs does not tally selected " \
                "PICK or the SMS format is invalid. Please try again"

        bet_slips = []
        selctions = []

        count = 0
        bet_total_odd = 1
        try:
            self.logger.info("RAW PICk %r %r" % (game_ids, bet_pick))
            for game_id, pick in dict(zip(game_ids, bet_pick)).iteritems():
                self.logger.info("PICKED VALUES %r %r" % (game_id, pick))
                parent_outright_id = self.get_parent_outright_id(game_id)
                if parent_outright_id is not None:
                   return "Sorry we could not your place bet right now. " \
                       "Please bet via website scorepesa.co.ke."
                else:
                   invalid_slip, response = \
                       self.invalid_betslip_message(game_id, pick, amount)
                self.logger.info("INVALID BET MESSAGE %r %r" \
                % (invalid_slip, response))
                if invalid_slip:
                    return response
                event_odd = response
                outcome_exist =\
                self.check_outcome_exists(event_odd.get("parent_match_id"))
                self.logger.info("Outcome exist? %r" % (outcome_exist))
                if outcome_exist:
                    return "Invalid gameid " + game_id +\
                     ". Please remove the match and try again."
                if len(game_ids)==1:
                    invalid_single_bet_message = self.invalid_single_bet_message(
                        profile_id, event_odd.get("parent_match_id"), amount)
                    if invalid_single_bet_message:
                        return invalid_single_bet_message
                bet_slips.append(
                    {"parent_match_id":event_odd.get("parent_match_id"),
                    "pick":event_odd.get("odd_key"), 
                    "odd_value": event_odd.get("odd_value"),
                    "sub_type_id": event_odd.get("sub_type_id"),
                    "special_bet_value": event_odd.get("special_bet_value")})

                sport_id = self.get_betrader_sport_id(event_odd.get("sport_id"))
                sbv = event_odd.get("special_bet_value") \
                    if event_odd.get("special_bet_value") else '*'
                odd_key = event_odd.get("odd_key")
                player_id = event_odd.get("id_of_player") or ""

                if int(event_odd.get("sub_type_id")) == 235:
                   r,h = lambda data: data, odd_key.split(' ')
                   #odd_key = "%s, %s!%s" % (h[0], h[1], player_id)
                   odd_key = "%s!%s" % (odd_key, player_id)
                   
                makoh = "lcoo:%s/%s/%s/%s" % (event_odd.get("sub_type_id"),
                         sport_id, sbv, odd_key)
                line = "prematch"
 
                #for custom markets to not be submitted to MTS hence being rejected
                if event_odd.get("parent_match_id") < 0:
                    self.custom_matches=True

                selctions.append({"line": line, "market": makoh,
                      "match": event_odd.get("parent_match_id"),
                       "odd": event_odd.get("odd_value"), "ways": 0, "bank": 0})
                self.logger.info("Cloected Bet SLIP: %r " % bet_slips)
                bet_total_odd = bet_total_odd * float(event_odd.get("odd_value"))
        except Exception, e:
            self.logger.info("Error fetching match_bet odds: %r " % e)
            return "Sorry we are unable to create your bet right now," \
                "please try again later"

        bet_total_odd = bet_total_odd or 1

        self.possible_win = float(bet_total_odd) * float(amount)
        if self.possible_win > float(self.scorepesa_configs['max_bet_possible_win']):
            self.possible_win = float(self.scorepesa_configs['max_bet_possible_win'])

        invalid_bet_message = self.invalid_bet_message(
            profile_id, amount, self.possible_win)

        if invalid_bet_message:
            return invalid_bet_message

        '''
         ```````````````````````````````
           All LIMITs on possible win is done on bet winners processor  application.
         ```````````````````````````````````````````````````````````

        if self.possible_win > float(self.scorepesa_configs['max_bet_possible_win']):
            self.possible_win = float(self.scorepesa_configs['max_bet_possible_win'])
            #return "Your possible win amount exceeds the maximum allowed 
            for a single bet. You can only win upto KESs %0.2f amount in a 
            single bet. Please try again. T&C apply" % \
            float(self.scorepesa_configs['max_bet_possible_win'])
        '''

        try:
            amount = Decimal(amount)
        except:
            amount = 0

        self.logger.info('READING PROFILE ID: %r' % profile_id)

        invalid_bet_limit_response = self.check_bet_limit(amount, 
            len(bet_slips))
        if invalid_bet_limit_response:
            self.logger.info("Invalid Bet Limit: %r" \
                % (invalid_bet_limit_response))
            return invalid_bet_limit_response

        bet_message = message.get('message')
        bet_id = self.place_bet(profile_id,
            bet_message, amount, bet_total_odd, self.possible_win, bet_slips)

        if not bet_id:
            if self.jp_bet_status == 423:
                return "Sorry cannot create bet, minimum odds accepted " \
                    "for bets on bonus amounts is {0}. Please review " \
                    "selections."\
                    .format(self.scorepesa_configs['bonus_bet_minimum_odd'])
            if self.jp_bet_status == 424:
                return "Sorry cannot create bet, minimum odds accepted " \
                   "for bets on bonus amounts is {0}. Please review " \
                   "selections and try again."\
                   .format(self.scorepesa_configs['bonus_bet_minimum_odd'])
            if self.jp_bet_status == 425:
               return "Sorry we are unable to create your bet" \
                   " right now.{0}"\
                   .format(self.scorepesa_bonus_cfgs['referal_bonus_not_bet_notify'])
            if self.jp_bet_status == 426:
               return "Sorry we are unable to create your bet right now.{0}"\
               .format(self.scorepesa_bonus_cfgs['referal_bonus_expired_notify']) 
            return "Sorry we are unable to create your bet right now, " \
                "please try again later. {0}"\
                .format(self.referral_bonus_advise_notification)
        self.logger.info('SAVED BET ID: %r' % bet_id)
        current_balance, bonus_balance = self.get_account_balance(message)
        #bonus_balance = self.get_bonus_balance(message)
        #bet_message = "Bet ID %s, %s. %s Possible win KES %0.2f. SCOREPESA bal" \
        #    " KES %0.2f. Bonus Bal KES %0.2f.%s%s%s%s" % (
        #    bet_id, bet_message, self.multibet_bonus_message, 
        #    (self.multibet_possible_win or self.possible_win), 
        #    current_balance, bonus_balance, self.freebet_notification, 
        #    self.bonus_bet_low_odd_msg, self.referal_bonus_fail_notify, 
        #    self.referal_bonus_extra)

        taxableAmount = float(self.multibet_possible_win or self.possible_win) - float(amount);
        tax = taxableAmount * 0.2;        

        bet_message = "SCOREPESA %s ID  %s. placed successfully."\
            "Possible WIN  KSH %s, Account balance KSH %0.2f. Bonus balance KSH %0.2f"\
            % ('MULTIBET' if len(bet_slips) > 1 else 'SINGLEBET', bet_id, float(self.multibet_possible_win or self.possible_win) - tax,
             current_balance, bonus_balance)
        #publish to betrader 4 validation
        #msisdn = message.get("msisdn", '')
        #self.logger.info('SMS Invoke betrader validation %r::%r::%r::%r::%r'
        #    % (msisdn, self.scorepesa_configs['enable_bet_validation'], 
        #    self.scorepesa_configs['validation_sport_ids'].split(','), 
        #    self.scorepesa_configs['tests_whitelist'].split(','), sport_id))

        #betid_linkid = "%s_%s" % (str(bet_id), str(message.get('linkId', '')))	

        #response = self.prepare_invoke_betrader(bet_message, msisdn, amount, 
        #    selctions, betid_linkid, bet_slips, profile_id, sport_id)
        #self.call_bet_award_scorepesa_points(profile_id, bet_slips, 
        #    bet_total_odd, bet_id)
        #referal bonus adjust after bet
        #if str(profile_id) not in \
        #    self.scorepesa_bonus_cfgs['referral_test_whitelist'].split(','):
        #    if self.scorepesa_configs['award_referral_bonus'] == '1':
         #       referral_bonus = ReferralBonus(self.logger, profile_id)
         #       referral_bonus.award_referral_bonus_after_bet(profile_id, 
         #           amount, bet_total_odd)
        return bet_message


    def prepare_bet_submit_to_betrader_validation(self, selctions, bet_id,
         bet_slips, profile_id, amount, ch_id=None, ip=None, devId=None):
        #publish to betrader 4 validation
        db_bet_id = bet_id

        if ip is not None:
            ips = ip.split(',')
            ip = ips[0]

        if ch_id is None:
            ch_id = 'SMS'
            try:
              db_bet_id = bet_id.split('_')[0]
            except:
              db_bet_id = bet_id
        self.logger.info("prepare bet submit {0}::{1}::{2}::{3}::{4}::{5}"
            "::{6}::{7}::{8}".format(selctions, bet_id,
                bet_slips, profile_id, amount, ch_id, ip, devId, db_bet_id))
        max_stake = self.scorepesa_configs['max_bet_amount_single']
        if len(bet_slips) > 1:
            max_stake = self.scorepesa_configs['max_bet_amount_multi']
        ltd_id = self.scorepesa_configs['betrader_ltd_id_%s' % ch_id.lower()]
        ticket_id = "%s_%s_%s" % (str(db_bet_id), 
            datetime.utcnow().strftime('%Y%m%d%H%M%S'), ltd_id)
        ticketMaxWin = float(self.scorepesa_configs['max_bet_possible_win'])
        
        json_msg = {
            "version": "1.4", 
            "bookmakerID":self.scorepesa_configs['sportrader_bookmark_id'] or "17403",
            "extTicketID": ticket_id,
            "limitID": ltd_id, 
            "source": {"channelID":ch_id or "SMS",
            "endCustomerID": str(profile_id),
            "endCustomerIP": ip or "",
            "deviceID": devId or "",
            "languageID": "EN"},
            "bet": {
                "stk": float(amount), "cur": "KES",
                "sys": [0],
                "ts_UTC": datetime.utcnow().strftime('%Y%m%d%H%M%S'),
                "selections": selctions, 
                "ext": {
                    "ticketMaxWin": ticketMaxWin, 
                    "bonusWin": self.bonusWin
                }
            }
        }

        if self.map_bet_to_mts_ticket(db_bet_id, ticket_id):
             return self.publish_validate_bet_betrader(json_msg, bet_id)
        return False
    
    def prepare_invoke_betrader(self, bet_msg, msisdn, amount, selctions, 
        bet_id, bet_slips, profile_id, sport_id, 
        ch_id=None, ip=None, devId=None):
        #publish to betrader 4 validation
        self.logger.info('Betrader validation %r::%r::%r::%r::%r::%s::%r::%r'\
            % (msisdn, self.scorepesa_configs['enable_bet_validation'], 
            self.scorepesa_configs['validation_sport_ids'].split(','), 
            self.scorepesa_configs['tests_whitelist'].split(','), 
            sport_id, bet_id, selctions, bet_slips))
        result = None

        self.logger.info('check custom matches before mts submit [][] {0}'\
            .format(self.custom_matches))
        if self.custom_matches:
             return bet_msg

        if int(self.scorepesa_configs['enable_mts_bet_verify_testing']) == 1:
            if int(self.scorepesa_configs['enable_bet_validation']) == 1\
                and str(sport_id) in \
                self.scorepesa_configs['validation_sport_ids'].split(',') and \
                str(msisdn) in self.scorepesa_configs['tests_whitelist'].split(','):
                self.logger.info("test prepare invoke betrader::{0}::{1}"\
                .format(msisdn, selctions))
                #result = self.prepare_bet_submit_to_betrader_validation(
                #    selctions, bet_id, bet_slips, profile_id, 
                #    amount, ch_id, ip, devId)
        else:
            if int(self.scorepesa_configs['enable_bet_validation']) == 1\
                and str(sport_id) in \
                self.scorepesa_configs['validation_sport_ids'].split(','):
                self.logger.info("production prepare invoke betrader::" \
                    "{0}::{1}".format(msisdn, selctions))
                if float(amount) >= \
                    float(self.scorepesa_configs['mts_bet_amount_submit_limit']):
                    if str(msisdn) in \
                        self.scorepesa_configs['tests_whitelist'].split(','):
                        return bet_msg
                    #result = self.prepare_bet_submit_to_betrader_validation(
                    #    selctions, bet_id, bet_slips, profile_id, 
                    #    amount, ch_id, ip, devId)
                    
        if result:
            return bet_msg #"%s" % self.scorepesa_configs['bet_default_response']
        return bet_msg


    def publish_validate_bet_betrader(self, message, bet_id, **kwargs):
        try:
            if kwargs:
               queue_name = kwargs['queue']
               exchange_name = kwargs['exchange']
               routing_key = kwargs['rkey']
               rkeyheader = kwargs['rkeyheader']
            else:
               queue_name = "scorepesa-Submit"
               exchange_name = "scorepesa-Submit"
               routing_key = "node1.ticket.confirm"
               rkeyheader = "node1.ticket.confirm"

            correlationId = bet_id

            self.logger.info("betrader validate bet json %s::%s::%s::%s::%r"\
                % (queue_name, exchange_name, routing_key, rkeyheader, message))
            pub = SendSmsPublisher(queue_name, exchange_name)

            pubresult=pub.publishBt(message, routing_key, correlationId, 
                rkeyheader, pub_type='topic')
            return pubresult
        except Exception, err:
            self.logger.error("EXCEPTION on MTS publish :: {0}".format(err))
            return False

    def call_bet_award_scorepesa_points(self, profile_id, bet_slips, 
        bet_total_odd, bet_id):
        self.logger.info("call_bet_award_scorepesa_points ...profile::{0}::"\
            "slips::{1}::odd::{2}::stake::{3}::live::{4}::betId::{5}..."\
            .format(profile_id, bet_slips, bet_total_odd, self.stake_on_cash,
             self.livebetting, bet_id))
        if self.stake_on_cash:
            if float(self.stake_on_cash) >= \
                float(self.scorepesa_points_cfgs['min_bet_amount_to_award']):
                #award points on bet
                betpoint = ScorepesaPoint(self.logger, profile_id)
                amount = self.stake_on_cash
                live_bet = self.livebetting
                trx_id = self.bet_transaction_id
                betpoint.bet_points_award(trx_id, live_bet, profile_id, 
                bet_slips, bet_total_odd, 
                amount, bet_id, bet_type='prematch')
        return True

    def check_multiple_bet_same_game(self, profile_id, betslips, amount):
        todays_bets_on_match = self.db.engine.execute(
            sql_text("select  bet.bet_id, group_concat(bs.parent_match_id)"
                "from bet_slip bs inner join bet on bet.bet_id=bs.bet_id"
                "where bet.profile_id =:pf and bet.bet_amount = :amount"
                "and bs.total_games = :total and bet.created > "
                "date_sub(now(), interval 10 minute) limit 10"),
                {'amount': amount, 'pf': profile_id, 'total':len(betslips)})\
                .fetchall()
        if not todays_bets_on_match or not todays_bets_on_match[0][0]:
            return False

        is_duplicate = True
        for bet in todays_bets_on_match:
            for b in betslips:
                if str(b.get('parent_match_id')) not in bet[1]:
                    is_duplicate = False
                    continue
        return is_duplicate

    def scorepesapoint_jackpot_bet(self, message, options, app=None):
        profile_id, new = self.create_profile(message, 1)
        jp_id = message.get("jackpot_id", None)
        self.logger.info("scorepesa point jackpot profile ::::: {0} ::jp:: {1} "
            "::message:: {2} ::: options::{3}.... "\
            .format(profile_id, jp_id, message, options))
        try:
            if jp_id:
                jackpot, jptype = self.db_session.query(
                    JackpotEvent, JackpotType).join(JackpotType,
                    JackpotEvent.jackpot_type == JackpotType.jackpot_type_id)\
                    .filter(JackpotEvent.jackpot_event_id == jp_id,
                    JackpotEvent.status == 'ACTIVE').one()
                jp_type = jptype.name
                jp_type_key = jackpot.jp_key.lower()
                self.logger.info("JP query %s %s" % (jp_type, jp_type_key))
                jp_key = jp_type_key
            else:
                try:
                    jp_key = options[0]
                except:
                    jp_key = 'jp'
                jackpot, jptype = self.db_session.query(JackpotEvent,
                     JackpotType).join(JackpotType,
                     JackpotEvent.jackpot_type == JackpotType.jackpot_type_id)\
                .filter(JackpotEvent.status == 'ACTIVE',
                     JackpotEvent.jp_key == jp_key).first()
        except NoResultFound:
            jackpot, jptype = None, None
        except Exception:
            jackpot, jptype = None, None

        if not jackpot:
            self.jp_bet_status = 421
            return "JACKPOT matches are already underway or"\
                "Invalid format for JACKPOT, kindly resend with the "\
                "correct format or visit scorepesa.co.ke. "\
                "Helpline: 0101 290080."   
      
        self.logger.info("got jackpot event :::: {0}::jpkey::{1}"\
            .format(jackpot.jackpot_event_id, jp_key))

        #check whitelist for test jps
        if jackpot.created_by.lower()=="test" and str(profile_id) \
            not in self.scorepesa_configs['jp_whitelist'].split(','):
              self.jp_bet_status = 421
              return 'JACKPOT is currently not available. Kindly try again later.'
       
        #all options minus keywork games
        if len(options) > 1:
            if jp_id is None:
                jp_type_key = options[0].lower()
           
            bet_pick_str = options[1]
            try:
                jp_type = self.scorepesa_configs[jp_type_key]
            except:
                jp_type = '1x2'
        else:
            bet_pick_str = options[0]

        bet_picks = [b for b in bet_pick_str.spit('#') if b]

        self.logger.info("Scorepesa point Jackpot match picks::{0}::btstring::"
            "{1}::opts::{2}".format(bet_picks, bet_pick_str, options))
        if len(bet_picks) > jackpot.total_games:
            bet_picks = bet_pick_str.split('#')
        
        self.logger.info("Jp picks after split on hash :: {0}"\
            .format(bet_picks))
        jackpot_matches = self.db_session.query(JackpotMatch, Match)\
        .join(Match, Match.parent_match_id == JackpotMatch.parent_match_id)\
        .filter(JackpotMatch.status == 'ACTIVE',
            JackpotMatch.jackpot_event_id == jackpot.jackpot_event_id)\
        .order_by(JackpotMatch.game_order.asc()).all()
 
        self.logger.info("jackpot matches for jackpot event.... {0}"\
            .format(jackpot_matches))
        if not jackpot_matches:
            self.jp_bet_status = 421
            return "No JACKPOT matches for the requested event."

        self.logger.info("JP match picks::{0}::total games::{1}::type::{2}.."\
            .format(bet_picks, jackpot.total_games, jp_type))
        #check if jp selections is equal to what is expected
        if len(bet_picks) != jackpot.total_games:
            self.jp_bet_status = 421
            self.logger.info("%s ==> %s " % (bet_picks, jackpot.total_games))
            return "To place JACKPOT,  SMS JP to 29008 " \
                "Helpline:0101 290080"

        amount = jackpot.bet_amount

        bet_slips = []
        count = 0
        bet_total_odd = 1
        try:
            ordered_game_ids = [m[1].game_id for m in jackpot_matches]
            self.logger.info("JP Raw ordered matches::{0}::betpicks::{1}"\
                .format(ordered_game_ids, bet_picks))
            for game_id, pick in dict(zip(ordered_game_ids, bet_picks))\
                .iteritems():
                self.logger.info("Picked JP game_id::{0}::pick::{1}"\
                    .format(game_id, pick))
                invalid_slip, response = self.invalid_jackpot_betslip_message(
                    game_id, pick, jackpot, jp_type_key)
                self.logger.info("Invalid JP Betslip string::{0}::{1}"\
                    .format(invalid_slip, response))
                if invalid_slip:
                    return response
                match = response
                #Bet closure already ready ignore bet
                if match.bet_closure < datetime.now():
                    self.jp_bet_status = 421
                    return "Invalid Bet, this autobet is no longer " \
                        " active. Watch out for the next JACKPOT."
                bet_slips.append({"parent_match_id": match.parent_match_id,
                    "pick": pick, "odd_value": 1,
                    "sub_type_id": jptype.sub_type_id,
                    "special_bet_value": "",
                    "bet_type": 7
                })
                self.logger.info("Collected JP Bet Slip::{0}".format(bet_slips))
                bet_total_odd = 1
        except Exception, e:
            self.logger.error("Error fetching match_bet odds::{0}".format(e))
            self.jp_bet_status = 500
            return "Sorry we are unable to create your JACKPOT right "\
                "now, please try again later."
        bet_total_odd = bet_total_odd or 1
        possible_win = jackpot.jackpot_amount
        try:
            amount = Decimal(amount)
        except:
            amount = 10.0
        self.logger.info('JP bet profile::{0}'.format(profile_id))
        bet_message = message.get('message')
        app_name = 'SMS' if not app else app
        connection = self.db.engine.connect()
      
        scorepesaPoint = ScorepesaPoint(self.logger, profile_id, connection)        
        redeem_amount = \
            float(self.scorepesa_points_cfgs['scorepesa_free_jp_redeem_amount'])
        jpEventId = jackpot.jackpot_event_id
        #check if has enough cash bal to proceed bet
        cash_bal, bonus_bal = self.get_account_balance(message)
        if float(cash_bal) >= float(amount):
            bet_on_balance = float(amount)
        else:
            return "Sorry you have insufficient balance JACKPOT stake is" \
                " KES. {0}. Please topup and try again."\
                .format(amount)

        bet_id = scorepesaPoint.place_points_bet(
            bet_message, redeem_amount, bet_slips, 
            bet_on_balance, app=app_name, jp=jpEventId)

        #get bal after for notification
        cashbal_after = float(cash_bal) - float(amount)

        if not bet_id:
            if scorepesaPoint.jp_bet_status == 424:
                return "Sorry you have insufficient points than " \
                    "required to place a free Jackpot bet."
            if scorepesaPoint.jp_bet_status == 421:
                return "Sorry Jackpot bet points weekly redeem has been " \
                    "reached. Lets do it again next week."
            if scorepesaPoint.jp_bet_status == 423:
                return "Sorry insufficient Scorepesa Points, Bal %0.2f. To earn" \
                    " more points bet everyday between 5am and 8am and earn" \
                    " double points." % (scorepesaPoint.scorepesa_points_bal)
            return "Sorry we are unable to create your jackpot bet right now" \
                ", please try again later."
        bet_message = "Jackpot Bet ID %s, %s. Scorepesa Points bal %0.2f. Bal " \
            "KES. %0.2f." % (bet_id, bet_message, 
                scorepesaPoint.scorepesa_points_bal, cashbal_after)
        self.jp_bet_status = 201
        return bet_message

    def jackpot_bet(self, message, options, app=None):
        #options looks like [Jp,190, 1, 250]
        #game id#pic#gameid#pick ....#amount
        if self.betting_disabled():
            return "This service is currently unavailable. Kindly try again after a few minutes"

        profile_id, new = self.create_profile(message, 1)
        jp_id = message.get("jackpot_id", None)
        jp_type_id = message.get('jackpot_type',None)
        try:
            if jp_id:
                jackpot, jptype = self.db_session.query(JackpotEvent,
                     JackpotType).join(JackpotType,
                      JackpotEvent.jackpot_type == JackpotType.jackpot_type_id)\
                      .filter(JackpotEvent.jackpot_event_id == jp_id,
                           JackpotEvent.jackpot_type == jp_type_id,
                           JackpotEvent.status == 'ACTIVE').one()
                jp_type = jptype.name
                jp_type_key = jackpot.jp_key.lower()
                self.logger.info("JP query %s %s" % (jp_type, jp_type_key))
            else:
                try:
                    jp_key = options[0]
                except:
                    jp_key = 'jp'
                jackpot, jptype = self.db_session.query(JackpotEvent,
                     JackpotType).join(JackpotType,
                     JackpotEvent.jackpot_type == JackpotType.jackpot_type_id)\
                .filter(JackpotEvent.status == 'ACTIVE',
                     JackpotEvent.jp_key == jp_key).first()
        except NoResultFound:
            jackpot, jptype = None, None
        except Exception:
            jackpot, jptype = None, None

        if not jackpot:
            self.jp_bet_status = 421
            return "JACKPOT matches are already underway visit scorepesa.co.ke. " \
                "For help: 0101 290080."

        #if jp_type_key and jp_type_key=='jp':
        #    self.jp_bet_status = 421
        #    return "Jackpot not available. Please contact customer care " \
        #       "for assistance."

        invalid_amount_response =\
            self.invalid_balance_for_bet_response(jackpot.bet_amount, message)
        #if invalid_amount_response and message.get('account', 0) == 1:
        #    return invalid_amount_response

        #if jp_id and 'bingwa' in jp_type_key:
        #    #check whitelist
        #    if str(profile_id) in self.scorepesa_configs['jp_whitelist'].split(','):
        #       return 'Bingwa5 is currently not available. Kindly try again later.'
        #    opts = 'bingwa#' + options[0]
        #    options = opts.split("#")

        #all options minus keywork games
        if len(options) > 1:
            if jp_id is None:
                jp_type_key = options[0].lower()
            pattern = '^\d{1,2}-\d{1,2}$'
            if jp_type_key == 'exact':
                try:
                    valid_options =\
                     map(lambda pick: self.is_correct_score_format(pick),
                          iter(options[1:5]))
                except:
                    return "Invalid format for scores, kindly resend the"\
                        "correct format or visit scorepesa.co.ke. " \
                        "For exampe 5#1-2#7-0#10-3#0-0#0-5"

            bet_pick_str =\
                map(lambda pick: pick.replace("-", ":"), iter(options[1:6]))\
                    if jp_type_key == 'bingwa5' else options[1]
            try:
                jp_type = self.scorepesa_configs[jp_type_key]
            except:
                jp_type = '1x2'
        else:
            bet_pick_str = options[0]

        bet_picks =\
            list(bet_pick_str) if jp_type_key == '1x2' else bet_pick_str

        self.logger.info("Jackpot match picks %s ::btstring:: %s :: opts ::%s"
         % (bet_picks, bet_pick_str, options))
        #pick every two consecutive picks
        #>>> picks = '1112341212121212'
        #>>> map(''.join, zip(*[iter(picks)]*2))
        #['11', '12', '34', '12', '12', '12', '12', '12']
       
        #if len(bet_picks) == jackpot.total_games * 2:
        #    bet_picks = map(''.join, zip(*[iter(bet_pick_str)] * 2))
        #elif len(bet_picks) > jackpot.total_games:
        #    bet_picks = bet_pick_str.split('#')
        bet_picks = [p for p in bet_pick_str.split('|') if p] \
            if "|" in bet_pick_str else \
            [ p.replace("-",":") for p in bet_pick_str.split('#') if p]

        jackpot_matches = self.db_session.query(JackpotMatch, Match)\
        .join(Match, Match.parent_match_id == JackpotMatch.parent_match_id)\
        .filter(JackpotMatch.status == 'ACTIVE',
            JackpotMatch.jackpot_event_id == jackpot.jackpot_event_id)\
        .order_by(JackpotMatch.game_order.asc()).all()
        if not jackpot_matches:
            self.jp_bet_status = 421
            return "No Jackpot matches for the requested Jackpot event."
        #all even options skipping first and last
        #game_ids = options[1:][::2][:-1]
        self.logger.info("Jackpot match picks %s ::"\
            "total games :: %s type:: %s " % (bet_picks, 
             jackpot.total_games, jp_type))
        if len(bet_picks) != jackpot.total_games:
            self.logger.info("NOTHINF %r, %s" % (bet_picks, jackpot.total_games))
            self.jp_bet_status = 421
            if jp_type == '1x2':
                return "To place the jackpot correct, kindly select "\
                    " pick for all the games and try again." \
                    " Help Tel:0101 29008"
            else:
                return "To place JACKPOT, SMS JP to 29008 " \
                    "Help Tel:0705 290080"

        amount = jackpot.bet_amount

        bet_slips = []
        count = 0
        bet_total_odd = 1.0
        try:
            ordered_game_ids = [m[1].game_id for m in jackpot_matches]

            self.logger.info("RAW PICk %r %r" % (ordered_game_ids, bet_picks))

            for game_id, pick in dict(zip(ordered_game_ids,
             bet_picks)).iteritems():
                self.logger.info("PICKED JACKPOT VALUES %r %r"
                 % (game_id, pick))

                invalid_slip, response = self.invalid_jackpot_betslip_message(
                    game_id, pick, jackpot, jp_type_key)
                self.logger.info("INVALID BET MESSAGE %r %r"
                 % (invalid_slip, response))
                if invalid_slip:
                    return response
                match = response
                #Bet closure already ready ignore bet
                if match.bet_closure < datetime.now():
                    self.jp_bet_status = 421
                    return "Invalid Bet, this JACKPOT is not"\
                        "longer active. Watch out for the next JACKPOT."

                bet_slips.append({"parent_match_id": match.parent_match_id,
                    "pick": pick, "odd_value": 1.0,
                    "sub_type_id": jptype.sub_type_id,
                    "special_bet_value": ""})
                self.logger.info("Collected Bet SLIP: %r " % bet_slips)
                bet_total_odd = 1.0
        except Exception, e:
            self.logger.info("Error fetching match_bet odds: %r " % e)
            self.jp_bet_status = 500
            return "Sorry we are unable to create your JACKPOT right now, "\
                "please try again later"

        bet_total_odd = bet_total_odd or 1.0

        possible_win = jackpot.jackpot_amount
        try:
            amount = Decimal(amount)
        except:
            amount = 0.0

        self.logger.info('READING PROFILE ID: %r' % profile_id)

        bet_message = ''.join(options)
        app_name = 'SMS' if not app else app
        bfa = int(message.get('account', 1))

        bet_id = self.place_bet(profile_id,
            bet_message, amount, bet_total_odd, possible_win, bet_slips,
             live_bet=None, app=app_name, jp=jackpot.jackpot_event_id)
        current_balance, bonus_balance = self.get_account_balance(message)
        if not bet_id:
            if self.jp_bet_status == 423:
                return "Bonus bet must be atleast a multibet of {0} teams " \
                    "and each team atleast a minimum odd value of {1}."\
                    .format(self.scorepesa_configs['bonus_bet_multibet'], 
                    self.scorepesa_configs['bonus_bet_minimum_odd'])
            if self.jp_bet_status == 421:
                return "Sorry current SCOREPESA bal is KES %0.2f. Bonus bal " \
                    "KESs %0.2f, To place JACKPOT BET of KES 20.00 to WIN" \
                    " millions, Topup your account." % (current_balance, 
                    bonus_balance)
            self.jp_bet_status = 421
            return "Sorry we are unable to create your bet right now, " \
                "please try again later."

        #bonus_balance = self.get_bonus_balance(message)
        #bet_message = "Jackpot Bet ID %s, %s. SCOREPESA bal KES %0.2f. "\
        #    "Bonus bal KES %0.2f." % (bet_id, bet_message, 
        #    current_balance, bonus_balance)
        bet_message = "JACKPOT ID  %s. placed successfully."\
            "Possible WIN  KSH %s, Account balance KSH %0.2f. Bonus balance KSH %0.2f"\
            % (bet_id, possible_win, current_balance, bonus_balance)
        self.jp_bet_status = 201
        self.bet_id = bet_id 
        return bet_message

    def _tostr(self, text):
        import unicodedata
        try:
            text = re.sub(r'[^\x00-\x7F]+',' ', text)
            if type(text) == str:
                text = unicode(text)
            return unicodedata.normalize('NFKD', text).encode('ascii','ignore')
        except Exception, e:
            return ""


    def invalid_jackpot_betslip_message(self, game_id, pick, jp,
         jackpot_type_key='jp'):

        self.logger.info("Extracting jackpot type %r" % jackpot_type_key)
        valid_picks = []
        #try:
        sql = "select odd_key from event_odd e inner join `match` m on m.parent_match_id "\
            " = e.parent_match_id inner join  jackpot_match jpm "\
	    " on jpm.parent_match_id = e.parent_match_id inner join jackpot_event je "\
            " on je.jackpot_event_id = jpm.jackpot_event_id inner join jackpot_type ty "\
            " on ty.jackpot_type_id =je.jackpot_type  where "\
	    " jpm.jackpot_event_id =  :jp_id and e.sub_type_id=ty.sub_type_id and "\
            " m.game_id=:game_id "
        jp_params = {"jp_id": jp.jackpot_event_id, "game_id":game_id}

        valid_picks = [self._tostr(r[0]).lower() for r in self.db_session.execute(sql_text(sql),
			jp_params).fetchall()]

        valid_pick_str =  "1,x,2" #self.scorepesa_configs["%s_%s"
        #      % (jackpot_type_key.lower(), 'valid_pick')]
        #except Exception as e:
        #    print e
        #return True, "Incorrect Jackpot Keyword. For jackpot bet send "\
        #    "JP#PICK1PICK2... to 29008. T&C apply."

        if jackpot_type_key != 'exact' and jackpot_type_key != 'auto':
            #valid_picks = valid_pick_str.split(",")
            self.logger.info("Found valid keys %r" % valid_picks)
            if not self._tostr(pick).lower() in valid_picks:
                return True, "Sorry, incorrect pick (%s) for GAMEID %s. "\
                    "Please review your selection and try again. "\
                    "T&C apply." % (pick, (game_id if game_id else ""))
        else:
            valid_picks = valid_picks + valid_pick_str.split(",")
            if not pick.lower() in valid_picks:
                valid_picks = map(lambda pick: pick.replace(":", "-"), iter(valid_picks))
                return True, "Incorrect pick (%s) for GameID %s. "\
                    "Review your seletion and try again. T&C apply." % \
                    (pick, game_id)

        match = self.db_session.query(Match)\
        .join(JackpotMatch, Match.parent_match_id ==
         JackpotMatch.parent_match_id).filter(Match.game_id == game_id,
            JackpotMatch.status == 'ACTIVE',
            JackpotMatch.jackpot_event_id == jp.jackpot_event_id).first()

        if not match:
            return True, "Sorry, incorrect GAMEID: %s. " \
                "For jackpot bet send JP#PICK1PICK2... " \
                "to 29008  scorepesa.co.ke. T&C apply." % (game_id, )

        return False, match

    def is_correct_score_format(self, pick):
        parts = pick.split("-")
        if not (len(parts) == 2):
            if len(parts) == 1:
                return True
            return False
        try:
            hm_score = int(parts[0])
            aw_score= int(parts[1])
        except ValueError:
            return False
        return True

    def invalid_live_betslip_message(self, parent_match_id, game_id, pick, 
        amount, sub_type_id=None, special_bet_value=None, current_odd=None):
        sub_type_id = sub_type_id if sub_type_id else self.default_sub_type_id
        self.logger.info("Livebet sub_type_id %r" % sub_type_id)

        if self.live_betting_disabled():
            return True, "Live betting is temporarily disabled. Kindly try again later"

        sport_sql = "select s.sport_id, betradar_sport_id, m.modified from `live_match` m " \
            " inner join competition c using(competition_id) inner join sport" \
            " s on s.sport_id=c.sport_id where parent_match_id=:pm_id "

        sport_sql_params = {"pm_id": parent_match_id}

        sport = self.db_session.execute(sql_text(sport_sql),
             sport_sql_params).fetchone()

        self.logger.info("sport result %r " % (sport))

        if not sport:
            return True, "Sport not found for Game Id {0} live event."\
                .format(game_id)

        sport_id, betradar_sport_id, modified = sport
        one_min_ago = datetime.now() - timedelta(minutes=1)
        if modified < one_min_ago:
            return True, "Live Betting Temporarily Disabled. Please try again later."

        self.logger.info("sport data %r :: %r :: %r" % (betradar_sport_id,
            sport_id, sport))
        if not special_bet_value:
            special_bet_value = ""

        live_sql = "select lo.parent_match_id, lo.odd_key, lo.odd_value, " \
			"lo.sub_type_id,lo.special_bet_value, lo.odd_active, lo.market_active "\
            " from live_odds_change lo inner join live_match lm using(parent_match_id) where " \
            " lo.parent_match_id=:pmId and lm.event_status=:status and " \
            " lo.odd_key=:pick and lo.sub_type_id=:sub_type_id and lo.special_bet_value = :sbv"\
            " order by lo.betradar_timestamp desc limit 1"
        
        sql_params = {"pmId": parent_match_id, "status": "Live", 
            "pick": pick, "sub_type_id": sub_type_id, "sbv":special_bet_value}
        self.logger.info("Validate live bet sql %s %r " % (live_sql, sql_params))

        live_odd = self.db_session.execute(sql_text(live_sql),
             sql_params).fetchone()

       
        if not live_odd:
            live_sql = "select lm.parent_match_id, lo.odd_key, lo.odd_value,"\
                "s.sub_type_id, lo.special_bet_value, lo.odd_active, lo.market_active "\
                "  from live_odds_change lo "\
                "inner join live_match lm using(parent_match_id) "\
                "inner join odd_key_alias oa on oa.odd_key=lo.odd_key inner" \
                " join odd_type s on s.sub_type_id=lo.sub_type_id where "\
                " lm.parent_match_id=:pmId and " \
                "lm.event_status=:status and oa.special_bet_value=" \
                "lo.special_bet_value and lo.sub_type_id=oa.sub_type_id " \
                "and oa.odd_key_alias=:pick order by lo.betradar_timestamp desc limit 1"
            sql_params = {"pmId": parent_match_id, "status": "Live",
                "pick": pick, "sub_type_id": sub_type_id,
                "spbval": special_bet_value}
            self.logger.info("SQL invalid live bet slip :%s: %r"
                % (str(live_sql), sql_params))
            live_odd = self.db.engine.execute(sql_text(live_sql), sql_params)\
                .fetchone()

        if not live_odd:
            return True, "Bet pick (%s) option not available for this" \
                " LIVE event." % (pick,)

        parent_match_id, odd_key, odd_value, sub_type_id, special_bet_value,odd_active, market_active =\
         live_odd
        if current_odd and current_odd != odd_value:
            return True, "Bet pick (%s) has odds update. Kindly accept the change by "\
                " clicking on PLACE BET again." % (pick,)

        if (odd_active != 1  or market_active != 'Active'):
            return True, "Bet pick (%s) option not available for this" \
                " LIVE event." % (pick,)

        return False, {"parent_match_id": parent_match_id,
             "odd_key": odd_key, "odd_value": odd_value,
             "sub_type_id": sub_type_id,
             "special_bet_value": special_bet_value,
             "sport_id": betradar_sport_id if betradar_sport_id else sport_id}
    

    def invalid_betslip_message(self, game_id, pick, amount, 
        sub_type_id=None, special_bet_value=None, o_dd_value=None):
        sub_type_id = sub_type_id if sub_type_id else self.default_sub_type_id
        self.logger.info("Extracting sub_type_id %r" % sub_type_id)

        if not special_bet_value:
            special_bet_value = ''
        c_pick = 0;
        if len(pick) <6 and pick != 'draw':
            self.logger.info("Pick less that 6 characters checking alias %r" % pick)
            alias_q = "select sub_type_id, odd_key, special_bet_value "\
                "from odd_key_alias where odd_key_alias=:pick";
            alias_d = self.db.engine.execute(sql_text(alias_q), {'pick':pick}).fetchone()
            if alias_d:
                sub_type_id, odd_key,special_bet_value = alias_d
                self.logger.info("DB ALIAS CHECK (sub_type %s, odd_key %s, spv %s )" % (sub_type_id, odd_key,special_bet_value))
                pattern = re.compile(r'(\{[^\}]*\})')
                kk= pattern.findall(odd_key);
                new_key = odd_key            
                for p in kk:
                    k = ','+p[1:-1]+','
                    new_key = new_key.replace(p, k)
                new_key  = new_key.strip(",")
                #override pick
                pick  = " concat("+new_key+") " if  kk  else "'"+odd_key+"'"   
                self.logger.info("Reloaded PICK as ==> " +pick )
                c_pick = 1
        if not 'concat' in pick:
            pick = pick[0] + pick[1:-1].replace("'","&apos;") + pick[-1]
        pick = "'"+pick+"'" if not c_pick else pick

        event_odd = self.db.engine.execute(
            sql_text("select m.match_id, m.bet_closure, c.max_stake," \
                "o.parent_match_id, o.special_bet_value, o.odd_value, " \
                "o.odd_key, o.sub_type_id, m.away_team, m.home_team, " \
                "c.sport_id, o.id_of_player from event_odd o inner join " \
                "`match` m on m.parent_match_id = o.parent_match_id inner " \
                "join competition c on c.competition_id = m.competition_id " \
                "where o.odd_key = "+pick+" and m.game_id=:game_id " \
                "and o.sub_type_id = :sub_type_id and o.special_bet_value "\
                " = :sp_bet_value and m.status=1"), 
                {
                    'pick':pick, 
                    'game_id':game_id,
                    'sub_type_id':sub_type_id,
                    'sp_bet_value': special_bet_value
                }
        ).fetchone()
        self.logger.info("SQL invalid bet slip :: %r" % (
            {'pick':pick, 'game_id':game_id,'sub_type_id':sub_type_id,
                'sp_bet_value': special_bet_value}))
        if not event_odd:
            return True, "The selected ID is not valid. "\
                "Review your selection and try again." 
            #% (pick, (game_id if game_id else ""))

        match_id, bet_closure, max_stake, parent_match_id, special_bet_value,\
            odd_value, odd_key, sub_type_id, home_team, away_team, sport_id, \
            id_of_player = event_odd
        if not match_id:
            return True, "The selected ID does not exit. "\
                "Please review your choice and try again"  
                #"Sorry, incorrect GAMEID: %s. For single bet send" \
                #" GAMEID#PICK#AMOUNT or for multibet" \
                #" GAMEID#PICK#GAMEID#PICK#AMOUNT to 29008" \
                #"scorepesa.co.ke T&C apply." % (
                #(game_id if game_id else ""), )
        self.logger.info("Comparing user_odd %r and db_odd %r, %r " % (o_dd_value, odd_value, (o_dd_value != odd_value)))
        #if o_dd_value and float(o_dd_value) != float(odd_value):
        #   return True, "The odds for match  %s vs %s have changed,"\
        #       " Accept the new odds and try again" % (home_team, away_team)

        if bet_closure < datetime.now():
            return True, "Betting time for match  %s vs %s"\
                " has expired, scorepesa.co.ke. Terms and conditions "\
                " apply." % (home_team, away_team)
        if max_stake > 0:
            if float(amount) > float(max_stake):
                return True, "Bet amount is greater that maximum "\
                    " allowed stake for this bet. "\
                    "The maximum allowed is  KES %s" % (
                    float(max_stake)
                )
                
                #"Stake amount for GAME ID %s exceeds the maximum" \
                #    " allowed. You can only stake upto KESs %0.2f for " \
                #    "this Game ID." % (game_id, float(max_stake))

        return False, {"match_id":match_id, "bet_closure":bet_closure,
            "max_stake":max_stake, "parent_match_id":parent_match_id,
            "special_bet_value":special_bet_value, "odd_value":odd_value,
            "odd_key":odd_key, "sub_type_id":sub_type_id, 
            "sport_id":sport_id, "id_of_player":id_of_player}

    def invalid_betslip_message_outright(self, betrader_competitor_id, 
        game_id, parent_outright_id, amount, event_name, 
        odd_type=None, special_bet_value='is null'):
        self.logger.info("invalid outright {0}::{1}::{2}::{3}::{4}::{5}"\
            .format(betrader_competitor_id, game_id, parent_outright_id, 
            amount, odd_type, special_bet_value))
        if not special_bet_value:
            special_bet_value = ''

        sqlQ = "select r.event_date, r.event_end_date, o.parent_outright_id," \
            " o.betradar_competitor_id, o.odd_value, o.odd_type, " \
            "o.special_bet_value, r.event_name, c.sport_id, " \
            "oc.betradar_super_id, oc.competitor_name from outright_odd o " \
            "inner join outright r on r.parent_outright_id = " \
            "o.parent_outright_id inner join competition c on " \
            "c.betradar_competition_id = r.competition_id inner join " \
            "outright_competitor oc on o.parent_outright_id = " \
            "r.parent_outright_id where o.parent_outright_id=:poid and " \
            "o.betradar_competitor_id=:bcid group by o.parent_outright_id" \

        event_odd = self.db.engine.execute(
            sql_text(sqlQ), {'bcid':betrader_competitor_id, 
                'poid':parent_outright_id,'spbv':special_bet_value}
        ).fetchone()
        self.logger.info("SQL invalid outright bet slip :: %r" 
            % ({'bcid':betrader_competitor_id, 'poid':parent_outright_id,
             'spbv':special_bet_value}))
        esqlQ = None
        if not event_odd:
            esqlQ="select r.event_date, r.event_end_date, " \
                " o.parent_outright_id, o.betradar_competitor_id, o.odd_value," \
                " o.odd_type, o.special_bet_value, r.event_name, c.sport_id," \
                " oc.betradar_super_id, oc.competitor_name from outright_odd " \
                " o inner join outright r on r.parent_outright_id = " \
                " o.parent_outright_id inner join competition c on " \
                " c.betradar_competition_id = r.competition_id inner join " \
                " outright_competitor oc on o.parent_outright_id = " \
                " r.parent_outright_id inner join odd_key_alias a on " \
                " (a.sub_type_id = o.odd_type and a.odd_key = " \
                " o.betradar_competitor_id and a.special_bet_value = " \
                " o.special_bet_value) where o.parent_outright_id=:potid " \
                " and a.odd_key_alias=:alias"

            event_odd = self.db.engine.execute(sql_text(esqlQ), 
            {"potid":parent_outright_id , "alias": betrader_competitor_id})\
            .fetchone()
            self.logger.info("SQL invalid outright bet slip oddalias sql::" \
            " %s :: %r :: %r" % (esqlQ, 
                {'oalias': betrader_competitor_id, 
                'potid': parent_outright_id}, event_odd))

        if not event_odd:
            return True, "Outright event#{0}//{1}/{2}/ was not found. " \
                "Kindly try again later or contact customer care."\
                .format(event_name, parent_outright_id, betrader_competitor_id)

        event_date,event_end_date,parent_outright_id, \
            betradar_competitor_id,odd_value,odd_type,special_bet_value,\
            event_name,sport_id,betradar_super_id,competitor_name = event_odd

        if not game_id:
            return True, "Outright event#{0}//{1}/{2}/ competitor was not" \
                " found. Kindly try again. T&C apply."\
                .format(event_name, parent_outright_id, betrader_competitor_id)

        if event_end_date < datetime.now():
            return True, "Outright event#{0}//{1}/{2}/ has already expired. " \
                "T&C apply.".format(event_name, parent_outright_id, 
                betrader_competitor_id)

        return False, {"event_date":event_date, 
            "event_end_date":event_end_date,
            "parent_match_id":parent_outright_id, 
            "special_bet_value":special_bet_value,
            "odd_key": betradar_competitor_id, 
            "odd_value":odd_value,
            "event_name": event_name, 
            "sub_type_id": odd_type, 
            "sport_id":sport_id, 
            "betradar_super_id": betradar_super_id, 
            "competitor_name": competitor_name}

    def get_betrader_sport_id(self, db_sport_id):
        sql = "select sport_id, betradar_sport_id from `sport` where " \
            "sport_id=:sport_id "
        sql_params = {"sport_id": db_sport_id}
        sport_result = self.db_session.execute(sql_text(sql), sql_params)\
            .fetchone()
        sport_id, betradar_sport_id = None, None
        if sport_result:
           sport_id, betradar_sport_id = sport_result
        
        return betradar_sport_id
    def get_sub_type_id(self, sub_type):
        try:
            sub_type_id = int(sub_type)
            return sub_type_id;
        except Exception, e:
            s = "select sub_type_id from odd_type where name=:sub_type"
            res = self.db.engine.execute(sql_text(s), {'sub_type':sub_type}).fetchone()
            return res[0]

    def invalid_single_bet_message(self, profile_id, parent_match_id, amount=0):
       
        #todays_bets_on_match = self.db.engine.execute(
        #    sql_text("select sum(bet_amount) from bet_slip join bet " \
        #        "using(bet_id) where bet_slip.parent_match_id = :p " \
        #        "and bet.profile_id=:pf and bet_slip.total_games=1 " \
        #        "and bet.status<>24"),
        #    {'p': parent_match_id, 'pf': profile_id}).fetchone()
         
        todays_bets_on_match = self.db.engine.execute(
            sql_text("select sum(bet_amount) from bet " \
                " where date(created) = curdate() " \
                " and bet.profile_id=:pf " \
                " and bet.status<>24"), {'pf': profile_id}).fetchone()
  
        l_sql = "select c.max_stake, m.competition_id, c.sport_id from " \
            "`match` m inner join competition c on c.competition_id = " \
            "m.competition_id where m.parent_match_id=:pmid limit 1"
        
        params = {'pmid': parent_match_id}
        match_league = self.db.engine.execute(sql_text(l_sql), params)\
            .fetchone()

        l_singleBetlimits = self.scorepesa_configs['single_bet_league_limit']\
            .split(",")

        self.logger.info("fetch match league [] {0} [] single bet limit " \
            "[] {1} [][]".format(match_league, l_singleBetlimits))
       
        max_stake, competition_id, sport_id = 0, 0, 0

        if match_league:
            max_stake, competition_id, sport_id = match_league
            self.logger.info("extract league match competition {0} [] " \
                "sport_id {1} [][]".format(competition_id, sport_id))

        if todays_bets_on_match and todays_bets_on_match[0]:
            self.logger.info("AMOUNT BET profileId {0} [] single " \
                "totalBetAmount {1} [] match {2} [][]"\
                .format(profile_id, todays_bets_on_match[0], parent_match_id))

            totalStake=float(todays_bets_on_match[0])+float(amount)

            self.logger.info("AdjusTING single STAKE AMOUNT %r, %r (%r) > %r" 
                % (todays_bets_on_match[0], float(amount), totalStake, 
                    self.scorepesa_configs['max_bet_amount_single']))

            if (float(todays_bets_on_match[0])+float(amount)) > \
                    float(self.scorepesa_configs['max_bet_amount_single']):
                return "Your total stake amount for today exceeds " \
                    "the maximum allowed. You have exhausted your limit of " \
                    " KES %0.2f for single bets." % \
                    float(self.scorepesa_configs['max_bet_amount_single'])

            self.logger.info("check single bet league limits ......")

            '''
              Check league limits for single bets
            '''
            for lsblimit in l_singleBetlimits:
                 competitionId, sbLimit = lsblimit.split(":") 
                 if str(competition_id) == str(competitionId):
                     if (float(todays_bets_on_match[0]) + float(amount)) > \
                         float(sbLimit):
                         return "Your total stake amount for this league " \
                            "exceeds the maximum allowed. You can only place " \
                            "bets of upto KESs %0.2f in stake amount for " \
                            "single bets." % float(sbLimit)
                     else:
                         break                  

        self.logger.info("return single bet response ......")

        return None

    def get_profile_setting(self, msisdn):
        msisdn = self.clean_msisdn(msisdn)
        if not msisdn:
            return 0
        sql = "select profile_id from profile inner join profile_settings "\
            "using(profile_id) where password is not null and msisdn =:msisdn "
        result = self.db.engine.execute(sql_text(sql), {"msisdn":msisdn}).fetchone()
        return 1 if result else 0


    def invalid_bet_message(self, profile_id, amount, possible_win=0):

        mx_daila_win = float(self.scorepesa_configs['max_daily_win'])
        mx_bet_amount = float(self.scorepesa_configs['max_bet_amount'])
        live_mx_bet_amount = float(self.scorepesa_configs['max_live_bet_stake'])
        tukosaa = datetime.now()

        if str(tukosaa.hour) in \
            self.scorepesa_configs["live_stake_limit_hours"].split(','):
            live_mx_bet_amount = \
                float(self.scorepesa_configs['hourly_live_bet_stake_limit'])

        self.logger.info("hourly stake limit profile{2} :: hour :: {0} ::" \
            " stake limit ::{1}"\
            .format(tukosaa.hour, live_mx_bet_amount, profile_id))

        psetting = self.db.engine.execute(
            sql_text("select max_stake, single_bet_max_stake, " \
                "multibet_bet_max_stake, max_daily_possible_win, status, name from" \
                " profile_settings where profile_id=:value"),
                {'value':profile_id}).fetchone()
        self.logger.info("profile settings configurations:: {0} ::"\
            .format(psetting))

        if psetting:
            self.name = psetting[5]
            if psetting[1] == 5:
                return "Your account is temporarily disabled. For help please" \
                    " contact customer care."

        if psetting and psetting[0]:
            self.punter_single_limit = psetting[1]
            self.punter_multibet_limit = psetting[2]
            self.daila_pwin_limit = psetting[3]
            max_stake_amount = psetting[0] if float(psetting[0]) > float(0) \
                else mx_bet_amount

            if float(amount) > float(max_stake_amount):
                return "Your stake amount exceeds the maximum allowed. " \
                    "You can only place bets of upto KES %0.2f in " \
                    "stake amount." % float(max_stake_amount)

        if psetting and float(psetting[1]) > float(0):
            if float(amount) > float(psetting[1]):
                return "Your maximum stake amount for singlebets is KES %0.2f."\
                    " T&Cs Apply." % float(psetting[1])

        if psetting and float(psetting[2]) > float(0):
            if float(amount) > float(psetting[2]):
                return "Your maximum stake amount for multibets is KES %0.2f." \
                    " T&Cs Apply." % float(psetting[2])

        if float(amount) > mx_bet_amount:
            return "Your stake amount exceeds the maximum allowed. You can " \
                "place bets of upto KESs %0.2f in stake amount." % \
                (mx_bet_amount, )

        if float(amount) > live_mx_bet_amount and self.livebetting:
            return "Sorry live bet maximum stake amount is KES.%0.2f. " \
                "Kindly review your stake amount and try again." % \
                (live_mx_bet_amount, )
        
        todays_bets_on_match = self.db.engine.execute(
            sql_text("select sum(bet_amount) from bet " \
                " where date(created) = curdate() " \
                " and bet.profile_id=:pf " \
                " and bet.status<>24"), {'pf': profile_id}).fetchone()
        totalStake = 0
        if todays_bets_on_match and todays_bets_on_match[0]:
            totalStake=float(todays_bets_on_match[0])+float(amount)

        self.logger.info("Invalid bet message  %r, %r (%r) > %r"
                % (todays_bets_on_match[0], float(amount), totalStake,
                    self.scorepesa_configs['max_bet_amount_single']))

        if totalStake > float(self.scorepesa_configs['max_bet_amount']):
            return "Your total stake amount for today exceeds " \
                "the maximum allowed. You have exhausted your limit of " \
                " KES %0.2f for todays bets." % float(self.scorepesa_configs['max_bet_amount_single'])

        '''
        `````````````````````````````````````````````````````
         This part will be done by winners processor app.
        ``````````````````````````````````````````````````````````

        todays_bets = self.db.engine.execute(
            sql_text("""
                select sum(possible_win) from bet where profile_id=:value and
                date(created) = curdate() and status in(1,5)
                """),
            {'value':profile_id}).fetchone()

        if todays_bets[0]:
          if psetting and float(psetting[3]) > float(0) and \
              float(todays_bets[0]) + float(possible_win) > float(psetting[3]):
              return "Your total win amount for today exceeds the "
              daily  maximum allowed. You can only bet upto a possible win"
              " of KESs %0.2f amount every day" % float(psetting[3])

          if float(todays_bets[0]) + float(possible_win) > mx_daila_win:
              return "Your total win amount for today exceeds the "
              "daily  maximum allowed. You can only bet upto a possible "
              "win of KESs %0.2f amount every day" % mx_daila_win"
        '''

        return None


    def place_peer_bet(self, connection, profile_id, parent_match_id, sub_type_id,
                special_bet_value, bet_amount, taxable_amount, tax, new_possible_win, pick):
        app_name = 'web-api'
        bet_dict = {
            "profile_id": profile_id,
            "bet_message": 'Peer Bet',
            "bet_amount": float(bet_amount),
            "total_odd": 2,
            "possible_win": Decimal(new_possible_win),
            "taxable_possible_win": Decimal(taxable_amount),
            "raw_possible_win": (float(bet_amount)*2),
            "tax":Decimal(tax),
            "status": 1,
            "win": 0,
            "reference":'BESTE_BET',
            "created_by": app_name,
            "created": datetime.now(),
            "modified": datetime.now()
        }
        self.peer_possible_win = new_possible_win 
        self.logger.info("bet dict to be inserted is %r" % (bet_dict))
        bet = connection.execute(Bet.__table__.insert(),
                 bet_dict)
        trace_id = bet_id = bet.inserted_primary_key
        self.peer_bet_id = trace_id
        sbv = special_bet_value
	bet_slip_dict = {
		"parent_match_id": parent_match_id,
		"bet_id": trace_id[0],
		"bet_pick": pick,
		"special_bet_value": sbv,
		"total_games": 1,
		"odd_value": 2,
		"win": 0,
		"live_bet": 0,
		"created": datetime.now(),
		"status": 1,
		"sub_type_id": sub_type_id
	}
	self.logger.info("bet_slip_dict to be inserted is %r" 
                    % (bet_slip_dict))
	connection.execute(BetSlip.__table__.insert(), bet_slip_dict)
	trx_debit_dict = {
		    "profile_id": profile_id,
		    "account": "%s_%s" % (profile_id, 'VIRTUAL'),
		    "iscredit": 0,
		    "reference": trace_id,
		    "amount": float(bet_amount),
		    "created_by": app_name,
		    "created": datetime.now(),
		    "modified": datetime.now(),
		    "status": 'COMPLETE'
        }
        self.logger.info("trx_debit_dict to be inserted is %r" 
                % (trx_debit_dict))
        trxd = connection.execute(Transaction.__table__.insert(),
                trx_debit_dict)
            
        trxd_id = trxd.inserted_primary_key
        self.bet_transaction_id2 = trxd_id[0]
        sql_pb = "INSERT IGNORE INTO profile_balance(profile_id, balance," \
                " bonus_balance, transaction_id, created) VALUES " \
                "(:pf, 0, 0, -1, NOW()) "
        self.db.engine.execute(sql_text(sql_pb),{'pf': profile_id})

        balance_update_Q = """update profile_balance set balance =
                (balance-%0.2f), bonus_balance=(bonus_balance-%0.2f)
                where profile_id=:profile_id limit 1""" % (\
                bet_amount, 0.00)
        connection.execute(sql_text(balance_update_Q),
                 {'profile_id': profile_id})
        self.beshte_bet_id = trace_id[0]
        return trace_id[0]

    def peerbet_transaction(self, connection, bet_slip, amount):
        place_bet = False
        peer_bet_id = bet_slip.get('peer_bet_id')
        parent_match_id = bet_slip.get('parent_match_id')
        sub_type_id = bet_slip.get('sub_type_id')
        commission = float(amount)*0.2
        pick = bet_slip.get('pick')
        peer_possbile_win = (float(amount)*2)-commission
        referred = self.clean_msisdn(bet_slip.get('peer_msisdn'))
        self.peer_msisdn=referred
        if peer_bet_id != '-1':
            self.logger.info("Found peer bet ID %s " % peer_bet_id)
            peer_bet_sql = "select profile_id, parent_match_id, sub_type_id, bet_amount,"\
               " '' as special_bet_value,pick, msisdn from peer_bet inner join profile using(profile_id) where peer_bet_id=:pb_id"

            result = self.db.engine.execute(sql_text(peer_bet_sql),{'pb_id': peer_bet_id}).fetchone()
            #update both bets as peer
            if not result:
                return True, None
            profile_id, parent_match_id, sub_type_id, bet_amount, special_bet_value, pick, msisdn = result
            amount = bet_amount
            commission = float(amount)*0.2
            taxable_amount = float(amount)-commission
            tax = taxable_amount*0.2
            new_possible_win = (float(amount)*2) - (tax + commission)

             
            self.place_peer_bet(connection, profile_id, parent_match_id, sub_type_id, 
                special_bet_value, amount, taxable_amount, tax, new_possible_win, pick)
            self.peer_bet_id= peer_bet_id    
            self.peer_msisdn = msisdn
            return True, amount
        else:
            new_peer = "INSERT INTO peer_bet(peer_bet_id, profile_id, bet_id, beshte_id,"\
            " parent_match_id, sub_type_id, pick, bet_amount, commission, peer_msisdn, status, created_by, created, modified)"\
            " values (null, :profile_id, null, null, :par, :sbt, :pick, :bet_amount, :comm, :msisdn, 'PENDING', "\
            " 'scorepesa-api', now(), now())" 
            res = self.db.engine.execute(sql_text(new_peer),
               {'msisdn':referred, 'par':parent_match_id, 'bet_amount':amount,
               'sbt':sub_type_id, 'comm':commission, 'profile_id':self.profile_id, 'pick':pick })
            return False, amount


    def place_bet(self, profile_id, bet_message, amount,
            bet_total_odd, possible_win, bet_slips,
             live_bet=None, app=None, jp=None):
        self.logger.info("in place_bet bet_slips : %r" % bet_slips)
        self.logger.info("in place_bet profile_id : %r" % profile_id)
        #if self.check_multiple_bet_same_game(profile_id, bet_slips, amount):
        #    return False
        game_count = len(bet_slips)
        app_name = 'SMS' if not app else app
        live_bet = 0 if not live_bet else live_bet
        bet_reference = "LIVE_MATCH" if self.livebetting else "PRE_MATCH"
        if live_bet == 2:
              bet_reference= "ScorepesaSpecials"
        bet_reference = "OURIGHT_BET" if self.outright_bet else bet_reference
        peer = len(bet_slips) == 1 and bet_slips[0].get('is_peer') == '1'
        bet_on_bonus, bet_on_balance = self.process_bonus_bet_amounts(amount, jp, peer)
        
        self.stake_on_cash = bet_on_balance
        self.logger.info("set bet on balance/cash global ::self:: "
            "{0}::bet on bal::{1}"\
            .format(self.stake_on_cash, bet_on_balance))
        if bet_on_bonus <= 0 and bet_on_balance <= 0:
            self.jp_bet_status = 421
            return  False

        if self.balance < bet_on_balance:
            self.jp_bet_status = 427
            return False
        ''' 
            Bonus limit minimum odd to 2
            If min odd is less check if a/c bal is enough-bonus
            If true proceed else return notification advising min odd for bonus bets
        '''
        
        if float(bet_on_bonus) >= \
            float(self.scorepesa_bonus_cfgs['min_bet_on_bonus']):
            #for slip in bet_slips:
            #    odd_value = slip.get("odd_value")
            #if len(bet_slips) \
            #    >= int(self.scorepesa_configs['bonus_bet_multibet']) \
            if float(bet_total_odd) \
                < float(self.scorepesa_configs['bonus_bet_minimum_odd']):
                if self.balance >= amount:
                    bet_on_bonus=float(0)
                    bet_on_balance=amount
                    self.bonus_bet_low_odd_msg = 'Minimum odds accepted for " \
                        "bets on bonus amounts is {0}. Please review selection"\
                        " and try again.'\
                        .format(self.scorepesa_configs['bonus_bet_minimum_odd'])
                else:
                    self.jp_bet_status = 423
                    return False

        #referal bonus adjust to stake of referred
        '''
        if bet_on_balance >= 50.0 and str(profile_id) not in 
            self.scorepesa_bonus_cfgs['referral_test_whitelist'].split(','):
             if self.scorepesa_configs['award_referral_bonus'] == '1':
                 referral_bonus = ReferralBonus(self.logger, profile_id)
                 result=referral_bonus.award_referral_bonus_after_
                 bet(profile_id, bet_on_balance, bet_total_odd)
        '''
        
        bet_status = 1 if not jp else 9

        give_bonus_on_multibet = False
        multi_bet_bonus_ratio = 0
        multi_bet_bonus_amount = 0

        # FOR TAX Reasons we do possbile_win value
        real_bet_amount = float(bet_on_balance)/1.2
        stake_tax = float(bet_on_balance) - real_bet_amount
        self.logger.info("Calculating bet stake taxes, amount %0.2f ==> %0.2f ==> %0.2f" % (bet_on_balance, real_bet_amount, stake_tax ))
        bet_amount_after_tax = real_bet_amount + float(bet_on_bonus)
        #amount = bet_amount_after_tax

        possible_win = float(bet_total_odd) * float(bet_amount_after_tax)
        if possible_win > float(self.scorepesa_configs['max_bet_possible_win']):
            possible_win = float(self.scorepesa_configs['max_bet_possible_win'])
        self.possible_win = possible_win


        self.logger.info("Bet on bonus : %r, Betslips : %r, limit :%r" 
            % (bet_on_bonus, len(bet_slips),
            int(self.scorepesa_configs['multibet_bonus_min_event_limit']) ) )
        if bet_on_bonus <= 0 and len(bet_slips) >= \
            int(self.scorepesa_configs['multibet_bonus_min_event_limit'])\
            and not (jp or peer):
            self.logger.info("Found multibet bonus : %r, %r " 
                % (possible_win, multi_bet_bonus_ratio) )
            multibet_possible_win, multi_bet_bonus_ratio = \
                self.calculate_multibet_bonus(
                bet_slips, possible_win
            )
            multi_bet_bonus_amount = multibet_possible_win - possible_win
            possible_win = multibet_possible_win
            self.multibet_possible_win = multibet_possible_win
            if multi_bet_bonus_ratio > 0: 
                give_bonus_on_multibet = True

        self.logger.info("Finally give_bonus_on_multibet : %r" 
            % (give_bonus_on_multibet) )
        discount_ratio = None
        if float(bet_on_balance) >= float(200) and not (jp or peer):
            multibet_num = int(self.scorepesa_configs['bet_discount_multibet'])
            testing = False
            if int(self.scorepesa_configs['bet_discount_testing']) == 1:
               testing = True
               if str(profile_id) in \
                   self.scorepesa_configs['bet_discount_whitelist'].split(','):
                    testing = False
            if game_count < multibet_num:
                testing = True
            if not testing:
                new_bet_on_balance,discount_awarded,discount_ratio = \
                    self.process_bet_amount_discount(bet_on_balance, jp)
                if float(new_bet_on_balance) > float(0):
                    bet_on_balance = new_bet_on_balance
                    amount = float(amount)-float(discount_awarded)
                    self.logger.info("Award bet discount profile_id :: %s " \
                        "bet amount after discount :: %r ::"\
                        "ratio :: %r :: discount amnt awarded :: %r" % \
                        (str(profile_id), amount, discount_ratio, 
                            discount_awarded))

        if float(bet_on_bonus) > float(0):
            bonus_possible_win, newpossible_win = \
                self.bonus_possible_award_calc(bet_on_bonus, 
                    amount, possible_win)
            if float(possible_win) > float(newpossible_win):
                possible_win = newpossible_win
                self.possible_win = newpossible_win
        self.logger.info("I get here, about to start inserting into bet")
        connection = self.db.engine.connect()
        trans = connection.begin()
        
        try:
            #CHECK just in case peer bet
            self.logger.info("CHECKING for peer bet, %s" % peer )
            commission = 0
            if peer:
                betsa = bet_slips[0]
                betsa['bet_amount'] = amount
                place_peer_bet, bb_amount = self.peerbet_transaction(connection, betsa, amount)
                if not place_peer_bet:
                    self.jp_bet_status = 700
                    return False 
                amount = bb_amount
            if peer:
                self.peer = 1
                commission = float(amount)*0.2
                possible_win = (float(amount)*2)-commission
                taxable_amount = float(amount)-commission
                #taxable_amount = float(possible_win)
                tax = taxable_amount*0.2
                net_possible_win = (float(bet_amount_after_tax)*2) - (tax + commission)

                bet_reference = 'BESHTE_BET'
            else:
		self.logger.info("Got values for odds: %s, possible win: %s, commission: %s, amount: %s"\
		    %(bet_total_odd, possible_win, commission, amount))
                taxable_amount = float(possible_win)-(float(bet_amount_after_tax) + commission) if not jp else float(possible_win)
                #taxable_amount = float(possible_win)
                tax = taxable_amount * 0.2
                net_possible_win  = possible_win - tax if not jp else possible_win
            #FUCK THE TAX THING
            #net_possible_win = round(possible_win)
                            
            bet_dict = {
                "profile_id": profile_id,
                "bet_message": 'Peer Bet' if peer else bet_message,
                "bet_amount": amount,
                "total_odd": Decimal(bet_total_odd),
                "possible_win": Decimal(net_possible_win),
                "taxable_possible_win": Decimal(taxable_amount),
                "raw_possible_win": possible_win if not jp else possible_win,
                "tax": Decimal(tax),
                "status": bet_status,
                "reference": bet_reference,
                "stake_tax":stake_tax,
                "win": 0,
                "created_by": app_name,
                "created": datetime.now(),
                "modified": datetime.now()
            }
            self.possible_win = net_possible_win 
            self.logger.info("bet dict to be inserted is %r" % (bet_dict))
            bet = connection.execute(Bet.__table__.insert(),
                 bet_dict)
            trace_id = bet_id = bet.inserted_primary_key
            if discount_ratio is not None:
                if float(discount_ratio) > float(0):
                    #record discount bet
                    bet_discount_dict = self.create_discount_bet_award(
                        discount_awarded, amount, profile_id, 
                        discount_ratio, trace_id)
                    bet_discount = connection.execute(
                        BetDiscount.__table__.insert(),bet_discount_dict)
                    bet_discount_id = bet_discount.inserted_primary_key
                    self.logger.info("Bet discount insert id ::%r:: discount"\
                        "dict::%r:: " % (bet_discount_id, bet_discount_dict))

            if float(bet_on_bonus) > float(0):
                self.logger.info("Someone pulled up for bonus bet ... yonga lumerera!")
                newpossible_win = self.update_bonus_bet(bet_on_bonus,
                    bet_id, amount, possible_win, profile_id,
                    app_name, connection, bet_total_odd)

            if self.referral_bonus_advise:
                trans.rollback()
                self.referral_bonus_advise_notification = \
                    "Minimum odds for referral bonus bet stake amounts "\
                    "is {0}."\
                    .format(self.scorepesa_bonus_cfgs['referral_bonus_minimum_odd'])
                return False

            elif give_bonus_on_multibet:
                self.logger.info("Calling give_bonus_on_multibet : %r" 
                    % (give_bonus_on_multibet) )
                self.award_multibet_bonus(bet_id, amount,  possible_win, 
                    multi_bet_bonus_ratio, app_name, connection )
                self.multibet_bonus_message = \
                    "You got KES %0.2f extra on possible win. "  % \
                    (multi_bet_bonus_amount,)
                self.bonusWin = float(multi_bet_bonus_amount)

            slip_data = []
            for slip in bet_slips:
                sbv = slip.get("special_bet_value") or slip.get("sbv")
                bet_slip_dict = {
                    "parent_match_id": slip.get("parent_match_id"),
                    "bet_id": trace_id,
                    "bet_pick": slip.get("pick"),
                    "special_bet_value": sbv if sbv else "",
                    "total_games": game_count,
                    "odd_value": slip.get("odd_value"),
                    "win": 0,
                    "live_bet": slip.get("live_bet", 0) or 0,
                    "created": datetime.now(),
                    "status": 1,
                    "sub_type_id": slip.get("sub_type_id")
                }
                self.logger.info("bet_slip_dict to be inserted is %r" 
                    % (bet_slip_dict))
                slip_data.append(bet_slip_dict)

            connection.execute(BetSlip.__table__.insert(), slip_data)
            #roamtech_id = self.get_roamtech_virtual_acc('ROAMTECH_VIRTUAL')
            trx_debit_dict = {
                "profile_id": profile_id,
                "account": "%s_%s" % (profile_id, 'VIRTUAL'),
                "iscredit": 0,
                "reference": "%s%s" % ('jp', bet_id) if jp else bet_id,
                "amount": amount,
                "created_by": app_name,
                "created": datetime.now(),
                "modified": datetime.now(),
                "status": 'COMPLETE'
            }
            self.logger.info("trx_debit_dict to be inserted is %r" 
                % (trx_debit_dict))
            '''
            trx_credit_dict = {
                "profile_id": roamtech_id,
                "account": "ROAMTECH_VIRTUAL",
                "iscredit": 1,
                "created_by": app_name,
                "reference": bet_id,
                "amount": amount,
                "created": datetime.now(),
                "modified": datetime.now()
            }
            self.logger.info("trx_credit_dict to be inserted is %r" 
                % (trx_credit_dict))
            '''
            trxd = connection.execute(Transaction.__table__.insert(),
                trx_debit_dict)
            
            trxd_id = trxd.inserted_primary_key
            '''
            connection.execute(Transaction.__table__.insert(),
                trx_credit_dict)
            '''
            self.bet_transaction_id = trxd_id[0]
            if int(self.scorepesa_freebet_cfgs['enable_free_bet_testing']) == 1:
                testing = None
                if str(profile_id) in \
                    self.scorepesa_configs["tests_whitelist"].split(','):
                    testing = True

            if testing is not None:
                #f ree bets award process
                if float(bet_on_balance) >= float(50) and len(bet_slips) >= \
                    int(self.scorepesa_freebet_cfgs['multibet_teams']):
                    daila_countx = self.countz_of_free_bet_daily_award(
                        connection, profile_id)
                    if daila_countx <= int(self.scorepesa_freebet_cfgs['daily_free_bet_limit']):
                        cfg_odd_avg = float(self.scorepesa_freebet_cfgs['average_odd_value'])
                        bet_avg_odd = float(bet_total_odd)/float(len(bet_slips))
                        if bet_avg_odd >= cfg_odd_avg:
                            self.create_free_bet_detail(connection, profile_id, bet_id)
               

            if jp:
                self.update_jackpot_bet(bet_id, jp, trxd_id, connection)
            sql_pb = "INSERT IGNORE INTO profile_balance(profile_id, balance," \
                " bonus_balance, transaction_id, created) VALUES " \
                "(:pf, 0, 0, -1, NOW()) "
            self.db.engine.execute(sql_text(sql_pb),{'pf': profile_id})
            self.logger.info("Balance update  profile_id %s, "\
                "bet on balance %0.2f, bet on bonus %0.2f " % (profile_id, bet_on_balance, bet_on_bonus))
            balance_update_Q = """update profile_balance set balance =
                (balance-%0.2f), bonus_balance=(bonus_balance-%0.2f)
                where profile_id=:profile_id limit 1""" % (\
                bet_on_balance, bet_on_bonus)
            connection.execute(sql_text(balance_update_Q),
                 {'profile_id': profile_id})
            if peer:
                connection.execute(sql_text("update peer_bet set bet_id =:bid, beshte_id=:bsid, "\
                    " status=:st where peer_bet_id = :pbid "), {"bid":self.beshte_bet_id, 
                    "bsid":trace_id[0], "pbid":self.peer_bet_id, 'st':'COMPLETE'})
            self.is_paid = True
            trans.commit()
            self.logger.info("Transaction saved success betID %r "
             % (trace_id, ))
            return trace_id[0]
        except Exception as e:
            trans.rollback()
            self.logger.error("Transaction creating bet, rolled back : %r " % e)
            return False

    def bonus_possible_award_calc(self, bet_on_bonus, bet_amount, possible_win):
         bonus_possible_win = (float(bet_on_bonus)/float(bet_amount)*possible_win)
         if bonus_possible_win > float(self.scorepesa_configs['max_win_on_bonus']):
            new_bonus_possible_win = float(self.scorepesa_configs['max_win_on_bonus'])
            new_possible_win = (possible_win - bonus_possible_win) \
                + new_bonus_possible_win
         else:
            new_bonus_possible_win = bonus_possible_win
            new_possible_win = possible_win
         return new_bonus_possible_win, new_possible_win

    def create_discount_bet_award(self, discount_amount_awarded, 
        bet_amount, profile_id, discount_ratio, bet_id):
         bet_discount_dict = {
                "bet_id": bet_id,
                "discount_amount": discount_amount_awarded,
                "ratio": discount_ratio,
                "status": 1,
                "created": datetime.now(),
                "modified": datetime.now()
            }

         return bet_discount_dict

    def process_bet_amount_discount(self, bet_on_balance, jp=None):
        if jp is None and int(self.scorepesa_configs['enable_bet_discount']) == 1:
            ratios = self.scorepesa_configs['discount_ratios'].split(",")
            self.logger.info("bet discount ratios {0}".format(ratios))
            discount_centage = 0
            for ratio_slice in ratios:
                ratio = ratio_slice.split(':')
                if float(bet_on_balance) <= float(ratio[0]):
                    discount_centage = float(ratio[1])
                    break
                self.logger.info("bet discount centage to offer {0}"\
                    .format(discount_centage))
                if discount_centage == 0 and len(ratios) >= 1:
                    ratio = ratios[len(ratios)-1].split(':')
                    discount_centage = float(ratio[1])

            new_bet_on_balance = ((float(100)-discount_centage)/float(100)) \
                * float(bet_on_balance) if discount_centage is not None \
                else bet_on_balance
            self.logger.info("new bet on balance after discount offer {0} "\
                .format(new_bet_on_balance))
            discount_amount = float(0)
            if bet_on_balance > new_bet_on_balance:
                discount_amount = float(bet_on_balance) \
                - float(new_bet_on_balance)

                self.logger.info("bet discont process:: betamount %r :: " \
                    "config ratios %r :: discount %r :: new amnt %r :: " \
                    "discount amount::%r" % (bet_on_balance, ratios, 
                    discount_centage, new_bet_on_balance, discount_amount))
                return float(new_bet_on_balance), \
                    float(discount_amount), float(discount_centage)
        return float(0),float(0),float(0)

    def update_jackpot_bet(self, bet_id, jackpot_event_id, trx_id, connection):
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

    def calculate_multibet_bonus(self, bet_slips, possible_win):
        if int(self.scorepesa_configs['award_multibet_bonus']) != 1:
            self.logger.info("award_multibet_bonus <> 1 : skipping bonus" )
            return possible_win, 0

        valid_odds = [slip.get("odd_value") for slip in bet_slips 
            if float(slip.get("odd_value")) > \
            float(self.scorepesa_configs['multibet_bonus_odd_limit']) ]

        self.logger.info("Valid odds = %r :" % valid_odds )
        raw_award_ratio = self.scorepesa_configs['multibet_bonus_event_award_ratio'] 

        award_ratio_dict  = dict([b.split(":") for b in \
            raw_award_ratio.split(",")])

        max_event_limit = self.scorepesa_configs['multibet_bonus_max_event_limit'] 

        bonused_percentage = 1
        if str(len(valid_odds)) in award_ratio_dict:
            bonused_percentage = 1+ float(award_ratio_dict.get(
                str(len(valid_odds)), '0'))/100
        elif len(valid_odds) >= max_event_limit.split(":")[0]:
            bonused_percentage = 1+ float(max_event_limit.split(":")[1])/100

        self.logger.info("New poddibe wim ratio: = %r :" % bonused_percentage )
        new_possible_win = possible_win*bonused_percentage

        return new_possible_win, (bonused_percentage-1)



    def award_multibet_bonus(self, bet_id, bet_amount,  possible_win, 
        ratio, app_name, connection):
        if int(self.scorepesa_configs['award_multibet_bonus']) != 1:
            return False

        bonus_bet_dict = {
            "bet_id":bet_id,
            "bet_amount":float(bet_amount),
            "possible_win":possible_win,
            "profile_bonus_id":None,
            "won": 0,
            "ratio":ratio,
            "created_by":app_name,
            "created":datetime.now(),
            "modified":datetime.now()
        }
        connection.execute(BonusBet.__table__.insert(), bonus_bet_dict)

        return True

    def update_outbox(self, outbox_id, d):
        sql= "update outbox set sdp_status=:st, reference=:ref, modified=now() where outbox_id=:id"
        update= self.db.engine.execute(
            sql_text(sql), 
	    {"st":d.get('status'), "ref":d.get('reference'), "id":outbox_id}
        )

    def update_bonus_bet(self, bet_on_bonus, bet_id, bet_amount, possible_win, 
        profile_id, app_name, connection, total_bet_odd):

        '''
           Get sum of claimed referal bonus and less from bonus bal is 
           result >= bet_on_bonus,
           proceed with bet and set flag referral_bonus_jump=True
           Elseset flag self.referral_bonus_advise=True and return 
           to exit and notify user.
        '''

        ref_Q="select sum(bonus_amount) from profile_bonus where "\
            "profile_id=:pifd and created_by in ('referral_message_re_award',"\
            " 'referral_message')"
        params = {"pifd": profile_id}
        referral_bonus_jump = False
        self.logger.info("refral bonus jump requsite {0}::{1}::{2} :: "\
            "{3} ::{4}....."\
            .format(total_bet_odd, 
            self.scorepesa_bonus_cfgs['referral_bonus_minimum_odd'], 
            bet_on_bonus, ref_Q, params))
        if float(total_bet_odd) < \
            float(self.scorepesa_bonus_cfgs['referral_bonus_minimum_odd']):
            ref_bonus = connection.execute(sql_text(ref_Q), params).fetchone()
            if ref_bonus and ref_bonus[0]:
                referral_claimed = float(ref_bonus[0])
                residue_bonus = self.bonus - referral_claimed
                if residue_bonus >= bet_on_bonus:
                    referral_bonus_jump = True
                else:
                    self.referral_bonus_advise = True
                    return False
            self.logger.info("Referral bonus jump betonbonus : " \
                "{0}::claimed flag:{1}"\
                .format(bet_on_bonus, referral_bonus_jump))

        self.logger.info("Bet on bonus : %0.2f" % (bet_on_bonus, ))
        r2 = connection.execute(sql_text(""" \
            select profile_bonus_id from profile_bonus \
            where profile_id=:pfid and
            status in ('CLAIMED', 'USED') order by profile_bonus_id desc limit 1"""),
            {'pfid': profile_id}).fetchone()

        if referral_bonus_jump:
            r2 = connection.execute(sql_text(
                "select profile_bonus_id from profile_bonus "\
                "where profile_id=:pfid and status = :st and created_by not in"
                \
                " ('referral_message_re_award', 'referral_message') " \
                "order by profile_bonus_id desc limit 1"),
                {'pfid': profile_id, 'st': 'CLAIMED'}).fetchone()

        if r2 or bet_on_bonus:
            new_bonus_possible_win, new_possible_win = \
                self.bonus_possible_award_calc(bet_on_bonus, 
                    bet_amount, possible_win)

            profile_bonus_id = r2[0] if r2 else -1
            self.bonusWin = new_bonus_possible_win
            bonus_bet_dict = {
                "bet_id":bet_id,
                "bet_amount":float(bet_on_bonus),
                "possible_win":new_bonus_possible_win,
                "profile_bonus_id":profile_bonus_id,
                "won": 0,
                "ratio":float(bet_on_bonus)/float(bet_amount),
                "created_by":app_name,
                "created":datetime.now(),
                "modified":datetime.now()
            }

            connection.execute(BonusBet.__table__.insert(), bonus_bet_dict)
            bonus_trx_dict = {
                "profile_id":profile_id,
                "profile_bonus_id":profile_bonus_id,
                "account":"%s_%s" % (profile_id, 'VIRTUAL'),
                "iscredit":0,
                "reference":bet_id,
                "amount":bet_on_bonus,
                "created_by":app_name,
                "created":datetime.now(),
                "modified":datetime.now()
            }
            connection.execute(BonusTrx.__table__.insert(), bonus_trx_dict)

            #invalidate profile bonus blocking on suceess bet
            if self.bonus_balance_amount:
                if float(self.bonus_balance_amount) - float(bet_on_bonus) < 1:
                    connection.execute(sql_text(
                        " update profile_bonus set status=:new_status, "\
                        " bet_on_status=:sta2 where profile_id = :pfid "\
                        " and status = :oldst "),{'new_status':'USED', 'sta2':2, 
                        'pfid':profile_id, "oldst":'CLAIMED'})

            return new_possible_win
        return possible_win

    def process_bonus_bet_amounts(self, amount, jp=None, peer=False):
        if int(self.scorepesa_configs['bet_on_bonus']) == 0:
            if self.balance < amount:
                return 0, -1
            return 0, amount

        if not self.balance:
            if self.bonus is None or self.bonus < 0:
                self.logger.error(
                    "Missing profile balance from self.balance."\
                    "NO BONUS OK, proceed with normal trx")
                return 0, -1
        if jp or peer:
            #Force jp and peer bet on real account balance
            if self.balance < amount:
                return 0, -1
            return 0, amount

        bonus_amount = self.bonus
        if bonus_amount <= 0:
            self.logger.info("No bonus proceeding with normal trs " \
                ": %0.2f " % bonus_amount)
            return 0, amount

        bet_on_bonus = amount
        bet_on_profile_balance = 0
        sql = "select count(*), sum(bet_amount) from bet where profile_id=:id and date(created)=curdate()"
        bets = self.db.engine.execute(
            sql_text(sql),{'id':self.profile_id}).fetchone()

        bets_count=0
        bet_amount = 0
        if bets:
            bets_count = bets[0]
            bet_amount = bets[1]
        bonus_config=self.scorepesa_configs['bonus_on_bets_count_award_ratio']
        max_bet_amount = self.scorepesa_configs['daily_bonus_bet_limit']

        bb_onf = bonus_config.split(',')
        if bets_count+1 > len(bb_onf):
            return 0, amount
        #self.logger.info("data bb_onf => %r, count %s " % (bb_onf, bets_count))
        bb_packt = bb_onf[bets_count]
        percentage = bb_packt.split(':')[1]

        bet_on_bonus = (float(percentage)/100)*float(amount)
        if  bet_on_bonus > max_bet_amount:
            bet_on_bonus = max_bet_amount
        if bet_on_bonus > self.bonus:
            bet_on_bonus = self.bonus

        bet_on_profile_balance = float(amount) - float(bet_on_bonus)
        #if bet_on_bonus > bonus_amount:
        #    bet_on_bonus = bonus_amount
        #    bet_on_profile_balance = float(amount) - float(bet_on_bonus)

        self.bonus_balance_amount = self.bonus
        return  bet_on_bonus, bet_on_profile_balance

    def check_bet_limit(self, amount, match_count):
        max_bet_multi = float(self.scorepesa_configs['max_bet_amount_multi'])
        max_bet_single = float(self.scorepesa_configs['max_bet_amount_single'])
        if float(amount) > max_bet_multi and match_count > 1:
            return "Your stake amount exceeds the maximum allowed for " \
                "multi-bet. You can place bets of upto KESs %0.2f amount" %\
                float(self.scorepesa_configs['max_bet_amount_multi'])

        if float(amount) > max_bet_single and match_count == 1:
            return "Your stake amount exceeds the maximum allowed for " \
                "single bet. You can place bets of upto KESs %0.2f amount" % \
                float(self.scorepesa_configs['max_bet_amount_single'])
        return False

    def check_outcome_exists(self, parent_match_id):
        outcome = self.db.engine.execute(
            sql_text("select match_result_id, home_team, away_team from outcome "
            "inner join `match` using(parent_match_id) where " \
                "parent_match_id = :p"),
            {'p':parent_match_id}).fetchone()

        if outcome:
            self.home_team = outcome[1]
            self.away_team = outcome[2]
            return outcome[0]
        return None

    def create_account_freeze(self, msisdn):
        try:
            account_freeze = AccountFreeze(
                msisdn=msisdn,
                status=1,
                created=datetime.now(),
                modified=datetime.now()
            )
            self.db_session.add(account_freeze)
            self.db_session.commit()
        except Exception, e:
            self.db_session.rollback()
            self.db_session.remove()
            self.logger.error(
                "Failed creating account freeze, rolled back : %r " % e)

    def get_roamtech_virtual_acc(self, acc):
        if acc == 'ROAMTECH_MPESA':
            if 'mpesa_roamtech_profile_id' in self.scorepesa_configs:
                return self.scorepesa_configs['mpesa_roamtech_profile_id']
            return 5

        if 'virtual_roamtech_profile_id' in self.scorepesa_configs:
            return self.scorepesa_configs['virtual_roamtech_profile_id']
        return 6

    def get_user_profile_data(self, msisdn):
        profile_data =\
         self.db_session.query(Profile).filter_by(msisdn=msisdn).first()
        if profile_data:
            return profile_data
        else:
            return None

    def get_roam_tech_vaccount_balance(self, profile_id):
        trx = self.db_session.query(Transaction).\
        filter_by(profile_id=profile_id)\
        .order_by(Transaction.id.desc()).first()
        if trx:
            return trx.running_balance
        return 0

    def create_profile(self, message, status=0):
        from sqlalchemy.sql import text
        new = False
        profile_id = message.get('profile_id')
        raw_text = message.get("message", '')
        profile_query = "select profile_id, msisdn, created, network " \
            "from profile where %s=:value limit 1"

        msisdn = self.clean_msisdn(message.get("msisdn"))
        if profile_id:
            profile_query = profile_query % ("profile_id")
            values = {'value':profile_id}
        else:
            profile_query = profile_query % ("msisdn")
            values = {'value': msisdn}

        profile = self.db.engine.execute(sql_text(profile_query),
                                values).fetchone()
        self.logger.info('Query profile sql %s' % (profile))

        if not profile:
            try:
                network = self.get_network_from_msisdn_prefix(msisdn)
                profile_dict = {
                    "msisdn":msisdn,
                    "created":datetime.now(),"modified":datetime.now(),
                    "status":status,"network":network,
                    "created_by":self.APP_NAME,
                }
                result_proxy = self.db.engine.execute(
                    Profile.__table__.insert(), profile_dict)
                profile_id = result_proxy.inserted_primary_key[0]
                msisdn = message.get("msisdn")
                created = profile_dict['created']
                profileUpdate = "INSERT IGNORE INTO profile_balance(profile_id, balance," \
                    " bonus_balance, transaction_id, created) VALUES " \
                    "(:pf, 0, :amount, :trx_id, NOW()) " 
                    #%\
                    #(float(self.scorepesa_configs['registration_bunus_amount']), )

                self.db.engine.execute(sql_text(profileUpdate),
                    {'pf': profile_id, 'amount': 0,'trx_id': -1})

                new = True
                self.logger.info("profile saved success : %r" 
                    % message.get('msisdn', None))
                #Add profile balance 
            except Exception as e:
                self.logger.error(
                    "Exception creating profile, rolled back : %r " % e)
                return -1, False
        else:
            profile_id, msisdn, created, network = profile
            
            self.logger.info("Checking user netwok for update ... : " \
                "%s, %s, provided %s" 
                % (msisdn, network,  message.get('network')))
            if not network or (network != message.get('network')) and \
                message.get('network') is not None:
                network = message.get('network')
                if network is None or network=='None' or network=='':
                    self.profile_no_network = True
                    network = self.get_network_from_msisdn_prefix(msisdn)
                self.logger.info("updating network as ....{0}".format(network))
                self.db.engine.execute(
                    sql_text("update profile set network=:net where " \
                        "profile_id=:pfid"),
                        {'net':network, 'pfid':profile_id})

        self.profile_id = profile_id
        self.msisdn = msisdn
	self.operator = network

        award_bonus = self.award_bonus(raw_text, msisdn)
        self.logger.info('Considering award registration bonus %r, %s' 
            % (award_bonus, msisdn))
        bonus_awarded = False
        if new and award_bonus:
            self.logger.info(
                'Found new profile and award register bonus processing ...')
            self.award_bonus_to_profile(profile_id, msisdn)
            bonus_awarded = True
        elif award_bonus and created.date() > \
            (datetime.today().date() - timedelta(days=5)):
            self.logger.info('Award bonus todays reg true')
            profile_balance = self.db.engine.execute(sql_text(
                "select bonus_amount from profile_bonus where profile_id=:value limit  1"),
                {'value':profile_id}).fetchone()
            if not profile_balance:
                self.logger.info('Profile bonus empty will award bonus')
                self.award_bonus_to_profile(profile_id, msisdn)
                bonus_awarded = True
            else:
                self.logger.info('Skipping award bonus profile balance exists')

        if bonus_awarded:
            self.logger.info('Bonus awarded sending bonus message %s' % msisdn)
            _text ='CONGRATULATIONS! you have been awarded KES. {0} bonus. Use your bonus to bet and WIN on Big!'.format(
                self.scorepesa_configs['registration_bunus_amount']);

            self.outbox_message(message, _text)
            message.update({'profile_id': profile_id,
               'outbox_id': self.outbox_id,
               'ref_no': self.outbox_id,
               'exchange':'SCOREPESA_SENDSMS',
               'message': _text,'text':_text,
           'msisdn': msisdn})
            #pb = Publisher(self.db_session, self.logger)
            #pb.publish_bonus_message(message)

        return profile_id, new

    def award_bonus_to_profile(self, profile_id, msisdn):
        connection = self.db.engine.connect()
        try:
            profile_bonus_dict = {
                "profile_id": profile_id,
                "referred_msisdn":msisdn, #bonus on same number
                "bonus_amount":self.scorepesa_configs['registration_bunus_amount'],
                "status":'CLAIMED',
                "expiry_date": datetime.now()+timedelta(days =1),
                "created_by":'registration_bonus',
                "bet_on_status": 1,
                "date_created": datetime.now(),
                "updated":datetime.now()
            }
            trans = connection.begin()
      
            result_proxy = self.db.engine.execute(
                ProfileBonu.__table__.insert(),
                profile_bonus_dict)
            profile_bonus_id = result_proxy.inserted_primary_key
            self.logger.info('Bonus creatd OK on registration %s ' % msisdn)

            #update profile for this dude to get bonus
            '''self.db.engine.execute(
               sql_text("update profile_bonus set status = 'CLAIMED' 
               where profile_bonus_id=:value"),
              {'value':profile_bonus_id})
            '''
            profileUpdate = "INSERT INTO profile_balance(profile_id, balance," \
                " bonus_balance, transaction_id, created) VALUES " \
                "(:pf, 0, :amount, :trx_id, NOW()) ON DUPLICATE KEY UPDATE " \
                " bonus_balance = (bonus_balance+%0.2f)" %\
                (float(self.scorepesa_configs['registration_bunus_amount']), )
                
            self.db.engine.execute(sql_text(profileUpdate), 
                {'pf': self.profile_id, 
                'amount': self.scorepesa_configs['registration_bunus_amount'], 
                'trx_id': -1})

            trans.commit()

            self.logger.info('Bonus claimed OK on registration %s ' % msisdn)
        except Exception, ex:
            trans.rollback()
            self.logger.info('Failed to award bonus on registration %s::%r '
             % (msisdn, ex))

    def award_bonus(self, message, msisdn):
        if int(self.scorepesa_configs['award_register_bonus']) == 1:
            #check registered no if airtel award registration bonus
            reg_msisdn = msisdn
            reg_operator = self.operator if self.operator is not None \
                else self.check_msisdn_operator(reg_msisdn, referred=True)
            self.logger.info("got registration operator [] {0} [] for " \
                "registered msisdn [] {1} []".format(reg_operator, reg_msisdn))

            if reg_operator not in \
                self.scorepesa_bonus_cfgs['registration_bonus_allowed_operators']\
                .split(','):
                return False
          
            return True 
            #message.lower() in self.scorepesa_configs['bonus_keywords'].split(',')

        return False

    def process_games(self, message):

        if self.scorepesa_configs['soccer_sport_id']:
            sport_id = self.scorepesa_configs['soccer_sport_id']
        else:
            sport = self.db_session.query(Sport)\
                .filter_by(sport_name='Soccer').first()
            sport_id = sport.sport_id if sport else 1

        profile_id, new = self.create_profile(message, 1)

        game_requests_Q ="select match_id from game_request where " \
            "profile_id=:profile_id and created > :today "


        todays_list_games = self.db.engine.execute(
                sql_text(game_requests_Q),
                {'profile_id':profile_id, 
                'today': time.strftime("%Y-%m-%d")}).fetchall()
	

        todays_list =  ",".join([str(result[0]) for result in todays_list_games ])
        
        self.logger.info("todays list of  already requested games %s::%r" 
            % (todays_list, todays_list_games))
        if not todays_list:
            todays_list = "0"

        games_sql = "select m.game_id,m.home_team, m.away_team," \
            " group_concat(concat( if(o.odd_key=m.home_team,  '1', "\
            " if(o.odd_key ='draw', 'X', '2')), '=', o.odd_value) "\
            " order by field(o.odd_key, m.home_team, 'draw', m.away_team)) as odds, m.match_id" \
            " from `match` m inner join event_odd o on " \
            " m.parent_match_id=o.parent_match_id inner join " \
            " competition c on c.competition_id=m.competition_id " \
            " where m.status=:status and m.bet_closure >now()" \
            " and m.bet_closure <= date_add(now(), interval 15 hour) and " \
            " m.match_id not in (%s) and c.sport_id=:sport_id " \
            " and o.sub_type_id=:sub_type_id group by m.parent_match_id " \
            " having odds is not null order by m.priority desc, m.start_time " \
            " asc, c.priority desc limit 5;""" % (todays_list,)

        games_result = self.db.engine.execute(
                sql_text(games_sql),
                {'status':1, 
                'game_requests':todays_list, 
                'sport_id':sport_id, 
                'sub_type_id':self.default_sub_type_id}).fetchall()

        self.logger.info("todays games sql %s %r" 
            % (games_sql, {'status':1, 'game_requests':todays_list, 
                'sport_id':sport_id, 'sub_type_id':self.default_sub_type_id}))
        if not games_result:
            self.logger.info("NO GAMES FOUND")
            return "Sorry, there are no more games to bet on right now. "\
                "Please try again later. Visit scorepesa.co.ke", new


        game_str = ""
        served_match_ids = []
        for _game_entry in games_result:
            game = _game_entry
            self.logger.info("GAME: %r, %r" % (game, self.default_sub_type_id))
            game_id = game[0]
            home_team = game[1]
            away_team = game[2]
            odds = game[3]
            served_match_ids.append(game[4])
            if game_str:
                game_str +='\n'
            game_str = game_str + "%s %s VS %s %s " % (
                #game.start_time,
                game_id,
                home_team,
                away_team,
                odds
            )
        self.logger.info("FULL GAME STRING: %r " % game_str)

        self.save_user_games(served_match_ids, profile_id)

        response = "%s%s" % (game_str, 
            "\nSMS ID#PICK#AMOUNT to 29008 to play")
        self.logger.info("GAME STRING RESPONSE: %r " % response)

        return response, new

    def process_jackpot_results(self, message, jp_key='auto', sub_type_id=1):
        try:
            self.logger.info("JP results request message %r" % message)
            profile, new = self.create_profile(message, 1)
            tm=datetime.now() - timedelta(days=15)
            qjp_event = self.db_session.query(JackpotEvent).filter(
                JackpotEvent.status == 'FINISHED', JackpotEvent.jp_key == jp_key,
                JackpotEvent.created >= tm.strftime('%Y-%m-%d %H:%M:%S'))\
                .order_by(JackpotEvent.created.desc()).limit(1)
            jp_event = qjp_event.first()

            self.logger.info("JP last finished event Request query:"\
                "%s result %r date %s" 
                % (qjp_event, jp_event, tm.strftime('%Y-%m-%d %H:%M:%S')))

            if not jp_event:
                return "There is no active JACKPOT results " \
                    "Kindly visit www.scorepesa.co.ke for more. Help. 0101 290080.", new

            q_jp_games = self.db_session.query(JackpotMatch)\
            .join(Match, JackpotMatch.parent_match_id == Match.parent_match_id)\
            .join(Outcome, Outcome.parent_match_id == Match.parent_match_id).filter(
                    JackpotMatch.jackpot_event_id == jp_event.jackpot_event_id,
                    Outcome.sub_type_id == sub_type_id,
                    JackpotEvent.jp_key == jp_key,
            Outcome.live_bet == 0,
            Outcome.is_winning_outcome == 1,
            JackpotEvent.status == 'FINISHED',
            JackpotEvent.jackpot_event_id == jp_event.jackpot_event_id,
            JackpotEvent.created >= tm.strftime('%Y-%m-%d %H:%M:%S'))\
            .add_columns(JackpotMatch.jackpot_event_id, Match.home_team,
                Match.away_team, Outcome.winning_outcome)\
                .order_by(JackpotMatch.game_order.asc())

            self.logger.info("JP results Request query: %s " % q_jp_games)
            jp_results = q_jp_games.all()
            self.logger.info("JP results request data: %r " % jp_results)

            if not jp_results:
                self.logger.info("NO Weeks JACKPOT Results FOUND")
                return "JACKPOT results for this week are not "\
                    "ready yet " \
                    "Please try again later. Ccs. 0101 290080.", new
            pre_str = 'JACKPOT Winning result #'
            nln = "\n"#'&#10;'
            xtr_inf = ''
            #xtr_inf =\
            #nln + " 12/12 - " + self.scorepesa_configs['bonus_jp_12_correct'] + \
            #    " each " + nln + "11/12 - KESs " + \
            #    self.scorepesa_configs['bonus_jp_11_correct'] + \
            #    " each " + nln + "10/12 - KESs. " + \
            #    self.scorepesa_configs['bonus_jp_10_correct'] + " each " +\
            #    nln + " New Jackpot (JP) KESs. " + \
            #    self.scorepesa_configs['jackpot_prize'] +\
            #    " SMS JP to 29008 to play. T & C apply." 

            #if jp_key.lower() != 'jp':
            #    pre_str = jp_key.title() + ' winning combination ' + jp_key + '#'
            #    xtr_inf = self.scorepesa_configs['bingwa5_results_msg']
            game_str = ''
            _result = ''

            i = 0
            for _result in jp_results:
                #self.logger.info("extracted result string: %r " % _result)
                if jp_key.lower() != 'jp':
                    if _result[4] and i > 0:
			if _result[4] != 'C':
				game_str += "#" + _result[4]
			else:
				game_str += "#other"
                    else:
                        game_str += _result[4]
                else:
                    game_str += _result[4]
            	i += 1
            self.logger.info("FULL JP RESULT STRING: %s " % game_str)

            response = "%s%s%s" % (pre_str, game_str, xtr_inf)
            self.logger.info("JP RESULT STRING RESPONSE: %r " % response)

            return response, new
        except Exception, e:
            self.logger.error("Exception %r" % e)
            return "Oops!, we could not process your JACKPOT match request."\
                "Please try again later. Visit scorepesa.co.ke", None

    def process_jackpot_games(self, message, jp_type):
        try:
            self.logger.info("JP games request message %r %r"
             % (message, jp_type))
            profile_id, new = self.create_profile(message, 1)
            jp_games = self.db_session.query(JackpotMatch)\
            .add_columns(Match.home_team, Match.away_team)\
                .filter(
                    JackpotMatch.status == 'ACTIVE',
                    Match.status == 1,
                    JackpotEvent.status == 'ACTIVE',
                    JackpotEvent.jp_key == jp_type)\
                .join(Match, JackpotMatch.parent_match_id ==
                 Match.parent_match_id)\
                .join(JackpotEvent, JackpotMatch.jackpot_event_id ==
                 JackpotEvent.jackpot_event_id)\
                 .order_by(JackpotMatch.game_order.asc())

            self.logger.info("JP Games Request result: %r " % jp_games)

            jp_games = jp_games.all()
            if not jp_games:
                self.logger.info("NO ACTIVE JACKPOT GAMES FOUND")
                return "Sorry, currently there is no active JACKPOT. \
    Please try again later. Visit scorepesa.co.ke", new
            pre_str = '%sBET Games: '\
             % ('Jackpot' if jp_type.upper() == 'JP' else jp_type.upper())
            game_str = ''
            n = 1
            for game in jp_games:
                ordr = str(n)
                game_str += '\n' + ordr + ". %s vs %s " % (
                     game[1] if len(game[1])
                     < 11 else game[1][:9] + '*',
                     game[2] if len(game[2])
                      < 11 else game[2][:9] + '*'
                )
                n += 1
            self.logger.info("FULL JP games STRING: %r " % game_str)
            if jp_type == 'jp' or jp_type == 'auto':
                post_str ='\n' + "SMS AUTO to 29008 or visit scorepesa.co.ke. T&C apply"
            else:
                post_str = '\n' + "SMS bingwa5#score1#score2# .... "\
                    "score5 e.g bingwa5#1-0#2-2#0-0#3-5#5-0 or " \
                    "visit scorepesa.co.ke. T&C apply"
            response = "%s%s%s" % (pre_str, game_str, post_str)
            self.logger.info("JP games STRING RESPONSE: %r " % response)

            return response, new
        except Exception, e:
            self.logger.error("Exception %r" % e)
            return "Oops!, we could not process your JACKPOT match request."\
                "Please try again later. Visit scorepesa.co.ke", None

    def save_user_games(self, game_ids, profile_id):
        request = None
        request_ids = []
        self.logger.info("my games %r" % game_ids)
        updateQ = "insert ignore into game_request (request_id, match_id, " \
            "profile_id, offset, created) values "
        values = []
        for game_id in game_ids:
            values.append( "(null, '%s', '%s', 1, now())" 
                % (game_id, profile_id))

        try:
            sql = "%s%s" % (updateQ, ','.join(values))
            if len(values) >0:
                self.db.engine.execute(sql_text(sql))

            self.logger.info("request saved success  ..%r:%r" % (sql, values) )

        except Exception as e:
            self.logger.error(
                "Exception creating game request : %r: %r " % (sql,e))


    """
    SMS Message may be send in the following formats
    @return tuple containig parsed message
    GAMES
    GAME#GAMEID#PICK#AMOUNT
    BALANCE
    WITHDRAW#AMOUNT
    """
    def parse_message(self, message):
        if not message:
            return None

        parts = message.split("#")
        self.info("Parsed message: %r, %r" % (message, parts))
        reg_keys = self.scorepesa_configs['registration_keywords']
        if 'scorepesapoint' in message.lower():
            self.message_type = 'SCOREPESAPOINT'
        elif filter(lambda y: y in message.lower(), reg_keys.replace(' ', '').split(',')): 
            if 'autobet' in message.lower():
                self.message_type = 'JACKPOT'
            elif 'result' in message.lower():
                self.message_type = 'JP_RESULT'
            elif 'auto' in message.lower():
                self.message_type = 'JP_MATCH'
            else:
                self.message_type = 'GAMES'
        elif 'next' in message.lower():
            self.message_type = 'GAMES'
        elif 'balance' in message.lower():
            self.message_type = 'BALANCE'
        elif 'w#' in message.lower() or 'withdraw' in message.lower():
            self.message_type = 'WITHDRAW'
        elif 'help' in message.lower():
            self.message_type = 'HELP'
        elif 'stop' in message.lower():
            self.message_type = 'STOP'
        elif 'accept' in message.lower():
            self.message_type = 'BONUS'
        elif 'bonus' in message.lower():
            self.message_type = 'BONUS_BALANCE'
        elif 'auto' in message.lower():
            if 'result' in message.lower():
                self.message_type = 'JP_RESULT'
            elif 'game' in message.lower():
                self.message_type = 'JP_MATCH'
            else:
                self.message_type = 'JACKPOT'
        elif 'jp' in message.lower():
            if len(message) == 2 and message.lower() == 'jp':
                self.message_type = 'JP_MATCH'
            elif 'result' in message.lower():
                self.message_type = 'JP_RESULT'
            else:
                self.message_type = 'FREEJACKPOT'
        elif 'bingwa' in message.lower():
            if len(message) == 7 and message.lower() == 'bingwa5':
                self.message_type = 'JP_MATCH'
            elif 'result' in message.lower():
                self.message_type = 'BINGWA_RESULT'
            else:
                self.message_type = 'JACKPOT'
        elif 'cancel' in message.lower():
            self.message_type = 'CANCEL_BET'
        else:
            if len(parts) == 1:
                self.message_type = 'UNKNOWN'
            else:
                try:
                    #game_id=int(parts[0])
                    parts = ['GAMES'] + parts
                    self.message_type = 'BET'
                except:
                    self.message_type = 'UNKNOWN'

        return parts

    def get_withdraw_details(self, msisdn):
        withdraw_limit = self.db_session.query(Withdrawal)\
        .filter(Withdrawal.msisdn == msisdn)\
        .order_by(Withdrawal.created.desc()).first()
        if withdraw_limit:
            return withdraw_limit
        return None

    def get_sum_withdraw(self, msisdn):
        after24hours = datetime.now() - timedelta(hours=24)
        sql = "select sum(amount) from withdrawal where created >= " \
            "date_sub(now(), interval 24 hour) and msisdn =:msisdn " \
            "and status=:status"""

        result = self.db.engine.execute(sql_text(sql),
            {'msisdn':msisdn, 'status':'SUCCESS'}).fetchone()

        if result and result[0]:
            return float(result[0])
        return 0

    def get_mpesa_charge(self, amount):
        sql = "select charge from mpesa_rate where min_amount <= :min " \
            "and max_amount >=:max limit 1"
        result = self.db.engine.execute(sql_text(sql), 
            {'min':amount, 'max':amount}).fetchone()
        if result and result[0]:
            return float(result[0])
        return float(33)

    def get_airtel_charge(self, amount):
        sql = "select charge from airtel_money_rate where " \
            "min_amount <= :min and max_amount >=:max limit 1"
        result = self.db.engine.execute(sql_text(sql), 
            {'min':amount, 'max':amount}).fetchone()
        if result and result[0]:
            return float(result[0])
        return float(20)

    def get_min_withdraw_amount(self):
        return Decimal(self.withdrawal_configs['min_amount'])

    def get_max_withdraw_limit(self):
        return float(self.withdrawal_configs['max_withdraw'])

    def get_max_withdraw_amount(self):
        return float(self.withdrawal_configs['max_amount'])

    def get_bonus_bet_profile_bonuses(self, profile_id):
        bet_count_qry = self.db_session.query(BonusBetCount)\
        .filter(BonusBetCount.num_bets >= 4,
                 BonusBetCount.profile_id == profile_id)
        self.logger.info("withdraw bonus bets query %s :: profileId %s" \
         % (bet_count_qry, profile_id, ))
        bet_count = bet_count_qry.all()
        if bet_count is not None:
            return bet_count
        return None

    def get_claimed_profile_bonuses(self, profile_bonuses):
        bonus_profile_ids = []
        for profile_bonus in profile_bonuses:
            bonus_profile_ids.append(profile_bonus.profile_bonus_id)
        claimed_bonus_qry = self.db_session.query(ProfileBonu).\
            filter(ProfileBonu.profile_bonus_id.in_[bonus_profile_ids],
                 ProfileBonu.status == 'CLAIMED').all()
        self.logger.info("withdraw claimed bonus query %s" % claimed_bonus_qry)
        claimed_bonus = claimed_bonus_qry.all()
        if claimed_bonus:
            return claimed_bonus
        return None

    def sum_bonus_bets_won(self, profile_bonus_ids):
        bonus_profile_ids = []
        for profile_bonuses in profile_bonus_ids:
            bonus_profile_ids.append(profile_bonuses.profile_bonus_id)
        bonus_bet_won_qry = self.db_session.\
        query(func.sum(BonusBet.possible_win).label("tot_bet_winnigs")).\
            filter(BonusBet.profile_bonus_id.in_[bonus_profile_ids],
                 BonusBet.win == 1)
        self.logger.info("withdraw sum_bonus_bets query %s" % bonus_bet_won_qry)
        winnings_bet_sum = bonus_bet_won_qry.all()
        if winnings_bet_sum:
            return winnings_bet_sum.tot_bet_winnigs
        return None

    def bonus_bet_limit_achieved(self, profile_id):
        bet_count = self.db_session.query(BonusBetCount).\
            filter_by(profile_id=profile_id).all()
        self.logger.info("withdraw in bonus_bet counts %s" % bet_count)
        if bet_count:
            return bet_count.num_bets
        return False

    def process_bonus_winnings_4_withdraw(self, profile_id, bonus_bal):
        return False
        profile_bonuses = self.get_bonus_bet_profile_bonuses(profile_id)
        return False
        self.logger.info("withdraw got profile bonuses %s" % profile_bonuses)
        if profile_bonuses:
            claimed_bonuses = self.get_claimed_profile_bonuses(profile_bonuses)
        else:
            return "NO_PROFILE_BONUS_BETS"
        self.logger.info("withdraw got claimed bonuses %s" % claimed_bonuses)
        if claimed_bonuses:
            won_bonus_bets_sum = self.sum_bonus_bets_won(claimed_bonuses)
        else:
            return "NO_BONUS_CLAIMED"
        self.logger.info("withdraw got sum of won bets %s" % won_bonus_bets_sum)
        if won_bonus_bets_sum >= 0 and won_bonus_bets_sum <= bonus_bal:
            self.logger.info("withdraw returning credit bonus bet won to trx")
            return self.credit_bonus_bet_won_to_trx(won_bonus_bets_sum,
                 profile_id, claimed_bonuses)
        return "BONUS_BALANCE_LESS_THAN_BETBONUS_WINNINGS"

        #return "To withdraw your bonus you have to bet with the bonus 
        #atleast 4 times. Your current bonus bet count is 0"
        #if _bonus_num_bets < 4:
            #return "To withdraw your bonus you have to bet on with the 
            #bonus atleast 4 times. Your current bonus bet count is %d" % _
            #bonus_num_bets
        #if _bonus_num_bets >= 4:
        #    return True
    def credit_bonus_bet_won_to_trx(self, won_bonus_bets_sum, 
        profile_id, claimed_bonuses):
        try:
            amount = float(won_bonus_bets_sum)
            for profile_bonus in claimed_bonuses:
                profile_bonus_id = profile_bonus.profile_bonus_id
                break
            trx_credit = Transaction(
                profile_id=profile_id,
                account="%s_%s" % (profile_id, 'VIRTUAL'),
                iscredit=1,
                reference="%s_%s" % ("BONUS_", profile_bonus_id),
                amount=amount,
                created_by=self.APP_NAME,
                created=datetime.now(),
                modified=datetime.now()
            )
            self.db_session.add(trx_credit)
            self.db_session.flush()
            roamtech_id = self.get_roamtech_virtual_acc('ROAMTECH_VIRTUAL')
            dbt_trx = Transaction(
                profile_id=roamtech_id,
                account="ROAMTECH_VIRTUAL",
                iscredit=0,
                created_by=self.APP_NAME,
                reference="%s_%s" % ("BONUS_", profile_bonus_id),
                amount=amount,
                created=datetime.now(),
                modified=datetime.now()
            )
            self.db_session.add(dbt_trx)
            self.db_session.flush()
            #flag profile bonuses to used
            for profile_bonus in claimed_bonuses:
                profile_bonus.status = "USED"
            self.db_session.commit()
            self.logger.info("withdraw created credit_bonus_bet_won_to_trx")
            return True
        except IntegrityError, e:
            self.logger.error("Integrity Error skipping trx save ...%r" % e)
            self.db_session.rollback()
            return "Error"
        except Exception, e:
            self.logger.error("Problem creating transaction message ...%r" % e)
            self.db_session.rollback()
            return False
        else:
            self.close()

    '''
    Check if bonus bal is zero then update all bonus award to used.
    If created by referral ignore coz will await bonus adjusting 
    after referred_msisdn bets.
    '''
    def check_profile_bonus_bet_status(self, msisdn, bonus_bal):
        self.logger.info("check bonus...msisdn::{0}::profile::{1}::"
            "bonus::{2}...".format(msisdn, self.profile_id, bonus_bal))

        sql = "select profile_bonus_id, sum(bonus_amount)bonus, created_by"\
            " from profile_bonus where profile_id=:prf and status=:state "\
            "and created_by <> 'referral_message' group by profile_bonus_id"
        result = self.db.engine.execute(sql_text(sql), 
            {'prf':self.profile_id, 'state':'CLAIMED'}).fetchone()

        if result:
            if result[0]:
                if float(bonus_bal) <= 0.0:
                    #update all user bonus awards to used and bet on
                    pbonusQ = "update profile_bonus set status=:state, "\
                        "bet_on_status=:bstatus where profile_id=:prid"
                    pbonus_params = {"prid": self.profile_id, 
                        "state":"CLAIMED", "bstatus":2}
                    rst = self.db.engine.execute(
                        sql_text(pbonusQ), pbonus_params)

                    self.logger.info("updated hang bonus ....profile{0} "
                        "::result{1}".format(self.profile_id, rst))
                    return True                           
        return True
    
    def check_account_freeze(self, message=None, profile_id=None):
        try:
            #get profile
            msisdn = message.get("msisdn") if message else ""
            if profile_id:
                pQ = "select msisdn from profile where profile_id=:pf limit 1"
                res = self.db.engine.execute(sql_text(pQ),
                 {'pf': profile_id}).fetchone()
                if res and res[0]:
                    msisdn = res[0]
                self.logger.info("profile msisdn %r :: %s" % (res, msisdn))
            fQ = "select msisdn from account_freeze where msisdn=:msisdn and"\
                " status=:status limit 1"
            result = self.db.engine.execute(sql_text(fQ),
                 {'msisdn': msisdn, 'status': 1}).fetchone()
            self.logger.info("account freeze result %r :: %r :: %r" 
                % (result, message, profile_id))
            if result and result[0]:
                return True
            return False
        except Exception, e:
            self.logger.error("Exception account freeze :: %r " % e)
            return False
	
    def get_profile_msisdn(self, profile_id):
        msisdn = ""
        if profile_id:
            pQ = "select msisdn from profile where profile_id=:pf limit 1"
            res = self.db.engine.execute(sql_text(pQ),
             {'pf': profile_id}).fetchone()
            if res and res[0]:
                msisdn = res[0]
            self.logger.info("get profile msisdn %r :: %s" % (res, msisdn))
	return msisdn

    def get_msisdn_profile_id(self, msisdn):
        profile_id = None
        if msisdn:
            mQ = "select profile_id from profile where msisdn=:mdn limit 1"
            res = self.db.engine.execute(sql_text(mQ), {'mdn': msisdn}).fetchone()
            if res and res[0]:
                profile_id = res[0]
            self.logger.info("get msisdn profile_id result [] %r [] msisdn"
                " [] %s [] profile_id [] %s[][]" % (res, msisdn, profile_id))
        return profile_id

    #queue message log transaction return response
    def process_withdrawal(self, message, text_dict):
        sanity_withdrawal = "select created from withdrawal where "\
             "msisdn=:msisdn and created > now()-interval 2 minute "
        lwithd = self.db_session.execute(
            sql_text(sanity_withdrawal), {'msisdn':message.get("msisdn")}).fetchone()
        if lwithd:
            return 'We are unable to process your withdrawal request "\
                "now due to another pending transaction, please try again later', 421

        balance, bonus_bal = self.get_account_balance(message)
        try:
            fQ = "select msisdn from account_freeze where msisdn "\
                "=:msisdn and status=:status limit 1"
            result = self.db.engine.execute(sql_text(fQ), 
                {'msisdn':message.get("msisdn"), 'status':1}).fetchone()
            if result and result[0]:
                return 'Withdrawal for this account has been temporary '\
                    'disabled.Read the terms and conditions.', 421
        except Exception, e:
            self.logger.error(
                "Exception getting account_freeze amount missing, : %r " % e)
            return 'We are unable to process your withdrawal request now'\
                ', please try again later', 421

        try:
            amount = float(text_dict[1])
        except Exception, e:
            self.logger.error("Exception withdrawal amount missing, : %r " % e)
            return 'Withdraw amount not properly specified. '\
                'Please send WITHDRAW#AMOUNT to 29008 to withdraw', 421

        clean_on_profile_bonus = self.check_profile_bonus_bet_status(
            message.get("msisdn"), bonus_bal)
        if not clean_on_profile_bonus:
            self.logger.info("Pending profile bonus claim advising to bet : %r " 
                %  message.get("msisdn"))
            return "Sorry you are required to bet on bonus awarded before"\
                " you are allowed to withdraw. Send word SCOREPESA to 29008 to "\
                "place your Bet. T&C Apply.", 421

        #balance, bonus_bal = self.get_account_balance(message)
        #bonus_bal = self.get_bonus_balance(message)
        min_amount = self.get_min_withdraw_amount()
        total_day_amount = self.get_sum_withdraw(message.get("msisdn"))

        self.logger.info("Withdraw total today : %r " %  total_day_amount)
        if float(amount) > float(self.get_max_withdraw_limit()):
            self.logger.info("Failing values %r + %r > %r ?" 
                % (float(total_day_amount), 
                    float(amount), float(self.get_max_withdraw_limit())))
            return "Maximum withdraw is limited to KESs.%0.2f. To exceed"\
                " the set limit please call customer care."  % \
                self.get_max_withdraw_limit(), 421

        self.logger.info("Max withdraw limit : %r " 
            %  self.get_max_withdraw_limit())

        if (float(total_day_amount) + float(amount)) > \
            float(self.get_max_withdraw_limit()):
            self.logger.info("Failing values %r + %r > %r ?" 
                % (total_day_amount, amount, 
                float(self.get_max_withdraw_limit())))
            return 'Maximum withdraw limit for today has been reached, next'\
                ' withdraw will be after 24hrs. For help please call '\
                'customer care.', 421

        charge = 0 #self.get_mpesa_charge(amount)
        operator = self.operator if not self.profile_no_network else \
            self.get_network_from_msisdn_prefix(message.get("msisdn"))

        self.logger.info("possible withdraw route operator {0} is db"\
            " network empty ? {1}".format(operator, self.profile_no_network))

        if self.profile_no_network:
            regex_operator = self.get_network_from_msisdn_prefix(
                message.get("msisdn"))
            self.logger.info("withdraw route comparing regex operator {0}"\
                " and saved db operator {1}".format(regex_operator, operator))

            if operator and regex_operator:
                if str(operator.upper()) != str(regex_operator.upper()):
                    operator = regex_operator
        
        self.logger.info("got withdraw operator to route:: {0}"\
            .format(operator))
        if operator == 'AIRTEL':
            #check whitelist
            if str(message.get("msisdn")) in \
                self.withdrawal_configs['airtel_whitelist'].split(','):
                return 'Airtel withdraw service is currently not available.'\
                    ' Kindly try again later.', 421
            charge = self.get_airtel_charge(amount)
            airtel_day_limit = \
                float(self.withdrawal_configs['airtel_max_withdraw'])
            if (float(total_day_amount) + float(amount)) > airtel_day_limit:
                self.logger.info("Failing values airtel %r + %r > %r ?" % \
                    (total_day_amount, amount, airtel_day_limit))
                return 'Maximum withdraw limit for today has been reached, '\
                    'next withdraw will be after 24hrs. For help please call'\
                    'customer care.', 421

        if operator == 'VODACOM' and \
            int(self.withdrawal_configs['safaricom_mpesa_outage']) == 1:
            return 'Sorry VODACOM withdraw service is not currently available. '\
                'Please try again later.', 421

        if operator == 'AIRTEL' and \
            int(self.withdrawal_configs['airtel_money_outage']) == 1:
            return 'Sorry Airtel withdraw service is not currently available.'\
                ' Please try again later.', 421

        if amount < min_amount:
            return 'Invalid withdraw amount. Your current balance is '\
                'KESs %0.2f. You can withdraw a minimun of  %0.2f' % \
                (balance, min_amount), 421

        max_amount = self.get_max_withdraw_amount()

        if amount > max_amount:
            return 'Invalid withdraw amount. Your current balance is '\
                'KESs %0.2f. The maximum possible amount to withdraw is '\
                'KESs %0.2f' % (balance, max_amount), 421

        if (balance - charge) < min_amount:
            return 'Insufficient balance. Your current balance is KESs'\
                ' %0.2f withdraw charges of KES %0.2f apply' % \
                (balance, charge), 421

        if balance < (amount + charge):
            return 'Insufficient balance. Your current balance is KES'\
                ' %0.2f. You can withdraw upto %0.2f' % \
                (balance, balance - charge), 421

        #insert withdrawal table
        result = self.save_withrawal(message, amount, charge, operator)

        if not result:
            response = 'Your request to withdraw KES %0.2f could not be'\
                'processed. Please try again later. %s' % \
                (amount, self.scorepesa_configs['scorepesa_fb_page_msg'])
            return response, 421

        message['created'] = result.get('created')
        message['withdrawal_id'] = result.get('withdrawal_id')

        message['request_amount'] = amount
        message['amount'] = amount

        message['charge'] = charge
        self.queue_withrawal(message, result, operator)

        response = 'Your request to withdraw KES %0.2f is being processed.' \
            'You will receive it shortly. %s' % \
           (amount, self.scorepesa_configs['scorepesa_fb_page_msg'])
        return response, 200

    def queue_withrawal(self, message, withdrawal, operator):
        pb = Publisher(self.db_session, self.logger)
        pb.publish(message, withdrawal, operator)

    def save_withrawal(self, message, amount, charge, operator):
        if not self.inbox_id:
            self.inbox_id = None

        withdraw_dict = {
            "msisdn":message.get('msisdn'), 
            "inbox_id":self.inbox_id,"created":datetime.now(),
            "raw_text":message.get("message", ''),
            "amount":amount,"reference":'',
            "charge":charge,"status":'TRX_SUCCESS',
            "created_by":self.APP_NAME, "network":operator
        }
        #amount = amount + charge

        connection = self.db.engine.connect()
        trans = connection.begin()

        try:
            result_proxy= connection.execute(
                Withdrawal.__table__.insert(),withdraw_dict)
            withdrawal_id = result_proxy.inserted_primary_key

            trx_credit ={
                "profile_id":self.profile_id,
                "account":"%s_%s" % (self.profile_id, 'VIRTUAL'),
                "iscredit":0,
                "reference":str(withdrawal_id[0]) \
                    + "_" + message.get("msisdn"),
                "amount":amount,
                "created_by":self.APP_NAME,"created":datetime.now(),
                "modified":datetime.now()
            }

            connection.execute(Transaction.__table__.insert(), trx_credit)
           
            updateQL = "update profile_balance set balance = balance - %0.2f"\
                "  where profile_id = :pf" % (amount, )
            connection.execute(sql_text(updateQL), {'pf':self.profile_id})
            trans.commit()

            withdraw_dict['withdrawal_id'] = withdrawal_id[0]
            withdraw_dict['request_amount'] = amount
            withdraw_dict['amount'] = amount
            withdraw_dict['charge'] = charge

            self.logger.info("request saved success : %r " 
                % (withdraw_dict['withdrawal_id']))
            return withdraw_dict

        except IntegrityError, e:
            self.logger.error("Integrity Error skipping trx save ...%r" % e)
            trans.rollback()
            return False
        except Exception, e:
            self.logger.error("Problem creating transaction message ...%r" % e)
            trans.rollback()
            return False
        else:
            self.close()

    def daily_bet_cancel_limit(self, profile_id):
         daily_cancels = self.daily_cancelled_bet_count(profile_id)
         cancel_limit = self.scorepesa_configs['cancel_times_limit']
         if int(daily_cancels) > int(cancel_limit):
             return "Your account has exhausted the daily bet cancellation"\
                " limit. T&C apply."
         return None

 
    def cancel_bet_trx(self, bet_id, app=None, profile_id=None, 
        message_dict=None):
        connection = self.db.engine.connect()
        dbtrx = connection.begin()
        try:
            if not app:
                self.APP_NAME = 'WEB_API'
            else:
                self.APP_NAME = app

            scorepesaPoint = ScorepesaPoint(self.logger, profile_id, connection)
            bet_result = scorepesaPoint.bet_is_free_jp(bet_id)
            if bet_result:
               return "Sorry, bet cancel is not allowed."
            bet_amount, profile_id, response = \
                self.get_bet_details_for_cancel(bet_id, profile_id, message_dict)
            res = self.daily_bet_cancel_limit(profile_id)
            if res is not None:
               return res

            if response:
                bet_on_bonus, bet_on_balance = \
                    self.get_bet_amount_ratio_to_refund(bet_id, profile_id)
                if bet_on_balance==float(0) and bet_on_bonus==float(0):
                    bet_on_balance = float(bet_amount)
                    bet_on_bonus=float(0)
                
            if not response:                
                return "BetID {0} Cancel failed. Confirm the BetId and ensure"\
                    " its within {1} minutes after placing."\
                    .format(bet_id, self.scorepesa_configs['cancel_bet_limit'])

            trx_dict = {
                "profile_id": profile_id,
                "account": "%s_%s" % (profile_id, 'VIRTUAL'),
                "iscredit": 1,
                "reference": "%s_%s" % ("CANCEL", bet_id),
                "amount": bet_amount,
                "created_by": self.APP_NAME,
                "created": datetime.now(),
                "modified": datetime.now()
            }
            connection.execute(Transaction.__table__.insert(), trx_dict)

            #flag bet and betslip status as cancelled
            if self.get_betslip_details_for_cancel(bet_id):
                u_sql = "update bet b inner join bet_slip s using(bet_id) "\
                    "set b.status=:bstatus, s.status=:sstatus WHERE "\
                    "b.bet_id=:bet_id and b.status=:bbstatus"
                u_params = {"bstatus":24, "sstatus":24, 
                "bet_id":bet_id, "bbstatus":1}
                connection.execute(sql_text(u_sql), u_params)                    

                #update balances and bonuses                    
                bal_update_Q = "update profile_balance set balance = "\
                    "(balance+%0.2f), bonus_balance=(bonus_balance+%0.2f) "\
                    "where profile_id=:profile_id limit 1" % \
                    (bet_on_balance, bet_on_bonus)
                connection.execute(sql_text(bal_update_Q), 
                    {'profile_id': profile_id})

                #update profile bonus incase of a bonus bet cancellation
                pbonus_update_Q = "update profile_bonus set status=:status, "\
                    "bet_on_status=:bet_on_status where "\
                    "profile_id=:profile_id1 and profile_bonus_id = "\
                    "(select profile_bonus_id from bonus_bet where "\
                    "bet_id=:bet_id and profile_id=:profile_id2 limit 1) "\
                    "limit 1" 
                profile_bonus_params = {"status": "CLAIMED", 
                    "bet_on_status": 1, "bet_id": bet_id, 
                    "profile_id1": profile_id, "profile_id2": profile_id}

                self.logger.info("update bonus query %s :: params :: %r" 
                    % (pbonus_update_Q, profile_bonus_params))
                connection.execute(sql_text(pbonus_update_Q), 
                    profile_bonus_params)
            else:
                dbtrx.rollback()
                return "Bet cancel failed, a match on BetID {0} "\
                    "is already underway.".format(bet_id)

            dbtrx.commit()
            self.logger.info("bet cancel transaction trx {0}::{1}::{2}::{3}"\
                .format(bet_on_balance, bet_on_bonus, bet_id, profile_id))
            return "Bet ID %s was successfully cancelled."\
                "Thank you." % (bet_id)

        except IntegrityError, e:
            self.logger.error(
                "Integrity Error skipping cancel trx save ...%r" % e)
            dbtrx.rollback()
            return "Oops! bet cancellation failed. Please retry cancelling."\
                " Helpline 0101 290080"
        except Exception, e:
            self.logger.error(
                "Problem creating cancel transaction message ...%r" % e)
            dbtrx.rollback()
            return "Bet cancellation failed. Please retry cancelling. "\
                "Helpline 0101 290080."
        else:
            self.close()

    def get_bet_details_for_cancel(self, bet_id, profile_id=None, 
        message_dict=None):
        minz = int(self.scorepesa_configs['cancel_bet_limit'])
        self.logger.info("config minutes %s" % minz)
        
        if message_dict is not None:
           profile_id, new = self.create_profile(message_dict, 0)

        t_sql = "select b.bet_amount, b.bet_id, p.profile_id, b.created, "\
            "b.reference from bet b inner join profile p using(profile_id) "\
            "where b.bet_id=:bet_id and p.profile_id=:ppid and b.created "\
            ">=date_sub(now(), interval {0} minute) and b.status=:status"\
            .format(minz)

        sql_params = {"bet_id": bet_id, "ppid": profile_id, "status":1}

        ticket = self.db_session.execute(sql_text(t_sql), sql_params).fetchone()
        self.logger.info("config pre-match bet cancel allowance minutes::"
            " %s | ticket:: %s |sql:: %s |params:: %s" 
            % (minz, ticket, t_sql, sql_params))
 	
        if ticket and ticket[0]:
            if str(ticket[4]) == 'LIVE_MATCH':
                return 0, 0, False
            bet_amount, bet_id, profile_id, created, reference = ticket
            self.logger.info("Got bet details betID {1}:: amount {0}:: "
                "profile {2}:: created {3} :: {4}"\
                .format(bet_amount, bet_id, profile_id, created,ticket))
            return bet_amount, profile_id, True
        return 0, 0, False

    def get_betslip_details_for_cancel(self, bet_id):
        s_sql = "select m.start_time from bet_slip s inner join `match` m "\
            "using(parent_match_id) where s.bet_id=:bet_id and "\
            "m.start_time < now()"

        sql_params = {"bet_id": bet_id}
        self.logger.info("config pre-match betslip sql %s :: params :: %s" % \
            (s_sql, sql_params))

        slip = self.db_session.execute(sql_text(s_sql), sql_params).fetchone()
        self.logger.info("Got betSlip detail :: {0}".format(slip))
        if slip and slip[0]:
           return False
        return True

    def get_bet_amount_ratio_to_refund(self, bet_id, profile_id):
        bb_sql = "select bb.bet_amount as bonus_bet_amount, b.bet_amount, "\
            "bb.ratio from bet b inner join bonus_bet bb using(bet_id) "\
            "where bet_id=:bet_id and profile_id=:profile_id and "\
            "profile_bonus_id is not null"
        sql_params = {"bet_id": bet_id, "profile_id": profile_id}
        self.logger.info("bets detail sql %s :: params :: %r" 
            % (bb_sql, sql_params))

        bet_detail = self.db_session.execute(sql_text(bb_sql), sql_params)\
            .fetchone()
        self.logger.info("Got bet amounts detail :: {0}".format(bet_detail))
        if bet_detail and bet_detail[0]:
           bonus_bet_amount, bet_amount, ratio = bet_detail
           bet_on_bonus = bonus_bet_amount
           bet_on_balance = bet_amount-bet_on_bonus
           return float(bet_on_bonus), float(bet_on_balance)
        return float(0), float(0)

    def get_game_id(self, parent_match_id):
        match = self.db_session.query(Match)\
        .filter_by(parent_match_id=parent_match_id).first()
        if match:
            return match.game_id
        return None

    def get_live_game_id(self, parent_match_id):
        match = self.db_session.query(LiveMatch)\
        .filter_by(parent_match_id=parent_match_id).first()
        if match:
            return match.game_id
        return None

    def get_parent_outright_id(self, game_id):
        s_sql = "select parent_outright_id from outright where game_id = :gid"
        sql_params = {"gid": game_id}
        self.logger.info("fetch outright parent id sql %s :: params :: %s" 
            % (s_sql, sql_params))
        result = self.db_session.execute(sql_text(s_sql), sql_params)\
            .fetchone()
        self.logger.info("Got outright parent id detail :: {0}"\
            .format(result))
        if result:
           pot_id, = result
           return pot_id
        return None

    def get_outright_game_id(self, parent_outright_id):
        s_sql = "select game_id, event_name, o.competition_id, "\
            "c.competition_name from outright o inner join competition c "\
            "on c.betradar_competition_id=o.competition_id where "\
            "parent_outright_id = :poid"
        sql_params = {"poid": parent_outright_id}
        self.logger.info("fetch outright gameid, eventname sql %s :: "
            "params :: %s" % (s_sql, sql_params))
        result = self.db_session.execute(sql_text(s_sql), sql_params)\
            .fetchone()
        self.logger.info("Got outright gameid,eventname detail :: {0}"\
            .format(result))
        if result:
           game_id, event_name, competition_id, competition_name = result
           return game_id, event_name, competition_id, competition_name
        return None, None, None, None

    def write_to_file(self, msisdn, **kwargs):
        try:
            cur_dir = os.path.dirname(os.path.realpath(__file__))
            filename = os.path.join(cur_dir, 'files/bulksubs.txt')
            target = open(filename, 'a')
            target.write(msisdn)
            target.write("%0A")
            target.close()
        except Exception, ex:
            self.logger.error("Problem writing to target file(%s) ...%r"
             % (ex, filename))

    def daily_games(self, message, ignore_filter=False):
        sLimit = self.redis_configs['games_query_limit']
        if self.scorepesa_configs['soccer_sport_id']:
            sport_id = self.scorepesa_configs['soccer_sport_id']
        else:
            sport = self.db_session.query(Sport).filter_by(
                sport_name='Soccer').first()
            sport_id = sport.sport_id if sport else 1

        profile_id, new = self.create_profile(message, 1)

        game_requests_Q ="select match_id from game_request where "\
            "profile_id=:profile_id and created > :today "

        todays_list = False

        if not ignore_filter:
            todays_list_games = self.db.engine.execute(
                  sql_text(game_requests_Q),
                  {'profile_id':profile_id, 
                  'today': time.strftime("%Y-%m-%d")}).fetchall()

            todays_list =  ",".join([str(result[0]) \
                for result in todays_list_games ])
            self.logger.info("todays list of  already requested "\
                "games %s::%r" % (todays_list, todays_list_games))
            if not todays_list:
                todays_list = "0"

        games_sql = "select m.game_id,m.home_team, m.away_team,"\
            "group_concat(concat(o.odd_key, '=', o.odd_value) order by "\
            "field(o.odd_key, 1, 'x', 2)) as odds, m.match_id, "\
            "m.parent_match_id from `match` m inner join event_odd o on"\
            " m.parent_match_id=o.parent_match_id inner join"\
            "competition c on c.competition_id=m.competition_id where "\
            "m.status=:status and m.start_time > now()"\
            "and m.start_time <= date_add(now(), interval 15 hour) "\
            "and m.match_id not in ({0}) and c.sport_id=:sport_id "\
            "and o.sub_type_id=:sub_type_id group by m.parent_match_id "\
            "having odds is not null"\
            "order by m.ussd_priority desc, m.priority desc, "\
            "m.start_time asc limit {1}".format(todays_list, sLimit)

        games_result = self.db.engine.execute(
                sql_text(games_sql),
                {'status':1, 'game_requests':todays_list, 
                'sport_id':sport_id, 
                'sub_type_id':self.default_sub_type_id}).fetchall()
        self.logger.info("todays games sql %s %r" 
            % (games_sql, {'status':1, 'game_requests':todays_list, 
            'sport_id':sport_id, 'sub_type_id':self.default_sub_type_id}))
        if not games_result:
            self.logger.info("NO GAMES FOUND")
            self.no_games_found = True
            return "Sorry, there are no more games to bet on right now. "\
                "Please try again later. Visit scorepesa.co.ke", new


        game_str = ""
        served_match_ids = []
        for _game_entry in games_result:
            game = _game_entry
            self.logger.info("GAME: %r, %r" % (game, self.default_sub_type_id))
            game_id = game[0]
            home_team = game[1]
            away_team = game[2]
            odds = game[3]
            parent_match_id = game[5]
            served_match_ids.append(game[4])

            game_str = game_str + "%s:%s-%s(%s)#" % (
                #game.start_time,
                "{0}_{1}".format(game_id, parent_match_id),
                home_team if len(home_team) < 11 else home_team[:9]+'*',
                away_team if len(away_team) < 11 else away_team[:9]+'*',
                odds
            )
        self.logger.info("PREPARED GAME STRING: %r " % game_str)
        
        if not ignore_filter:
            self.save_user_games(served_match_ids, profile_id)

        response = game_str
        self.logger.info("API GAME STRING RESPONSE: %r " % response)

        return response

    def map_bet_to_mts_ticket(self, bet_id, mts_ticket):
        try:
            mts_sql = "INSERT INTO mts_ticket_submit(bet_id, mts_ticket, "\
                "created) VALUES (:bet_id,:ticket, :date) "
            pars={'bet_id': bet_id, 'ticket': mts_ticket, 
                'date': datetime.now().strftime('%Y%m%d%H%M%S')}
            result=self.db.engine.execute(sql_text(mts_sql), pars)
            self.logger.info("bet map to mts ticket %s :: %r :: %r" 
                % (mts_sql, result, pars))
            return result
        except Exception, e:
            self.logger.info("Exception insert to mts_ticket %r :: %s "
             % (e, mts_sql))
            return None

    def daily_cancelled_bet_count(self, profile_id):
        try:
            sql = "SELECT count(*) AS num, profile_id FROM bet WHERE "\
                "profile_id=:profile_id AND status=:status "\
                "AND date(created)=:date"
            dpars = {'profile_id': profile_id, 'status': 24, 
                'date': datetime.now().strftime('%Y%m%d')}
            result=self.db.engine.execute(sql_text(sql), dpars).fetchone()
            num, profile_id = result
            self.logger.info("bet daily cancel counts %s :: %r :: %r" 
                % (sql, num, dpars))
            return num
        except Exception, e:
            self.logger.info("Exception daily bet cancels %r :: %s " 
                % (e, sql))
            return 0

    def persist_kannel_send_sms(self, payload):
        try:
            sendsms = "INSERT INTO kannel.send_sms(momt, sender, receiver,"\
             "msgdata, time, smsc_id, sms_type, boxc_id, meta_data) "\
             "VALUES (:momt, :sender,:recv, :msg, :tm, :smsc, :type, :boxc, "\
             ":meta) "
            result=self.db.engine.execute(sql_text(sendsms), 
                {'momt': payload.get('momt'), 'sender': payload.get('sender'),
                'recv': payload.get('receiver'),
                'msg': payload.get('msgdata').replace('%','%25'),
                'tm': payload.get('time'),
                'smsc': payload.get('smsc_id'),
                'type': payload.get('sms_type'),
                'boxc': payload.get('boxc_id'),
                'meta': payload.get('meta_data')})
            return result
        except Exception, e:
            self.logger.info("Exception insert to kannel %r :: %s "
             % (e, sendsms))
            return None

    def clean_msisdn(self, msisdn):
        if not msisdn:
            return None
        _msisdn = re.sub(r"\s+", '', msisdn)
        res = re.match('^(?:\+?(?:[254]{3})|0)?(7[0-9]{8})$', _msisdn)
	if res:
	   return "254" + res.group(1)
	return None

    def get_mts_ticket_bet_map(self, bet_id, profile_id, cancel_reason):
        minz = int(self.scorepesa_configs['cancel_non_prematch_bet_limit']) or 60
        if int(cancel_reason) == 101:
           minz = int(self.scorepesa_configs['cancel_bet_limit'])

        mts_sql = "select m.bet_id, mts_ticket, m.created from "\
            "mts_ticket_submit m inner join bet b using(bet_id) "\
            "where m.bet_id=:bet_id and b.profile_id=:pid and "\
            "m.created >=date_sub(now(), interval {0} minute) "\
            .format(minz)
        
        sql_params = {"bet_id": bet_id, "pid": profile_id}
        self.logger.info("config cancel allowance minutes %s :: %s" 
            % (minz, mts_sql))

        mts_ticket = self.db_session.execute(sql_text(mts_sql), 
            sql_params).fetchone()
        if mts_ticket and mts_ticket[0]:
            bet_id, mts_ticket, created = mts_ticket
        else:
            bet_id, mts_ticket, created = None, None, None
        self.logger.info("mts ticket sql data %s :: %s :: %s"
            " :: %s :: %s :: %r" % (profile_id, bet_id, mts_ticket, 
            created, mts_sql, sql_params))
        return bet_id, mts_ticket, created

    def betrader_bet_cancel_submit(self, bet_id, cancel_reason, 
        message_dict=None, msisdn=None, profile_id=None):
        try:
            self.logger.info("Betrader bet cancel params %s::%s::%r::%s " 
                % (bet_id, cancel_reason, message_dict, profile_id))
            if int(self.scorepesa_configs["enable_mts_bet_cancel"]) == 0:
                return False
            if msisdn not in self.scorepesa_configs["tests_whitelist"].split(','):
                if self.scorepesa_configs['tests_whitelist_boolean'] != "1":
                   return False
                   
            if message_dict:
                profile_id, new = self.create_profile(message_dict, 0)
            
            res = self.daily_bet_cancel_limit(profile_id)
            if res is not None:
               return res

            #bet_id, mts_ticket, created = self.get_mts_ticket_bet_map(bet_id, 
            #    profile_id, cancel_reason)
            #self.logger.info("mts cancel ticket submitted %s::%s::%s" 
            #    % (bet_id, mts_ticket, created))
            #if mts_ticket is not None:
            #     key = "%s%s" % ("CANCEL", str(cancel_reason))
            #     desrc = self.scorepesa_configs[key.lower()]
            #     ltd_id = self.scorepesa_configs['betrader_ltd_id_internet']
            #     payload = {"version": "1.2",
            #        "extTicket": mts_ticket,
            #        "ts": datetime.utcnow().strftime('%Y%m%d%H%M%S'),
            #        "bookmakerId": self.scorepesa_configs['sportrader_bookmark_id'],
            #        "cancellationReason": cancel_reason,
            #        "reasonMessage": desrc
            #     }
            #     self.logger.info("Betrader payload %r" % payload)
            #     kwargs = {"exchange": self.scorepesa_configs['cancel_exchange'],
            #       "queue": self.scorepesa_configs['cancel_queue'],
            #       "rkey": self.scorepesa_configs['cancel_rkey'],
            #       "rkeyheader": self.scorepesa_configs['cancel_rkeyheader']
            #     }

            #    return self.publish_validate_bet_betrader(payload, 
            #        bet_id, **kwargs)
            return False
        except Exception, e:
            self.logger.error("Problem invoke betrader cancel submit...%r" % e)
            return False

    def get_network_from_msisdn_prefix(self, msisdn):
        try:
            regexs = {
              "SAFARICOM": '^(?:254|\+254|0)?(7(?:(?:[12][0-9])|(?:0[0-9])|(9[0-9])|(4[0-9]))[0-9]{6})$',
              "AIRTEL": '^(?:254|\+254|0)?(7(?:(?:[3][0-9])|(?:5[0-6])|(8[5-9]))[0-9]{6})$',
              "ORANGE": '^(?:254|\+254|0)?(77[0-6][0-9]{6})$',
              "EQUITEL": '^(?:254|\+254|0)?(76[34][0-9]{6})$'
            }
            operator = "SAFARICOM"
            for rgx_key, rgx_val in regexs.iteritems():
               rs = re.match(rgx_val, str(msisdn.strip()))
               if rs:
                  operator = rgx_key.upper()
                  break
            return operator
        except Exception, e:
            return None

    def wapie_free_bet_trx(self, connection, profile_id, bet_id):
        tsql = "select free_bet_id from free_bet_transactions where "\
            "profile_id=:pf and bet_id=:bet_id"
        params = {'pf': profile_id, 'bet_id': bet_id}
        trx_res = connection.execute(sql_text(tsql), params).fetchone()
        if trx_res:
           return True
        self.logger.info("FREE BET TRX result :: {0} ::sql:: {1} "\
            "::params::{2}".format(trx_res, tsql, params))
        return False

    def countz_of_free_bet_daily_award(self, connection, profile_id):
        sql = "select count(*)countz from free_bet where profile_id=:pf"\
            " and event_date=curdate()"
        params = {'pf': profile_id}
        res = connection.execute(sql_text(sql), params).fetchone()
        countz = 0
        if res:
           countz, = res
        self.logger.info("Free Bet counts result :: {3} :: count :: {0} "\
            ":: sql :: {1} :: params :: {2}".format(countz, sql, params, res))
        return countz

    def count_of_free_bets(self, connection, profile_id):
        Csql = "select free_bet_id, no_of_bets from free_bet where "\
            "profile_id=:pf and event_date=curdate() and status=:state"
        params = {'pf': profile_id, 'state': 'INCOMPLETE'}
        bnum_res = connection.execute(sql_text(Csql), params).fetchone()
        num=0
        free_bet_id=0
        if bnum_res:
           free_bet_id, num = bnum_res
        self.logger.info("FREE BET result :: {3} :: count :: {0} "\
            ":: sql :: {1} :: params :: {2} ::freebet ::{4}"\
            .format(num, Csql, params, bnum_res, free_bet_id))
        return free_bet_id, num

    def create_free_bet_detail(self, connection, profile_id, bet_id):
        try:
            free_bet_amount = float(self.scorepesa_freebet_cfgs['free_bet_amount'])
            free_bet_id, num_bets = self.count_of_free_bets(connection, profile_id)
            self.logger.info("Creting free bet count :: {0} :: "
                "profile :: {1}:: free_bet_id :: {2}"\
                .format(num_bets, profile_id, free_bet_id))
            if num_bets is not None and num_bets < \
                int(self.scorepesa_freebet_cfgs['min_award_counts']):
                free_betQ = "INSERT INTO free_bet(profile_id, event_date, "\
                    "created) VALUES (:pf, NOW(), NOW()) ON DUPLICATE KEY "\
                    "UPDATE  no_of_bets = no_of_bets + 1"
                params = {'pf':profile_id}
                rproxy = connection.execute(sql_text(free_betQ), params)
                fbi = rproxy.lastrowid
                fbetTrxQ = "INSERT INTO free_bet_transactions(profile_id, "\
                    "bet_id, free_bet_id) VALUES (:pf, :bet_id, :free_bet_id)"

                params = {'pf':profile_id, 'bet_id':bet_id, 'free_bet_id':fbi}
                connection.execute(sql_text(fbetTrxQ), params)
            if num_bets is not None and num_bets == \
                (int(self.scorepesa_freebet_cfgs['min_award_counts'])-1):
                self.award_free_bet(connection, profile_id, free_bet_amount, 
                    num_bets, free_bet_id)
            else:
                return True
        except Exception, xe:
            self.logger.error("Create Free Bet Exception :: {0}".format(xe))
            raise

    def award_free_bet(self, connection, profile_id, free_bet_amount, 
        bets_num, free_bet_id):
        try:
            xminz = int(self.scorepesa_configs['cancel_bet_limit'])
            bal_updQ = "update profile_balance set bonus_balance = "\
                "(bonus_balance+%0.2f)  where profile_id=:pid limit 1"  % \
                (free_bet_amount)
            freeBetQ = "update free_bet set status='AWARDED', "\
                "free_bet_amount=:amount, to_award_on=:to_award_on where "\
                " profile_id=:pid and event_date=curdate() and status=:state"
            freeqparas = {'pid': profile_id, 'amount': free_bet_amount, 
                'state': 'INCOMPLETE', 
                'to_award_on': datetime.now()+timedelta(minutes=xminz)}
            qparas = {'pid': profile_id}
            trx_dict = {
                "profile_id": profile_id,
                "account": "%s_%s" % (profile_id, 'VIRTUAL'),
                "iscredit": 1,
                "reference": "%s-%s" % ('FREEBET', free_bet_id),
                "amount": free_bet_amount,
                "created_by": 'scorepesa_mo_consumer',
                "created": datetime.now(),
                "modified": datetime.now()
            }
            #trxid = connection.execute(Transaction.__table__.insert(), trx_dict)
            msisdn = self.get_mobile_number_from_profile(profile_id)
            profile_bonus_dict = {
                "profile_id": profile_id,
                "referred_msisdn": msisdn if msisdn is not None else profile_id,
                "bonus_amount": free_bet_amount,
                "status":'CLAIMED',
                "expiry_date": datetime.now()+timedelta(days =1),
                "created_by":'free_bet_bonus',
                "bet_on_status": 1,
                "date_created": datetime.now(),
                "updated":datetime.now()
            }
            result_proxy = self.db.engine.execute(
                ProfileBonu.__table__.insert(), profile_bonus_dict)
            profile_bonus_id = result_proxy.inserted_primary_key

            self.logger.info("Profile bal params :: {0} :: bal SQL :: {1} "\
                "::free bet sql :: {2} ::free params:: {3}:: TrxID :: {4}"\
                .format(qparas, bal_updQ, freeBetQ, freeqparas, profile_bonus_id))
            connection.execute(sql_text(bal_updQ), qparas)
            connection.execute(sql_text(freeBetQ), freeqparas)
            self.freebet_notification = \
                self.scorepesa_freebet_cfgs['freebet_notification']\
                .format(free_bet_amount)
            return True
        except IntegrityError, e:
           self.logger.error("AWARD Free Bet IntegrityError, :: {0}".format(e))
           return False
        except Exception, x:
           self.logger.error("AWARD Free Bet Exception :: {0}".format(x))
           raise

    def get_mobile_number_from_profile(self, profile_id):
         pQ = "select msisdn from profile where profile_id=:pf limit 1"
         res = self.db.engine.execute(sql_text(pQ), {'pf': profile_id}).fetchone()
         if res and res[0]:
            return res[0]
         return None

    def get_match_details(self, game_id, pick, sub_type=10, parent_match_id=None):
        try:
            self.logger.info("get_match_details data ....{0}::{1}::{2}::{3}"\
                .format(game_id, pick, sub_type, parent_match_id))
            if parent_match_id:
                 sql = "select sub_type_id, odd_key as pick_key, odd_value, "\
                    "m.parent_match_id from event_odd e inner join `match` m"\
                    " on e.parent_match_id=m.parent_match_id where "\
                    "m.parent_match_id=:pmid and e.sub_type_id=:sub_type "\
                    "and odd_key=:pick"
                 dpars = {'sub_type': sub_type, 'pick':pick, 
                    'pmid':parent_match_id}
            else:
                 sql = "select sub_type_id, odd_key as pick_key, odd_value, "\
                    "m.parent_match_id from event_odd e inner join `match` m"\
                    " on e.parent_match_id=m.parent_match_id where "\
                    "m.game_id=:gmid and e.sub_type_id=:sub_type and"\
                    " odd_key=:pick"

                 dpars = {'sub_type': sub_type, 'gmid':game_id, 'pick':pick}
            result=self.db.engine.execute(sql_text(sql), dpars).fetchone()
            if result:
               sub_type_id, pick_key, odd_value, parent_match_id = result
            else:
               sub_type_id, pick_key, odd_value, parent_match_id = \
                sub_type, pick, None, parent_match_id
            data={"sub_type_id": sub_type_id, "pick_key":pick_key, 
                "odd_value":odd_value, "parent_match_id":parent_match_id}
            self.logger.info("bet match detail fetch %s :: %r :: %r::%r" 
                % (sql, result, dpars, data))
            return data
        except Exception, e:
            self.logger.error(
                "Exception match detail fetch %r :: %s " % (e, sql))
            return None

    def search_for_match(self, search_term, msisdn, subtype=10):
        try:
            self.logger.info("bet match searchterm :: {0} :: msisdn :: {1}"\
                .format(search_term, msisdn))
            if search_term and msisdn:
                 search_term = "%{0}%".format(search_term)
                 sql = "select home_team, away_team, "\
                 "group_concat(concat(e.odd_key, '=', e.odd_value) order by"\
                 " field(e.odd_key, 1, 'x', 2)) as odds, "\
                 "sub_type_id, m.parent_match_id,m.game_id, m.start_time "\
                 "from event_odd e inner join `match` m on e.parent_match_id "\
                 "= m.parent_match_id inner join competition c on "\
                 "(c.competition_id=m.competition_id and c.sport_id=14) "\
                 "where (m.parent_match_id like :pmid or m.game_id "\
                 "like :gmid or m.home_team like :hometeam or m.away_team "\
                 "like :away_team or c.competition_name like :compname or "\
                 "c.category like :category) and (sub_type_id = 10 and "\
                 "start_time > now()) group by m.parent_match_id "\
                 "having odds is not null order by m.start_time asc LIMIT 20"
                 dpars = {'pmid': search_term, 'gmid':search_term, 
                    'hometeam': search_term, "away_team": search_term, 
                    "compname":search_term, "category":search_term, 
                    "subtype":subtype}
            else:
                return "Error missing required parameters"

            results = self.db.engine.execute(sql_text(sql), dpars).fetchall()
            self.logger.info(
                "bet match search sql :: %s ::params::: %r ::results::%r" 
                % (sql, dpars, results))
            match_string=''
            if results:
               for result in results:
                   home_team, away_team, odds, sub_type_id, parent_match_id, \
                   game_id, start_time = result
                   home_team = home_team if len(home_team) < 11 else home_team[:9]+'*'
                   away_team = away_team if len(away_team) < 11 else away_team[:9]+'*'
                   
                   match_string = match_string + "%s:%s-%s(%s)#" % (
                        "{0}_{1}".format(game_id, parent_match_id),
                        home_team if len(home_team) < 11 else home_team[:9]+'*',
                        away_team if len(away_team) < 11 else away_team[:9]+'*',
                        odds
                   )
            else:
               return "No results found."

            self.logger.info("bet match search got data ::: %r" % (match_string))
            return match_string
        except Exception, e:
            self.logger.error(
                "Exception match search ::: %r :: %s :: %s" 
                % (e, msisdn, search_term))
            return None

    def formatted_date(self, date_obj):
        s_format = "%Y-%m-%d %H:%M:%S"
        return date_obj.strftime(s_format)

    def info(self, text):
        self.logger.info(text)

    def debug(self, text):
        self.logger.debug(text)

    def fatal(self, text):
        self.logger.fatal(text)

    def error(self, text):
        self.logger.error(text)

