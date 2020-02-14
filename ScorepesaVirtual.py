from utils import LocalConfigParser
from db.schema import Db, Profile, Bet, EventOdd, Match, \
GameRequest, Transaction, BetSlip, Outbox, Inbox, ProfileBonu,\
 BonusTrx, BonusBet, MtsTicketSubmit, ProfileMap, TransactionMap
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
from sqlalchemy import desc

class ScorepesaVirtual(object):

    APP_NAME = 'scorepesa_mo_consumer_virtuals'

    def __del__(self):
        self.logger.info("Cleanup ScorepesaVirtual object.....")
        if self.db:
            self.db.close()
        
        if self.db_vt:
            self.db_vt.close()
            
        self.inbox_id = None
        self.outbox_id = None
        if self.bet_transaction_id:
            self.bet_transaction_id=None
        if self.stake_on_cash:
            self.stake_on_cash=None
        if self.bet_transaction_id:
            self.bet_transactioni_id=None

    def __init__(self, logger):
        self.logger = logger
        self.bonus_balance_amount = None
        self.db_configs = LocalConfigParser.parse_configs("DB")
        self.vtdb_configs = LocalConfigParser.parse_configs("VTDB")
        self.scorepesa_configs = LocalConfigParser.parse_configs("SCOREPESA")
        self.scorepesa_points_cfgs = LocalConfigParser.parse_configs("SCOREPESAPOINT")
        self.scorepesa_freebet_cfgs = LocalConfigParser.parse_configs("FREEBET")
        self.scorepesa_bonus_cfgs = LocalConfigParser.parse_configs("SCOREPESABONUS")
        self.scorepesa_virtuals_cfgs = LocalConfigParser.parse_configs("SCOREPESAVIRTUALS")
        self.logger.info("Scorepesa virtuals process init.....")
        self.profile_id=None
        self.stake_on_cash=None
        self.bet_transaction_id=None
        self.virtual_bet_error=None
        self.bonus, self.balance=None,None
        self.db_vt, self.db = self.db_factory()
        self.bet_string_message=''
        self.bet_odd_total=1.0
 
        super(ScorepesaVirtual, self).__init__()

    def db_factory(self):
         self.db = Db(self.db_configs['host'], self.db_configs['db_name'],
            self.db_configs['username'], self.db_configs['password'], 
             socket=self.db_configs['socket'], port=self.db_configs['port'])

         self.db_vt = Db(self.vtdb_configs['host'], self.vtdb_configs['db_name'],
            self.vtdb_configs['username'], self.vtdb_configs['password'], port=self.vtdb_configs['port'])

         return self.db_vt, self.db
     
    def place_virtual_bet(self, bet_message):
         try:
            profile_id = bet_message.get("profile_id")
            msisdn = bet_message.get("msisdn")
            bet_stake = bet_message.get("stake_amount")
            bet_stake = abs(float(bet_stake))
            app_name = bet_message.get("app_name")
            if float(bet_stake) < 1.0:
               return "Sorry we could not place your bet.", 421

            if float(bet_stake) < float(self.scorepesa_virtuals_cfgs["virtuals_min_stake_amount"]):
                return "Sorry virtuals minimum bet stake amount is Kshs. {0}.".format(float(self.scorepesa_virtuals_cfgs["virtuals_min_stake_amount"])), 421

            betslips=bet_message.get("slip")
            bet_total_odd=1.0
            self.balance, self.bonus = self.get_account_balance(profile_id)

            profile_map_id = self.create_profile_mapping(profile_id)
            if profile_map_id is None:
               return "Sorry we could not process your bet.", 421

            self.logger.info("in placing virtual bet msisdn::{0}::betslip::{1}::profile::{2}::{3}::profile_map::{4}".format(msisdn, len(betslips), profile_id, bet_stake, profile_map_id))
            #validations
            response, result = self.bet_amounts_validation(bet_stake, profile_id, betslips)
            if result:
               return response, 421

            bet_type=bet_message.get("bet_type")

            result, response = self.bet_odds_validation(betslips, bet_stake, bet_type) 
            if result:
               return response, 421

            bet_total_odd = self.bet_odd_total
            bet_string = "{0}#{1}".format(self.bet_string_message[1:], bet_stake)

            possibleWin = float(bet_stake)*float(bet_total_odd)

            self.logger.info("virtual bet totalodd::{1}::posbleWin::{2}::profile::{0}::stake::{3}".format(profile_id, bet_total_odd, possibleWin, bet_stake))
            #place bet transaction
            no_bonus_use=False
            if int(self.scorepesa_virtuals_cfgs['scorepesa_virtuals_bet_on_bonus']) == 1:
                no_bonus_use=True

            bet_on_bonus, bet_on_balance = self.bet_stake_spread_on_bals(profile_id, bet_stake, no_bonus_use)

            self.logger.info("betOnbal::{0}::betOnbonus::{1}::stake::{2}:::".format(bet_on_balance, bet_on_bonus, bet_stake))

            if float(bet_stake) > (float(bet_on_balance) + float(bet_on_bonus)):
                return "Sorry insufficient balance, kindly top up your Scorepesa account and try again.", 421

            bet_id = self.bet_transaction(profile_id, bet_string, bet_stake, bet_on_bonus, bet_on_balance, bet_total_odd, possibleWin, response, app_name)

            self.balance, self.bonus = self.get_account_balance(profile_id)
            self.logger.info("finally finished create bet {0}::{1}::{2}".format(bet_id, self.balance, self.bonus))

            if self.virtual_bet_error == 423:
               return "Sorry we could not place your bet right now. Please try again later.", 421
            message=False
            if bet_id:
               message = "BetID {0}, {1}, possible win Kshs.{4}. Bal Kshs.{2}. Bonus Kshs.{3}.".format(bet_id, bet_string, self.balance, self.bonus, possibleWin)
            else:
               return "Sorry we could not process your bet right now. Please try again later.", 421
            return message, 201
         except Exception, e:
            self.logger.info("Exception in place virtual bet.... {0}".format(e))
            return "Sorry we could not place your bet right now. Please try again later.", 421
       
    def bet_odds_validation(self, betslips, bet_amount, bet_type):
         self.logger.info("virtual bet validate bet slip :: {0}::{1}::{2}".format(len(betslips), bet_amount, bet_type))
         bet_string=[]
         response=[]
         
         for slip in betslips:
             game_id=slip.get('game_id') or ''
             parent_virtual_id=slip.get('parent_virtual_id')
             odd_type=slip.get('sub_type_id')
             special_bet_value=slip.get('special_bet_value')
             pick_key = slip.get('pick_key')

             if pick_key is None or odd_type is None or parent_virtual_id is None:
                  return True, "Sorry we could not place your virtual bet right now. Kindly try again later."          
         
             self.logger.info("invalid virtual {0}::{1}::{2}::{3}::{4}::".format(game_id, parent_virtual_id, bet_amount, odd_type, special_bet_value))
             if not special_bet_value:
                special_bet_value = ''

             sqlQ = """SELECT vm.start_time,ve.odd_key,vm.parent_virtual_id,ve.odd_value,ve.sub_type_id,ve.special_bet_value,vm.home_team,vm.away_team,vc.sport_id,vm.competition_id,vc.competition_name FROM virtual_event_odd ve INNER JOIN virtual_match vm ON ve.parent_virtual_id=vm.parent_virtual_id INNER JOIN virtual_competition vc ON vc.v_competition_id=vm.competition_id INNER JOIN virtual_odd_type o ON o.sub_type_id=ve.sub_type_id WHERE ve.special_bet_value=:spbv AND vm.parent_virtual_id=:paroid AND ve.sub_type_id=:sbtid AND ve.odd_key=:oddky"""
             params = {'paroid':parent_virtual_id,'spbv':special_bet_value, 'sbtid':odd_type, 'oddky':pick_key}

             event_odd = self.db_vt.engine.execute(sql_text(sqlQ), params).fetchone()
             self.logger.info("SQL invalid virtual bet slip :: {0}::{1}::{2}".format(params, event_odd, sqlQ))

             if not event_odd:
                return True, "Virtual event//{0}/ was not found. Kindly try again later or contact customer care.".format(parent_virtual_id)

             start_time,odd_key,parent_virtual_id,odd_value,sub_type_id,special_bet_value,home_team,away_team,sport_id,competition_id,competition_name = event_odd

             if start_time < datetime.now():
                return True, "Virtual event//{0}/{1}/{2}/ has already expired. T&C apply.".format(home_team, away_team, parent_virtual_id)
             
             self.bet_string_message += "#{0}#{1}".format(parent_virtual_id, odd_key)
             self.bet_odd_total = self.bet_odd_total * float(odd_value)
 
             response.append({"start_time":start_time, "parent_virtual_id":parent_virtual_id, "special_bet_value":special_bet_value,"odd_key": odd_key, "pick": odd_key, "live_bet": bet_type, "odd_value": odd_value,"home_team": home_team, "away_team": away_team, "sub_type_id": odd_type, "sport_id":sport_id, "competition_id": competition_id, "competition_name": competition_name})
             self.logger.info("odds validation response.... {0}".format(response))

         return False, response


    def bet_amounts_validation(self, bet_amount, profile_id, betslips):
        self.logger.info("virtual bet validate bet slip :: {0}::{1}::{2}".format(bet_amount, profile_id, len(betslips)))
        max_bet_multi = float(self.scorepesa_configs['max_bet_amount_multi'])
        max_bet_single = float(self.scorepesa_configs['max_bet_amount_single'])
        if float(bet_amount) > max_bet_multi and len(betslips) > 1:
            return "Your stake amount exceeds the maximum allowed for multi-bet. You can place bets of \
upto Kshs %0.2f amount" % float(self.scorepesa_configs['max_bet_amount_multi']), True

        if float(bet_amount) > max_bet_single and len(betslips) == 1:
            return "Your stake amount exceeds the maximum allowed for single bet. You can place bets of \
upto Kshs %0.2f amount" % float(self.scorepesa_configs['max_bet_amount_single']), True
        return False, False

    '''
      self.balance and self.bonus used 2 store the profile balances(bonus and balance) during request
    '''
    def bet_stake_spread_on_bals(self, profile_id, amount, no_bonus_use=False):
        amount = float(amount)
        bet_on_balance, bet_on_bonus = 0.0, 0.0
        #config to toggle bonus use on and off
        if int(self.scorepesa_configs['bet_on_bonus']) == 0:
            if self.balance < amount:
                return 0, -1
            return 0, amount

        if not self.balance:
            if self.bonus is None or self.bonus < 0:
                self.logger.error("Profile got no real balance from self.balance. No bonus as well, return proceed with virtuals trx...")
                return 0, -1
        if no_bonus_use:
            #Force bet on real account balance may be needed in future
            if self.balance < amount:
                return 0, -1
            return 0, amount

        bonus_amount = self.bonus
        if bonus_amount <= 0.0:
            self.logger.info("No bonus return to use real balance and proceed with virtuals transaction :: %0.2f " % bonus_amount)
            return 0, amount

        bet_on_bonus = amount
        bet_on_profile_balance = 0
        if bet_on_bonus > bonus_amount:
            bet_on_bonus = bonus_amount
            bet_on_profile_balance = float(amount) - float(bet_on_bonus)

        self.bonus_balance_amount = self.bonus
        return  bet_on_bonus, bet_on_profile_balance


    def bet_transaction(self, profile_id, bet_string,stake_amount, bet_on_bonus, bet_on_balance, totalOdd, possibleWin, bet_slips, app_name='WEB_API'):
         self.logger.info("in create bet transaction....")
         vt_connection = self.db_vt.engine.connect()
         trans1 = vt_connection.begin()
         connection = self.db.engine.connect()
         trans2 = connection.begin()
         self.logger.info("created transactions to use.....")
         try:
             bet_dict = {
                "profile_id": profile_id,
                "bet_message": bet_string,
                "bet_amount": stake_amount,
                "total_odd": Decimal(totalOdd),
                "possible_win": possibleWin,
                "status": 1,
                "reference": 'VIRTUALS_BET',
                "win": 0,
                "created_by": app_name,
                "created": datetime.now(),
                "modified": datetime.now()
             }
             self.logger.info("got bet dict ...... {0}".format(bet_dict))

             bet = vt_connection.execute(Bet.__table__.insert(), bet_dict)
             bet_id = bet.inserted_primary_key[0]

             self.logger.info("created scorepesa virtual bet .....betId {0}".format(bet_id))
             slip_data = []
             for slip in bet_slips:
                 bet_slip_dict = {
                    "parent_match_id": slip.get("parent_virtual_id"),
                    "bet_id": bet_id,
                    "bet_pick": slip.get("pick"),
                    "special_bet_value": slip.get("special_bet_value") if slip.get("special_bet_value") else '',
                    "total_games": len(bet_slips),
                    "odd_value": slip.get("odd_value"),
                    "win": 0,
                    "live_bet": 0,#slip.get("bet_type"),
                    "created": datetime.now(),
                    "status": 1,
                    "sub_type_id": slip.get("sub_type_id")
                 }
                 slip_data.append(bet_slip_dict)

             vt_connection.execute(BetSlip.__table__.insert(), slip_data)

             self.logger.info("Created scorepesa virtual betslip .....")

             #roamtech_id = self.get_roamtech_virtual_acc('ROAMTECH_VIRTUAL')
             trx_debit_dict = {
                "profile_id": profile_id,
                "account": "%s_%s" % (profile_id, 'VIRTUAL'),
                "iscredit": 0,
                "reference": "{0}_{1}".format(bet_id, 'ScorepesaVirtualBet'),
                "amount": stake_amount,
                "created_by": app_name,
                "created": datetime.now(),
                "modified": datetime.now()
             }
             trxd = connection.execute(Transaction.__table__.insert(), trx_debit_dict)
             trxd_id = trxd.inserted_primary_key[0]
             
             #deduct stake from profile account
             deduct_res=self.deduct_stake_amount(bet_on_balance, bet_on_bonus, profile_id, connection)

             self.logger.info("Created scorepesa virtual transaction .....TrxId:: {0}:::deduct result::{1}".format(trxd_id, deduct_res))
             #create transaction mapping 
             trx_map_dict = {
                "scorepesa_transaction_id":trxd_id,
                "iscredit": 0,
                "created": datetime.now(),
                "modified": datetime.now(),
                "created_by": "ScorepesaVirtualBet",
             }
             trxmap = vt_connection.execute(TransactionMap.__table__.insert(), trx_map_dict)
             trxmap_id = trxmap.inserted_primary_key[0]
             self.logger.info("Created scorepesa virtual transaction map..... {0}".format(trxmap_id))
             #record virtual bet bonus if enabled
             self.record_bonus_bet(bet_id, stake_amount, possibleWin, profile_id, bet_on_bonus, vt_connection, app_name)

             trans2.commit()
             trans1.commit()
             return bet_id
         except Exception as e:
             trans2.rollback()
             trans1.rollback()
             self.logger.error("Transaction creating virtuals bet, rolled back :: {0} ...".format(e))
             return False

    def get_profile_bonus_id(profile_id):
        profile_bonus_id = None
        r3 = connection.execute(sql_text("""
            select profile_bonus_id from profile_bonus \
            where profile_id=:pfid and
            status = :st order by profile_bonus_id desc limit 1"""),
            {'pfid': profile_id, 'st': 'CLAIMED'}).fetchone()
        if r3:
           profile_bonus_id = r3[0]
        return profile_bonus_id

    def record_bonus_bet(self, bet_id, stake_amount, possibleWin, profile_id, bet_on_bonus, vt_connection, app_name):
         if float(bet_on_bonus) > 0.0:
             profile_bonus_id = self.get_profile_bonus_id(profile_id)
             if profile_bonus_id is None:
                 profile_bonus_id=0
             #record virtual bonus bets
             bonus_bet_dict = {
                   "bet_id":bet_id,
                   "bet_amount":float(bet_on_bonus),
                   "possible_win": possibleWin,
                   "profile_bonus_id":profile_bonus_id,
                   "won": 0,
                   "ratio":float(bet_on_bonus)/float(stake_amount),
                   "created_by":app_name,
                   "created":datetime.now(),
                   "modified":datetime.now()
             }

             vt_connection.execute(BonusBet.__table__.insert(), bonus_bet_dict)
             if profile_bonus_id != 0:
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
                 bonustrx = vt_connection.execute(BonusTrx.__table__.insert(), bonus_trx_dict)
                 bonustrx_id = bonustrx.inserted_primary_key[0]
                 self.logger.info("Created scorepesa virtual bonus transaction..... {0}".format(bonustrx_id))
                 #flag profile_bonus as used if no more bonus to use left in account
                 if (float(self.bonus) - float(bet_on_bonus)) < float(1):
                     connection.execute(sql_text("""update profile_bonus set status=:new_status,bet_on_status=:sta2
                     where profile_id = :pfid and status = :oldst"""),{'new_status':'USED', 'sta2':2, 'pfid':profile_id, "oldst":'CLAIMED'})
             else:
                 self.logger.info("ProfileId :: {0} ::: no profile bonus id matched but bonus was not used so created bonusbet only skipped bonus trx..".format(profile_id))
         else:
             self.logger.info("bet on bonus is insufficient....{0}".format(bet_on_bonus))
             return True

    def deduct_stake_amount(self, bet_on_balance, bet_on_bonus, profile_id, connection):
         try:
            bal_Q = """update profile_balance set balance = (balance-%0.2f), bonus_balance=(bonus_balance-%0.2f) where profile_id=:profile_id limit 1""" % (bet_on_balance, bet_on_bonus)
            connection.execute(sql_text(bal_Q), {'profile_id': profile_id})
            return True
         except Exception, e:
            self.logger.error("Transaction deductions virtuals, rolled back :: {0} ...".format(e))
            raise

    def validate_singlebet_daily_limit(self, profile_id, parent_match_id, amount):
        todays_bets_on_match = self.db_vt.engine.execute(
            sql_text("""select sum(bet_amount) from bet_slip join bet using(bet_id)
                where bet_slip.parent_match_id = :p and bet.profile_id=:pf and bet_slip.total_games=1 group by bet_slip.bet_id"""),
            {'p': parent_match_id, 'pf': profile_id}).fetchone()
        if todays_bets_on_match and todays_bets_on_match[0]:
            self.logger.info("AMOUNT BET so far invalid_single_bet_message : %r" % todays_bets_on_match[0])

            self.logger.info("CULCULATING STAKE AMOUNT %r, %r (%r) > %r" % (todays_bets_on_match[0], float(amount),
                (float(todays_bets_on_match[0])+float(amount)),float(self.scorepesa_configs['max_bet_amount_single']) ))
            if (float(todays_bets_on_match[0])+float(amount)) > float(self.scorepesa_configs['max_bet_amount_single']):
                return "Your total stake amount for this match exceeds the maximum allowed. You can place bets of \
upto Kshs %0.2f amount per game" % float(self.scorepesa_configs['max_bet_amount_single'])
        return None


    def get_account_balance(self, profile_id):
        bal = False
        if profile_id:
            bal = self.db.engine.execute(
                sql_text("select balance, bonus_balance from profile_balance where profile_id = :value"),
                {'value':profile_id}).fetchone()
        if bal:
            available_bonus=float(bal[1])
            self.balance, self.bonus = float(bal[0]), available_bonus
        else:
            self.balance, self.bonus = 0, 0
        self.logger.info("returned balance and bonus ::{0}::{1}".format(self.balance, self.bonus))
        return self.balance, self.bonus

    def create_profile_mapping(self, profile_id):
        profilemap_id=None
        profile_map_dict = {
           "scorepesa_profile_id":profile_id,
           "status": 1,
           "created":datetime.now(),
           "modified": datetime.now(),
           "created_by": "ScorepesaVirtualBet",
        }
        #fetch if exists
        bprofile_res = self.db_vt.engine.execute(sql_text("select scorepesa_profile_id from profile_map where scorepesa_profile_id=:value"),
                {'value':profile_id}).fetchone()
        if bprofile_res:
           profilemap_id=bprofile_res[0]
        else:
           #else create mapping
           profilemap = self.db_vt.engine.execute(ProfileMap.__table__.insert(), profile_map_dict)
           if profilemap:
              profilemap_id = profilemap.inserted_primary_key[0]
        self.logger.info("returned profile map id...... {0}".format(profilemap_id)) 
        return profilemap_id

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
            fQ = "select msisdn from account_freeze where msisdn=:msisdn and\
             status=:status limit 1"
            result = self.db.engine.execute(sql_text(fQ),
                 {'msisdn': msisdn, 'status': 1}).fetchone()
            self.logger.info("virtuals bet account freeze result %r :: %r :: %r" % (result, message, profile_id))
            if result and result[0]:
                return True
            return False
        except Exception, e:
            self.logger.error("Exception account freeze :: %r " % e)
            return False

    def get_roamtech_virtual_acc(self, acc):
        if acc == 'ROAMTECH_MPESA':
            if 'mpesa_roamtech_profile_id' in self.scorepesa_configs:
                return self.scorepesa_configs['mpesa_roamtech_profile_id']
            return 5

        if 'virtual_roamtech_profile_id' in self.scorepesa_configs:
            return self.scorepesa_configs['virtual_roamtech_profile_id']
        return 6
