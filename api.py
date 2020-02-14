import json
from flask import request, make_response, render_template, Response
from flask_restful import Resource, reqparse
from SendSmsPublisher import SendSmsPublisher
from Scorepesa import Scorepesa
from flask import current_app
from decimal import Decimal
import requests
from utils import LocalConfigParser, Helper
from jose.exceptions import JWTError
from jose import jwt
import urllib
from datetime import datetime
from ScorepesaPoint import ScorepesaPoint
from sqlalchemy.exc import IntegrityError
from ReferralBonus import ReferralBonus
from functools import wraps
from RedisCore import RedisCore
from ScorepesaUssd import ScorepesaUssd
from ScorepesaSpecials import ScorepesaSpecials
import xmltodict
import math


def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.
    """
    current_app.logger.info("scorepesa api auth received creds....{0}::{1}"\
        .format(username, password))
    scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
    passwd=scorepesa_cfgs['scorepesa_api_password']
    user=scorepesa_cfgs['scorepesa_api_username']
    #current_app.logger.info("config creds ...{0}::{1}".format(user, passwd))

    return str(username) == str(user) and str(password) == str(passwd)

def authenticate():
    """Sends a 401 response that enables basic auth"""

    res = json.dumps({"msg": 'Access denied.'})
    return Response(res, 401, 
        {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        current_app.logger.info(" scorepesa app auth request got ...{0}"\
            .format(auth))
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

def sms_gateway_send_sms(message):
    current_app.logger.info("Calling sms gateway ::%r" % (message))
    sdp_configs = LocalConfigParser.parse_configs("SDP")
    bulk_configs = LocalConfigParser.parse_configs("BULK")
    
    msisdn = message.get('msisdn') or message.get('phone')

    if message.get("message_type") == 'mo':
        url = sdp_configs["url"]
        current_app.logger.info("SDP URL %s" % (url,))
        sender = sdp_configs['short_code']
        sms_payload = {
           "msisdn":msisdn,
           "message":message.get('message') or message.get('text'),
           "short_code":message.get('sender') or sender,
           "link_id":message.get('linkid'),
           "reference":message.get('outbox_id'),
           "sdp_id":message.get('service_id')
        }

    else:
        url = bulk_configs["url"]
        current_app.logger.info("BULK URL %s" % (url,))
        sender = bulk_configs['access_code']
        sms_payload = {
           "msisdn":msisdn,
           "message":message.get('message') or message.get('text'),
           "short_code":message.get('access_code') or sender,
           "access_code":sender,
           "reference":message.get('outbox_id'),
           "sdp_id":message.get('service_id') or bulk_configs['service_id']
        }


    current_app.logger.info("API SMS Send params %r ==> %r" % (url, sms_payload))
    output = None

    try:
        output = requests.get(url, params=sms_payload, timeout=30)
        current_app.logger.info(
            "Sending payload headers ==> %r, payload => %r" % (output.headers, sms_payload))
        current_app.logger.info(
            "Found result from sdp call from send sms GEN: (%s, %s) " % (url, output.text, ))
        
        sms_payload['status'] = "Message sent OK"
        return sms_payload
    except requests.exceptions.RequestException as e:
        current_app.logger.error(
        "Exception attempting sms gateway : %r :: %r " % (sms_payload, e))
        output = None

    return output


class JackpotBet(Resource):
    def post(self):
        message = request.get_json()
        current_app.logger.info("Received JackpotBet Request %r" % message)

        if message:
            try:
                status, response = self.jackpot_bet(message)
            except Exception, e:
                current_app.logger.error("Error on JackpotBet : %r " % e)
                status = 500
                response = 'We are unable to place your bet right now,'\
                    ' Please try again later.'
        else:
            status, response = 421, 'Bad request'
        current_app.logger.info("api response message:: %r" % response)
        return make_response(response, status)

    def jackpot_bet(self, message):
        scorepesa = Scorepesa(current_app.logger)
        #bet_picks = message.get('bet_picks') or message.get("message")
        slips = message.get('slip')
        current_app.logger.info("Beslps %r " % slips)
        bet_picks  = '#'.join([a['pick_key'] for a in slips]) 
        app_name = message.get('app_name') \
            if message.get('app_name') else 'API_WEB'
        response = scorepesa.jackpot_bet(message, [bet_picks], app_name)
        if scorepesa.jp_bet_status == 201:
            payload = {
                'reference_id':scorepesa.bet_id,
                'msisdn':message.get('msisdn'),
                'netwok':message.get('network', 'SAFARICOM'),
                'profile_id':message.get('profile_id'),
                'message':response}
            send = sms_gateway_send_sms(payload)

        return scorepesa.jp_bet_status, response


class TransactionMgt(Resource):
    @requires_auth
    def post(self):
        try:
           message = request.get_json()
        except Exception, e:
           current_app.logger.error(
            "Error on TransactionMgt received request ::{0}".format(e))
           status, response = 400, {'error': 'Bad request'}
           return response, status #make_response(response, status)
           
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        current_app.logger.info(
            "TransactionMgt JWT Bet api args :: {0} :configs: {1}"\
            .format(message, scorepesa_cfgs['encrption_key']))   

        if message:
            try:
                msg_data = jwt.decode(
                    message['token'], scorepesa_cfgs['encrption_key'], 
                    algorithms=['HS256'])
            except Exception, e:
                current_app.logger.error(
                    "Error on TransactionMgt decode token :: {0}".format(e))
                status, response = 400, {'error': 'Bad request'}
                return response, status #make_response(response, status)

            try:
                helper = Helper(current_app.logger)
                whitelisted_sources = scorepesa_cfgs['transaction_mgt_source_apps']              
  
                profile_id = msg_data.get("userId")
                reference_id = msg_data.get("referenceId")
                transaction_type = msg_data.get("transactionType")
                source = msg_data.get("sourceApp")
                amount = msg_data.get("amount")
                bonus = msg_data.get("bonus")

                if source.lower() in whitelisted_sources.split(','):
                    res = helper.helper_credit_debit_transaction(
                        profile_id, reference_id, transaction_type, 
                        source, amount, bonus)
                    if res:
                        status, response = 201, {'success': True}
                else:
                    current_app.logger.error(
                        "TransactionMgt wrong source data got:: {0}"\
                        .format(msg_data))
                    status = 421
                    response = {'error': 'Failed. Please try again later.'}
            except Exception, e:
                current_app.logger.error(
                    "Error on TransactionMgt processing:: {0}".format(e))
                status = 421
                response = {'error': 'Failed. Please try again later.'}
        else:
            status, response = 400, {'error': 'Bad request'}
        current_app.logger.info(
            "TransactionMgt api response message::{0}::status::{1}"\
            .format(response, status))
        #return make_response(response, status)
        return response, status


class ScorepesaPointJackpot(Resource):
    def post(self):
        message = request.get_json()
        current_app.logger.info(
            "Received scorepesa point Jackpot Request %r" % message)
        if message:
            try:
               
                status, response = self.jackpot_bet(message)
            except Exception, e:
                current_app.logger.error("Error on scorepesa point Jackpot : %r " % e)
                status = 500
                response = 'We are unable to place your bet right now, '\
                    'Please try again later.'
        else:
            status, response = 421, 'Bad request'
        current_app.logger.info(
            "free jackpot api response message:: %r" % response)
        return make_response(response, status)

    def jackpot_bet(self, message):
        scorepesa = Scorepesa(current_app.logger)
        #bet_picks = message.get('bet_picks') or message.get("message")
        current_app.logger.info("Calling self.jackpot bet")
        slips = message.get('slip')
        bet_picks  = '#'.join([a['pick_key'] for a in slips])
        app_name = message.get('app_name') \
            if message.get('app_name') else 'API_WEB'
        jp_event_id = message.get('jackpot_id')
        if jp_event_id is None:
            jp_event_id = message.get('jackpot_event_id')
        profile_id = message.get('profile_id')
        current_app.logger.info("jackpot points... {0}::{1}::{2}::{3}"\
            .format(jp_event_id,app_name,bet_picks,profile_id))
        if jp_event_id is None:
            return 421, "Sorry we could not create your bet."
        #if isinstance(message.get('profile_id'), basestring):
        #     return 421, "Sorry we could not create your bet."
        if message.get('profile_id') is None:
             return 421, "Sorry we could not create your bet."
        #return 421, "Sorry we could not create your bet."
        response = scorepesa.jackpot_bet(message, [bet_picks], app_name)
        return scorepesa.jp_bet_status, response


class ScorepesaReferral(Resource):
    def post(self):
        message = request.get_json()
        current_app.logger.info("Received referral retry Request %r" % message)
        if message:
            try:
                status, response = self.retry_award(message)
            except Exception, e:
                current_app.logger.error("Error on scorepesa referral : %r " % e)
                status = 500
                response = 'Failed.'
        else:
            status, response = 421, 'Bad request'
        current_app.logger.info(
            "referral retry award api response message:: %r" % response)
        return make_response(response, status)

    def retry_award(self, message):
        profile_id = message.get('profile_id')
        scorepesa_ref = ReferralBonus(current_app.logger, profile_id)
        amount = message.get('stake')
        total_odd = message.get('total_odd')
        response = scorepesa_ref.apply_referal_bonus_on_bet(
            profile_id, amount, total_odd)
        return 201, "Done."


class BonusPromo(Resource):
    def post(self):
        message = request.get_json()
        current_app.logger.info("Received bonus award Request %r" % message)
        if message:
            try:
                status, response = self.bonus_award(message)
            except Exception, e:
                current_app.logger.error("Error on scorepesa bonus award : %r " % e)
                status = 500
                response = 'Failed.'
        else:
            status, response = 421, 'Bad request'
        current_app.logger.info(
            "bonus award api response message:: %r" % response)
        return make_response(response, status)

    def bonus_award(self, message):
        profile_id = message.get('profile_id')
        scorepesa_ref = ReferralBonus(current_app.logger, profile_id)
        amount = message.get('amount')
        msisdn = message.get('msisdn')
        bonus_type = message.get('bonus_type') or None
        response = scorepesa_ref.award_bonus_on_request(
            profile_id, msisdn, amount, bonus_type)
        return 201, "Awarded."


class JpBonusAward(Resource):
    def post(self):
        message = request.get_json()
        current_app.logger.info("Received jp bonus award Request %r" % message)
        if message:
            try:
                status, response = self.jpbonus_award(message)
            except Exception, e:
                current_app.logger.error("Error on scorepesa jp bonus award : %r " % e)
                status = 500
                response = 'Failed.'
        else:
            status, response = 421, 'Bad request'
        current_app.logger.info(
            "jp bonus award api response message:: %r" % response)
        return make_response(response, status)

    def jpbonus_award(self, message):
        profile_id = message.get('profile_id')
        jp_event_id = message.get('jp_event_id')
        bet_id = message.get('bet_id')
        total_correct_matches = message.get('total_correct_matches')
        scorepesa_ref = ReferralBonus(current_app.logger, profile_id)
        amount = message.get('jp_bonus_amount')
        msisdn = message.get('msisdn')
        return 201, "Awarded."


class ScorepesaPointAward(Resource):
    def post(self):
        message = request.get_json()
        current_app.logger.info(
            "Received scorepesapoint award Request....%r" % message) 
        if message:
            try:
                profile_id = message.get("profile_id")
                if not profile_id or profile_id is None:
                   return make_response("No profile Id.", 201)
                bp = ScorepesaPoint(current_app.logger, profile_id)
                transaction_id = message.get("transaction_id") or None
                points=message.get("points")
                reqtype=message.get("reqtype")
                profile_id = message.get("profile_id")
                bp.profile_id = profile_id
                bet_id = message.get("bet_id") or None
                if message.get("manual") == "true":
                    transaction_id = \
                    bp.transaction_for_bonus_scorepesa_point_award(profile_id, reqtype)
                    msisdn = message.get("msisdn")
                    bet_cancelled=False
                    message = 'Congratulations, you have been awarded 450 Scorepesa'\
                        ' points bonus. Enjoy a free Jackpot bet with only 500'\
                        ' Scorepesa points, it is first of a kind Jackpot from Scorepesa.'
                    message_type = 'BULK'
                    short_code = 101010
                    correlator = ''
                    link_id = ''
                    payload = urllib.urlencode({"message": message, 
                        "msisdn":msisdn, "message_type":message_type, 
                        "short_code":short_code, "correlator":correlator, 
                        "link_id":link_id})
                   #bp.send_notification(payload)
                else:
                   if bet_id is None:
                      return make_response("No Bet Id.", 201)
                   bet_cancelled = bp.check_bet_cancelled(bet_id)               
                current_app.logger.info(
                    "scorepesapoint params.. trx::{0}...points::{1}..reqtype::{2}"
                    "::profile::{3}::betcancel::{4}::bet_id::{5}"\
                    .format(transaction_id, points, reqtype,
                        profile_id, bet_cancelled, bet_id))
                status, response = 201, "Bet cancelled."
                if not bet_cancelled:
                    bp.process_scorepesa_points(transaction_id, 
                        points=points, reqtype=reqtype)
                    status, response = 201, "Success."
            except IntegrityError, ex:
                current_app.logger.error(
                    "Integrity error on scorepesapoint::: %r " % ex)
                status=421
                response = 'Dup! transaction.'
            except Exception, e:
                current_app.logger.error(
                    "Error on scorepesapoint award::: %r " % e)
                status=500
                response = 'Failed!.'
        else:
            status, response = 421, 'Bad request'
        current_app.logger.info(
            "API response message...{0}::status::{1}..."\
            .format(response, status))
        return make_response(response, status)


class Bet(Resource):
   
    def send_sms(self, payload):
        bikio = Scorepesa(current_app.logger)
        if not payload.get('msisdn', None):
            payload['msisdn'] = bikio.get_profile_msisdn(payload.get('profile_id'))
        bikio.outbox_message(payload, payload.get('message'))
        payload['outbox_id'] = bikio.outbox_id
        #url = LocalConfigParser.parse_configs("BULK")["url"]
        send = sms_gateway_send_sms(payload)

    def get(self):
        self.bet_id = 0
        current_app.logger.info("in bet api placing bet now ..........." )
        args = request.args#parser.parse_args(strict=True)
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        current_app.logger.info(
            "JWT Bet api args :: {0} :configs: {1}"\
            .format(args, scorepesa_cfgs['encrption_key']))
        response = u'Invalid request.'
        status = 421
        try:
            data = jwt.decode(args['token'], scorepesa_cfgs['encrption_key'], 
                algorithms=['HS256'])
            current_app.logger.info("Decorded request data => %r" % data)
            scorepesa = Scorepesa(current_app.logger)
            message_json = data #.get('user')
            profile_id = message_json.get('profile_id')
            if message_json.get("sdial") == 1:
                message_json['app_name'] = "SPEEDDIAL"
            current_app.logger.info(
                "bet data extracted ::got speeddial={3} :::{0} ::: {1}"
                "...profileid_{2}".format(message_json, data, 
                profile_id, message_json.get("sdial")))
            if message_json:
                res = "Message failed, Invalid bet. "\
                    "Please select matches provided and try again."
                status, response = self.api_bet(message_json)
            #log speeddial 
            if message_json.get("sdial") == 1:
                message_json['source'] = "BET API"
                helper=Helper(current_app.logger)
                helper.create_speed_dial_history(message_json)
            if message_json.get('profile_id') is not None and \
                    message_json.get('app_name')=='android':
                #push notification
                helper = Helper(current_app.logger)
                data = {"fcm_token": None, "msg": response, 
                    "profile_id": message_json.get('profile_id')}
                helper.push_notification(data)
       
            #self.send_sms({'msisdn':message_json.get('msisdn'),
            #    'reference_id':self.bet_id,
            #    'netwok':message_json.get('network', 'VODACOM'),
            #    'profile_id':message_json.get('profile_id'),
            #    'message':response}
            #)
        except JWTError as e: 
            current_app.logger.error("Bet token exception %r " % e)
        current_app.logger.info(
            "jwt bet api response message:: %r :: %r" % (response, status))
        return response, status #make_response(response, status)

    def post(self):
        self.bet_id = 0
        try:
           message = request.get_json()
        except Exception as ex:
           current_app.logger.error("bet invalid request...{0}".format(ex))
           message=None

        if message is None:
            return 'Bad request', 421

        current_app.logger.info(
            "Received Bet Request message {0} :: profileID ::{1} "\
            .format(message, message.get('profile_id')))
        if message:
            status, response = self.api_bet(message)
        else:
            status, response = 421, 'Bad request'
        current_app.logger.info("api response message:: %r" % response)

        if message.get('profile_id') is not None and \
                message.get('app_name')=='android':
            #push notification
            helper = Helper(current_app.logger)
            data = {"fcm_token": None, "msg": response, 
                "profile_id": message.get('profile_id')}
            helper.push_notification(data)
        if status == 201: 
            self.send_sms({'msisdn':message.get('msisdn'),
                'reference_id':self.bet_id,
                'netwok':message.get('network', 'SAFARICOM'),
                'profile_id':message.get('profile_id'),
                'message':response}
            )
       
        return response, status

    def api_bet(self, message):
        is_paid = False
        scorepesa = Scorepesa(current_app.logger)
        #options looks like [GAME,190, 1, 250]
        #game id#pic#gameid#pick ....#amount
        profile_id, new = scorepesa.create_profile(message, 1)
        if scorepesa.betting_disabled():
           return 421, "This service is currently unavailable. Kindly try again after a few minutes" 
        #profile_id = message.get('profile_id')
        if scorepesa.check_account_freeze(None, profile_id):
            return 421, scorepesa.scorepesa_configs['account_bet_block_msg']
        slips = message.get('slip')
        scorepesa.betslipLen = len(slips)
        possible_win = message.get('possible_win')
        app_name = message.get('app_name') \
            if message.get('app_name') else 'API_WEB'
        current_app.logger.info("received bet app name::{0} :: {1}"\
            .format(app_name, message))
        #live_betting = 1 if message.get('live_bet') == 1 else 0
        amount = message.get('stake_amount')
        amount = abs(float(amount))
        #bet_total_odds = message.get('bet_total_odds')
        if len(slips) < 1:
            return 421, "Your betslip selection is not valid. Kindly review your selection and try again"

        balance, bonus_balance = scorepesa.get_account_balance(message)
        #bonus_balance = scorepesa.get_bonus_balance(message)
        if float(amount) < float(scorepesa.scorepesa_configs["min_amount"]):
            return 421,\
                "Your current account balance is insufficient to place this bet"\
                "Minimum Bet amount is KSH %0.2f, Your balance is KSH %0.2f. Kindly deposit "\
                "via PAYBILL 290080 or visit www.scorepesa.co.ke" % (
			float(scorepesa.scorepesa_configs["min_amount"]), balance)

        if amount < 1:
            return 421, "Your bet amount is not valid. "\
                "Place choose {0} or more to place bet. SCOREPESA T&C apply."\
                .format(scorepesa.scorepesa_configs["min_amount"])

        current_app.logger.info(
            "check stake amount against available:: %r :: %r :: %r ::" 
            % (amount, balance, bonus_balance))

        if float(amount) > float(balance + bonus_balance):
            is_paid = False 
            return 421,\
                "Cannot place bet as current balance %0.2f,"\
                 "bonus %0.2f is less than minimum stake amount.%s"  % \
                 (float(balance), bonus_balance, scorepesa.free_bonus_advise)

        if len(slips) > int(scorepesa.scorepesa_configs['max_match_bet']):
            return 421, "Your slip exceeds the maximum number of games "\
                "allowed in multibet. Kindly reduce your selection to "\
                "a maximum of %s games" % float(scorepesa.scorepesa_configs['max_match_bet'])

        bet_slips = []
        selctions = []
        #count = 0
        bet_total_odd = 1
        gameid_picklist = []
        betmessage_string = None
        game_id_check_list = []
        outrights_check_list = []
        '''
         Live bet=1
         Outrights bet=3
         Pre-match bet=0 as default
        '''
        try:
            is_peer = 0
            peer_msisdn = ''
            peer_bet_id = None
            current_app.logger.info("RAW API PICK %r" % (slips))
            for bet_slip in slips:
                user_odd_value = bet_slip.get('odd_value')
                is_peer = bet_slip.get('is_peer')
                peer_bet_id = bet_slip.get('peer_bet_id')
                peer_msisdn = bet_slip.get('peer_msisdn')
                bet_type = 0
                scorepesa.outright = \
                    3 if bet_slip.get('parent_outright_id') is not None else 0
                if scorepesa.outright == 3:
                    bet_type=3
                betrader_competitor_id = \
                    bet_slip.get('betrader_competitor_id') or ""
                parent_outright_id = bet_slip.get('parent_outright_id') or ""
                if bet_slip.get('bet_type') is not None:
                    try:
                       bet_type = int(bet_slip.get('bet_type'))
                    except Exception as e:
                        current_app.logger.info(
                            "Exception on extract bet_type... {0}".format(e))
                        return 421, \
                            "We are unable to complete your bet at this time. Kindly try again in a few minutes"

                current_app.logger.info(
                    "PICKED VALUES profile%s :: %s :: %r " 
                    % (profile_id, bet_type, bet_slip))
                if bet_type == 1:
                    scorepesa.livebetting = True
                    game_id = scorepesa.get_game_id(bet_slip.get('parent_match_id'))
                    if game_id is None:
                       game_id = bet_slip.get('parent_match_id')
                    current_app.logger.info(
                        "live bet true...bet_type:: {0} ::game_id:: {1}"
                        " ::betslip:: {2}".format(bet_type, game_id, bet_slip))
                elif bet_type == 3: 
                    scorepesa.outright = bet_type
                    scorepesa.outright_bet = True
                    game_id, event_name, ot_competition_id, \
                    ot_competition_name = scorepesa.get_outright_game_id(
                        bet_slip.get('parent_outright_id'))
                    if event_name is None:
                        return 421, "Outright event#{0} not found. "\
                            "Please try again later."\
                            .format(bet_slip.get('parent_outright_id'))
                    game_id = "OT;{0}".format(bet_slip.get('parent_outright_id')) 
                else:
                    game_id = scorepesa.get_game_id(bet_slip.get('parent_match_id'))
                current_app.logger.info("Collected GAME ID MOVING Forward ..")
                parent_match_id = bet_slip.get('parent_match_id')
                outcome_exist = scorepesa.check_outcome_exists(
                    bet_slip.get('parent_match_id'))

                current_app.logger.info("confirm outcom exists ..");
                if outcome_exist and bet_type == 0:
                    return 421, "Betting time for "\
                     " {0} vs {1} has elapsed, www.scorepesa.co.ke."\
                     " T&C apply.".format(scorepesa.home_team,
                     scorepesa.away_team)

                current_app.logger.info("confirm outcom exists Done ..");
                pick = bet_slip.get('pick_key') if bet_slip.get('pick_key') \
                    is not None else betrader_competitor_id
                try:
                    special_bet_value = str(bet_slip.get('special_bet_value'))\
                        if bet_slip.get('special_bet_value') else None
                except:
                    special_bet_value = None
                gameid_picklist += "#" + str(game_id) + "#" + pick
                #get from sub type
                sub_type = bet_slip.get('sub_type')  or bet_slip.get('sub_type_id')
                sub_type_id = scorepesa.get_sub_type_id(sub_type) 

                current_app.logger.info("Parsing params : %s, %s, %s, %s, %r"
                     % (game_id, pick, amount, sub_type_id, scorepesa.outright))
                if bet_type == 1:
                    invalid_slip, response = \
                        scorepesa.invalid_live_betslip_message(parent_match_id, 
                            game_id, pick, amount, sub_type_id,
                            special_bet_value)
                elif bet_type == 3:
                    scorepesa.outright == 3
                    current_app.logger.info("Outrights bet slip.....")
                    invalid_slip, response = \
                        scorepesa.invalid_betslip_message_outright(
                            betrader_competitor_id, game_id, 
                            parent_outright_id,
                            amount, event_name, sub_type_id, special_bet_value)
                    current_app.logger.info(
                        "Outright response {0}::{1}"\
                        .format(invalid_slip, response))
                else:
                    current_app.logger.info("Reading values from post: game_id %s, pick %s, "\
                        "amount %0.2f sub_type_id %s, sbv %s, odds %s" \
                        % (game_id, pick, amount, sub_type_id, special_bet_value, user_odd_value))    
                    user_odd_value = user_odd_value if is_peer != '1' else  None
                    invalid_slip, response = scorepesa.invalid_betslip_message(
                        game_id, pick, amount, sub_type_id, special_bet_value, user_odd_value)
                
                current_app.logger.info(
                    "getting invalid slip %s, %r ", response, invalid_slip)
                if invalid_slip:
                    return 421, response
                event_odd = response
                if game_id in game_id_check_list:
                    if scorepesa.outright == 3:
                        return 421, "Duplicate outright selection, to proceed"\
                            " remove event {0}. Not all selections can be "\
                            "combined.".format(event_name)
                    return 421,"Duplicate GAMEID in request. Please select "\
                        "one pick for each game."
                if scorepesa.outright == 3:
                   if ot_competition_id in outrights_check_list:
                        return 421, "Ticket contains multiple selections from"\
                            "outright markets belonging to the same "\
                            "tournament {0}.".format(ot_competition_name)

                if len(slips) == 1:
                    invalid_single_bet_message = \
                        scorepesa.invalid_single_bet_message(
                            profile_id, parent_match_id, amount)

                    current_app.logger.info(
                        "after checking for singlebets limits.....{0}"\
                        .format(invalid_single_bet_message))

                    if invalid_single_bet_message:
                        return 421, invalid_single_bet_message

                special_bet_value = event_odd.get("special_bet_value")
                #bet_slips.append({"parent_match_id":match.parent_match_id
                if scorepesa.outright == 3:
                    bet_type=0

                bet_slips.append({"parent_match_id":
                     event_odd.get("parent_match_id"),
                    "pick": event_odd.get("odd_key"),
                    "is_peer":is_peer,
                    "peer_bet_id":peer_bet_id,
                    "peer_msisdn":peer_msisdn,
                    "odd_value": float(event_odd.get("odd_value")),
                    "sub_type_id": event_odd.get("sub_type_id"),
                    "special_bet_value": special_bet_value,
                    "live_bet": bet_type})

                current_app.logger.info(
                    "betslips append so far....profile{3} ::bettype {2}"
                    "::slips::{0}:sport:{1}...".format(bet_slips, 
                        event_odd.get("sport_id"), bet_type, profile_id))

                sport_id = scorepesa.get_betrader_sport_id(event_odd.get("sport_id"))
                sbv = special_bet_value if special_bet_value else '*'
                odd_key = str(event_odd.get("odd_key"))
                player_id = str(event_odd.get("id_of_player")) or ""

                current_app.logger.info(
                    "Collected after betslips....{0}::{1}::{2}::{3}"\
                    .format(sport_id, sbv, odd_key, player_id))

                if int(event_odd.get("sub_type_id")) == 235:
                   r,h = lambda data: data, odd_key.split(' ')
                   #odd_key = "%s, %s!%s" % (h[0], h[1], player_id)
                   odd_key = "%s!%s" % (odd_key, player_id)

                makoh = "lcoo:%s/%s/%s/%s" % (event_odd.get("sub_type_id"),
                    sport_id, sbv, odd_key)
                line = "prematch"

                current_app.logger.info(
                    "MTS prematch args:markey:{0}::line:{1}:player:{2}"
                        ":sport:{3}".format(makoh, line, player_id, sport_id))
                if bet_type == 1:
                    current_app.logger.info("Prepare live betting .....")
                    sbv = '%s%s' % ("1/3/", special_bet_value) \
                        if special_bet_value else '*'
                    makoh = "live:%s/%s/%s/%s"  % \
                        (str(event_odd.get("sub_type_id"))[0],
                         str(event_odd.get("sub_type_id"))[1:] or 
                         str(event_odd.get("sub_type_id"))[0],
                         sbv, str(odd_key))
                    line = "live"
                    current_app.logger.info(
                        "Live args :markey:{0}::line:{1}:player:{2}:sport:{3}"\
                        .format(makoh, line, player_id, sport_id))

                #for custom markets to not be submitted to MTS hence being rejected
                if event_odd.get("parent_match_id") < 0:
                    scorepesa.custom_matches=True

                selctions.append({"line": line, "market": makoh,
                      "match": event_odd.get("parent_match_id"),
                       "odd": event_odd.get("odd_value"), "ways": 0, "bank": 0})

                current_app.logger.info(
                    "Collected Bet SLIP: %r :: %r" % (bet_slips, selctions))
                bet_total_odd = bet_total_odd * float(event_odd.get("odd_value"))
                current_app.logger.info("BET TOTAL ODD :: %r" % bet_total_odd)
                game_id_check_list.append(game_id)
                if scorepesa.outright == 3:
                   outrights_check_list.append(ot_competition_id)
        except Exception, e:
            current_app.logger.info(
                "Error fetching match_bet odds: %r :: %r :: %r" 
                % (e, profile_id, message))
            return 421, "We are unable to complete your bet at this time. Kindly try again in a few minutes"

        bet_total_odd = bet_total_odd or 1

        scorepesa.possible_win = float(bet_total_odd) * float(amount)
        invalid_bet_message = scorepesa.invalid_bet_message(profile_id, 
            amount, scorepesa.possible_win)

        current_app.logger.info(
            "Found result form invalid_bet_message: %r" % invalid_bet_message)
        if invalid_bet_message:
            return 421, invalid_bet_message

        fullbet_string = ''.join(gameid_picklist)
        betmessage_string = fullbet_string[1:] + "#" + str(amount)
        if scorepesa.possible_win > float(scorepesa.scorepesa_configs['max_bet_possible_win']):
            scorepesa.possible_win = float(scorepesa.scorepesa_configs['max_bet_possible_win'])
            '''return 421,\
            "Your possible win amount exceeds the maximum allowed for a single \
            bet. You can only win upto Kshs %0.2f amount in a single bet.\
            Please try again. T&C apply"\
            % float(scorepesa.scorepesa_configs['max_bet_possible_win'])'''

        try:
            amount = Decimal(amount)
        except Exception, e:
            current_app.logger.info("cannot cast amount to decimal: %r " % e)
            amount = 0

        current_app.logger.info('READING PROFILE ID: %r' % profile_id)
     
        invalid_bet_limit_response = scorepesa.check_bet_limit(amount, len(bet_slips))
        if invalid_bet_limit_response:
            current_app.logger.info("Invalid Bet Limit: %r" 
                % (invalid_bet_limit_response))
            return 421, invalid_bet_limit_response

        if len(bet_slips) < 1:
            return 421, "Your betslip selection is not valid. Kindly review your selection and try again"

        #if scorepesa.livebetting and profile_id not in scorepesa.scorepesa_configs["tests_whitelist"].split(','):
        #    return 421, "Live is currently unavailable."

        bet_message = betmessage_string
        bet_id = scorepesa.place_bet(profile_id, bet_message, amount, 
            bet_total_odd, scorepesa.possible_win, bet_slips,
            live_bet=bet_type, app=app_name)

        if not bet_id:
            current_app.logger.info(
                'Unable to place api bet for profile:: %r, %r' % (profile_id, scorepesa.jp_bet_status))
            if scorepesa.jp_bet_status == 700:
                pname = scorepesa.name.split(' ')[0] + ' ' if scorepesa.name else ''
                self.send_sms({'msisdn':scorepesa.peer_msisdn,
                    'reference_id':'',
                    'netwok':message.get('network', 'SAFARICOM'),
                    'profile_id':'',
                    'message':"%s(%s) has invited you for BESTE BET on scorepesa.co.ke."\
                             " Kindly accept the bet to complete your bet" % (pname,scorepesa.msisdn )}
                )
                return 201, "Your Beste bet has been booked pending acceptance from "\
                    "%s Kindly advice them to accept in order to complete this bet na beste with SCOREPESA." % (scorepesa.peer_msisdn, )
            if scorepesa.jp_bet_status == 423:
                return 421, "Sorry cannot create bet, minimum odds accepted "\
                    "for bets on bonus amounts is {0}. Please review selections."\
                    .format(scorepesa.scorepesa_configs['bonus_bet_minimum_odd'])
            if scorepesa.jp_bet_status == 424:
                return 421, "Sorry cannot create bet, minimum odds accepted"\
                    "for bets on bonus amounts is {0}. Please review "\
                    "selections and try again."\
                    .format(scorepesa.scorepesa_configs['bonus_bet_minimum_odd'])
            if scorepesa.jp_bet_status == 425:
               return 421, "Sorry we are unable to create your bet.{0}"\
               .format(scorepesa.scorepesa_bonus_cfgs['referal_bonus_not_bet_notify'])
            if scorepesa.jp_bet_status == 426:
               return 421, "Sorry we are unable to create your bet.{0}"\
               .format(scorepesa.scorepesa_bonus_cfgs['referal_bonus_expired_notify'])
            if scorepesa.jp_bet_status == 427:
                return 421, "You have insufficient balance, Kindly top up your account "\
                    "to enjoy 10% bonus on your first three bets. "\
                    "PAYBILL 290080.SCOREPESA T&C apply"

            return 421, "We are unable to complete your bet at this time. Kindly try again in a few minutes"\
                .format(scorepesa.referral_bonus_advise_notification)
        current_app.logger.info('SAVED BET ID: %r' % bet_id)
        current_balance, bonus_balance = scorepesa.get_account_balance(message)
        #bonus_balance = scorepesa.get_bonus_balance(message)
        #bet_message = "Bet ID %s, %s. %sPossible win Ksh %0.2f. bal is Ksh"\
        #    " %0.2f. Bonus bal Ksh %0.2f.%s%s%s%s" % \
        #   (bet_id, betmessage_string, scorepesa.multibet_bonus_message, 
        #    (scorepesa.multibet_possible_win or scorepesa.possible_win), 
        #    current_balance, bonus_balance, 
        #    scorepesa.freebet_notification, 
        #    scorepesa.bonus_bet_low_odd_msg, 
        #    scorepesa.referal_bonus_fail_notify, scorepesa.referal_bonus_extra)
        converter = lambda amount: "%s%s" % ( "-" if amount < 0 else "",\
            ('{:%d,.2f}'%(len(str(amount))+3)).format(abs(amount)).lstrip())

        if scorepesa.is_paid:
            desc = 'MULTIBET' if len(bet_slips) > 1 else 'SINGLEBET'
            if scorepesa.beshte_bet_id:
                desc = 'BESTE BET'
                self.send_sms({'msisdn':scorepesa.peer_msisdn,
                    'reference_id':'',
                    'netwok':message.get('network', 'SAFARICOM'),
                    'profile_id':'',
                    'message':"SCOREPESA %s ID %s placed successfully. Possible WIN "\
                             "KSH %s. " % (desc, scorepesa.beshte_bet_id, converter(scorepesa.possible_win))}
                )

            bet_message = "SCOREPESA %s ID  %s. placed successfully."\
                 "Possible WIN  KSH %s, Account balance KSH %0.2f. Bonus balance KSH %0.2f" % (\
                 desc, bet_id, 
                 converter(scorepesa.multibet_possible_win or scorepesa.possible_win),
                 math.floor(current_balance), math.floor(bonus_balance))
        else:
            #bet_message = "Bet yako imekamilika, Namba ya tiketi/kumbukumbu namba ni %s, "\
            #     "Mechi zako ulizochagua ni %s, "\
            #     "Kiasi ulichobet ni KSH %0.2f, Ushindi wako ni KSH %s.  Ushindi utaupata"\
            #     " kupitia namba uliyolipia. Namba ya kampuni 101010." % (\
            #     bet_id, len(bet_slips), amount, converter(scorepesa.multibet_possible_win or scorepesa.possible_win) )
            bet_message = "SCOREPESA %s ID %s has been BOOKED. "\
                "Bet amount KSH %0.2f, Possible WIN KSH %s. Pay for your bet via "\
                "PAYBILL 290080 Account %s. T&C apply" % (\
                'MULTIBET' if len(bet_slips) > 1 else 'SINGLEBET',bet_id,
                 amount, converter(scorepesa.multibet_possible_win or scorepesa.possible_win), bet_id)


        #publish to betrader 4 validation
        #profile_id = message.get("profile_id", '')
        #msisdn = scorepesa.get_profile_msisdn(profile_id)

        #current_app.logger.info(
        #    'API Invoke betrader validation %r::%r::%r::%r::%r' % \
        #    (msisdn, scorepesa.scorepesa_configs['enable_bet_validation'], 
        #    scorepesa.scorepesa_configs['validation_sport_ids'].split(','), 
        #    scorepesa.scorepesa_configs['tests_whitelist'].split(','), sport_id))

        #ch_id = message.get('channelID', None)
        #ip = message.get('endCustomerIP', None)
        #devId = message.get('deviceID', None)
          
        #enable/disable mts live bets submits
        #if scorepesa.livebetting and \
        #    scorepesa.scorepesa_configs['enable_mts_live_bet_submit'] == '1':
        #    response = 201, bet_message
        #else:
        #    response = 201, scorepesa.prepare_invoke_betrader(bet_message, 
        #        msisdn, amount, selctions, bet_id, 
        #        bet_slips, profile_id, sport_id, ch_id, ip, devId)
        #scorepesa.call_bet_award_scorepesa_points(
        #    profile_id, bet_slips, bet_total_odd, bet_id)
        #referal bonus adjust after bet
        #if str(profile_id) not in \
        #    scorepesa.scorepesa_bonus_cfgs['referral_test_whitelist'].split(','):
        #    if scorepesa.scorepesa_configs['award_referral_bonus'] == '1':
        #         referral_bonus = ReferralBonus(
        #            current_app.logger, profile_id)                                 
        #         referral_bonus.award_referral_bonus_after_bet(
        #            profile_id, amount, bet_total_odd)
        self.bet_id = bet_id
        return 201, bet_message


class BetVirtual(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('token', type=str, required=True, 
            help='Provide token')
        args = parser.parse_args(strict=True)
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        current_app.logger.info(
            "JWT Virtuals bet api args :: {0} :configs: {1}"\
            .format(args, scorepesa_cfgs['encrption_key']))
        response = 'Bad Request'
        status = 421
        try:
            data = jwt.decode(args['token'], 
                scorepesa_cfgs['encrption_key'], algorithms=['HS256'])
            scorepesa = Scorepesa(current_app.logger)
            message_json = data.get('user')
            current_app.logger.info(
                "virtual bet data extracted :::{0} ::: {1}"\
                .format(message_json, data))
            if message_json:
                res = "Sorry Scorepesa virtual is not available now. "\
                    "Please check again later."
                status, response = self.virtuals_api_bet(message_json)
        except JWTError as e: 
            current_app.logger.error("Virtual Bet token exception %r " % e)
        current_app.logger.info(
            "jwt virtual bet api response message:: %r :: %r" 
            % (response, status))
        return status, response

    def virtuals_api_bet(self, message):
        scorepesa = Scorepesa(current_app.logger)
        scorepesaVirtual = ScorepesaVirtual(current_app.logger)
        profile_id = message.get('profile_id')
        if profile_id is None or profile_id=='':
            return 421, "Bet failed, please try again later."
        if int(profile_id) < 0:
            return 421, "Bet failed, please try again later."

        if scorepesa.check_account_freeze(None, profile_id):
            return 421, scorepesa.scorepesa_configs['account_bet_block_msg']
    
        slips = message.get('slip')
        possible_win = message.get('possible_win')
        app_name = message.get('app_name') \
            if message.get('app_name') else 'API_WEB'
        amount = message.get('stake_amount')
        amount = abs(float(amount))
        if float(amount) < 1.0:
            return 421, "Bet failed, please try again later."
        if len(slips) < 1:
            return 421, "Bet failed, please select a game pick to bet on."
        #bonus_balance = scorepesa.get_bonus_balance(message)
        current_app.logger.info(
            "virtuals bet amount::: {0} ::message:: {1} ::profile:: {2}:::"\
            .format(amount, message, profile_id))

        status, response = scorepesaVirtual.place_virtual_bet(message)
        return status, response


class ProfileMgt(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('token', type=str, required=True,
                 help='Provide valid token')
        args = parser.parse_args(strict=True)
        current_app.logger.info(
            "create profile/award registration bonus api args %r" % args)
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        try:
            _code = 421
            data = jwt.decode(args['token'], 
                scorepesa_cfgs['encrption_key'], algorithms=['HS256'])
            current_app.logger.info(
                "create profile/registration bonus api req data %r" % data)
            scorepesa = Scorepesa(current_app.logger)

            profile_id = data.get("user").get('profile_id')
            msisdn = data.get("user").get('msisdn')
            
            if not profile_id:
                profile_id = scorepesa.get_msisdn_profile_id(msisdn)
            
            operator = data.get("user").get('operator') \
                if data.get("user").get('operator') \
                        else scorepesa.get_network_from_msisdn_prefix(msisdn)

            message = {"profile_id":profile_id,
                 "message": "JOIN", "sdp_id": "6013852000120687",
                  "short_code": "101010", "network": operator,
                   "link_id": "7892937"}
            current_app.logger.info("create profile message %s" % message)
            profile, new = scorepesa.create_profile(message)
            current_app.logger.info(
                "award registration bonus api response %r::%r" % (profile, new))
            result = "Registration bonus award failed"
            if profile:
                _code = 200
                result = "Registration bonus awarded success."
            current_app.logger.info(
                "award bonus response %s::%s" % (result, _code))
        except JWTError as e:
            current_app.logger.error("registration bonus api xception %r " % e)
            _code = 421
            result = "Invalid token provided."
        resp = make_response(json.dumps(result), _code)
        return resp


class BetCancel(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('token', type=str, required=True,
                 help='Provide a valid token')
        args = parser.parse_args(strict=True)
        scorepesa = Scorepesa(current_app.logger)
        current_app.logger.info("cancel bet api args %r" % args)
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        try:
            data = jwt.decode(args['token'], scorepesa_cfgs['encrption_key'],
                 algorithms=['HS256'])
            current_app.logger.info("got cancel bet data %r " % data)
    	    msisdn = data.get("user").get('msisdn')
            operator = scorepesa.get_network_from_msisdn_prefix(msisdn)
            if operator == "SAFARICOM":
                smsc = "SAFARICOM_TX"
                network = "SAFARICOM"
            elif operator == "AIRTEL":
                smsc = "KIBOKO_AIRTEL_TRX"
                network = "AIRTEL"
            else:
                smsc = "SAFARICOM_TX"
                network = "SAFARICOM"
            profile_id = data.get("user").get('profile_id')
            current_app.logger.info(
                "cancel bet api profile_id :: {0} ::msisdn:: {1}"\
                .format(profile_id, msisdn))
            #for cases like ussd
            if not profile_id:
                 profile_id = scorepesa.get_msisdn_profile_id(msisdn)
            
            cancel_code = data.get("user").get('cancel_code') or 101
            bet_id = data.get("user").get('bet_id')

            response = "Bet cancel service is currently disabled."

            if int(scorepesa.scorepesa_configs['enable_bet_cancel_testing']) == 1:
                if msisdn in scorepesa.scorepesa_configs["tests_whitelist"].split(','):
                   testing=True
                else:
                   testing=False
            else:
                testing=True

            if testing and int(scorepesa.scorepesa_configs["enable_bet_cancel"]) == 1:
                result = scorepesa.betrader_bet_cancel_submit(bet_id, 
                    cancel_code, None, msisdn, profile_id)
                if result:
                    response = "Bet cancel request was received."
                else:
                    self.APP_NAME = "WEB_API"
                    response = scorepesa.cancel_bet_trx(bet_id, None, profile_id, None)

            scorepesa.profile_id = profile_id
            message={
               'short_code':719408,
               'profile_id':profile_id,
               'message':response,
               'network':network,
               'link_id':476628891,
               'sdp_id':'6013852000120687',
               'msisdn':msisdn
            }

            outbox = scorepesa.outbox_message(message, response)
            outbox_id=scorepesa.outbox_id
            #sdp_service_id = '6013852000120687'
            #meta_data = \
            #    urllib.quote('?http?sdp_service_id=%s&correlator=%s&link_id=%s' 
            #        % (sdp_service_id, outbox_id, outbox_id))
            current_app.logger.info("betcancel calling sms gateway ::%r" 
                % (message,))

            payload={
               'momt':'mo', 
               'sender':29008,
               'receiver':msisdn,
               'message':response,
               'time':datetime.now().strftime('%Y%m%d%H%M%S'),
               'smsc_id':smsc,
               'sms_type':2,
               'boxc_id':'sqlbox_content',
               'short_code':29008,
               'network':network,
               'outbox_id':outbox_id,
               'sdp_id':'6013852000120687',
               'msisdn':msisdn
            }
            output = sms_gateway_send_sms(payload)
            current_app.logger.info(
                "Found result from sms gateway: (%r) Outbox result: (%r)" 
                    % (output, outbox))
            return make_response(response, 201)
        except JWTError as e:
            current_app.logger.error("Exception %r " % e)
            _code = 421
            result = "Invalid token provided."
            return make_response(json.dumps(result), _code)


class TelegramRequest(Resource):
    def post(self):
        scorepesa = Scorepesa(current_app.logger)
        parser = reqparse.RequestParser()
        parser.add_argument('msisdn', type=int, required=True,
                 help='Provide mobile number')
        parser.add_argument('message', type=str, required=True,
                 help='Provide message string')
        args = parser.parse_args(strict=True)
        current_app.logger.info("telegram api args %r" % args)
        data = {
            "msisdn": str(args['msisdn']),
            "message": args['message']
            }
        _id, result, ignore = scorepesa.process_request(data)
        resp = make_response(result, 200)
        return resp

class MQRequest(Resource):
    def post(self):
        message_object = request.get_json()
        scorepesa = Scorepesa(current_app.logger)
        operator = scorepesa.get_network_from_msisdn_prefix(
            message_object.get("msisdn"))
        current_app.logger.info(
            "mqrequest operator ....{0}:::mqreq msisdn{1} ..."\
            .format(operator, message_object.get("msisdn")))
        if operator == "SAFARICOM":
            smsc = "SAFARICOM_TX"
            network = "SAFARICOM"
            operator = 'safaricom'
            paybill_detail = "Mpesa paybill 290080 Account no. SCOREPESA"
            customer_care = "Call-0101 290080"
        elif operator == "AIRTEL":
            smsc = "KIBOKO_AIRTEL_TRX"
            network = "AIRTEL"
            operator = 'airtel'
            paybill_detail = "Airtel money  business name is SCOREPESA"
            customer_care = "Call-0101 290080"
        else:
            smsc = "SAFARICOM_TX"
            network = "SAFARICOM"
            operator = 'safaricom'
            paybill_detail = "MPESA PAYBILL 290080 Account no. SCOREPESA"
            customer_care = "Call-0101 290080"

        DEFAULT_REG_BULK_MESSAGE = "Karibu scorepesa.co.ke SMS GAMES to 29008."\
            " Paybill 290080. Singlebet:Send"\
            " GAMEID#PICK#AMOUNT. Multibet: "\
            "GAMEID#PICK#GAMEID#PICK#AMOUNT to 29008. Call-0101290080"
        
        current_app.logger.info("MQ REQUEST api args %r" % message_object)
        sdp_configs = LocalConfigParser.parse_configs("SDP")
        sdp_url = sdp_configs['url']
        try:
            scorepesa = Scorepesa(current_app.logger)
            outbox_id, response, new = scorepesa.process_request(message_object)
            current_app.logger.info(
                "Response from scorepesa process (%r, %r, %r) " 
                % (outbox_id, response, new))
            payload = {
                'phone': message_object.get("msisdn"),
                'message': response,
                'linkid': message_object.get("link_id"),
                'refNo':"",
                'serviceId': message_object.get("sdp_id"),
                'service_id': message_object.get("sdp_id"),
                'short_code': message_object.get("short_code"),
                "sender": message_object.get("short_code"),
                "message_type": 'mo',
                "sms_type": 2,
                "smsc": smsc,
                'correlator': outbox_id,
                'outbox_id':outbox_id
            }
            send = None
            _response = None
            if response == 'WITHDRAW':
                return make_response('Success', 200)

            if response:
                current_app.logger.info(
                    "FOUND RESPONSE: %r, %r, %r" % (outbox_id, response, new))
                if new:
                    current_app.logger.info("New user sending bulk message ")
                    self.send_bulk_message(sdp_configs,
                        {
                            "msisdn": message_object.get("msisdn"),
                            "message": DEFAULT_REG_BULK_MESSAGE,
                            'correlator': outbox_id,
                            'linkid':'',
                            'refNo': outbox_id,
                            'outbox_id': outbox_id,
                            "smsc": smsc,
                            "sender": "SCOREPESA" if network != "SAFARICOM" else None
                        })

                send = sms_gateway_send_sms(payload)
                scorepesa.update_outbox(outbox_id, send)
                current_app.logger.info("Send message response : %r " % send)
                _response = make_response('Success', 200)
            else:
                _response = make_response('Failed to send MO Message', 500)
            current_app.logger.info(
                "Message process success with message: %r " % response)
        except Exception, ex:
            current_app.logger.error(
                "Error picking message: %r %r " % (ex, message_object))
            _response = make_response('Exception processing', 500)

        return _response or make_response('Error', 500)


    def send_bulk_message(self, sdp_configs, message):
        current_app.logger.info("Calling send_bulk_message ..blk")
        url = sdp_configs["bulk_url"]

        payload = {
            "msisdn": message.get('msisdn'),
            "message": message.get('message') or message.get('text'),
            "access_code": sdp_configs["access_code"],
            "linkid": "",
            "refNo": "",
            "outbox_id":message.get("outbox_id"),
            "message_type": 'mt',
            "sms_type": 2,
            "short_code": sdp_configs["bulk_code"],
            "smsc": message.get('smsc'),
            "sender": sdp_configs["access_code"] \
                if message.get('bsender') is None else message.get('bsender'),
            "correlator": message.get("correlator"),
            "service_id": sdp_configs["service_id"] or "6013852000120499"
        }

        current_app.logger.info(
            "Calling SDP URL BULK: (%s, %r) " % (url, payload))
        output = None

        try:
            output = sms_gateway_send_sms(payload)
            if output is None and message.get('smsc') == "SAFARICOM_TX":
                current_app.logger.info(
                    "Found result from sdp call: (%r) " % (output.text, ))
	    current_app.logger.info(
            "Found result kannel sendsms: (%r) " % (output))
        except requests.exceptions.RequestException as e:
            current_app.logger.error(
                "Exception attempting to send MO message : %r :: %r " 
                % (payload, e))
            output = None

        return output

    

class Withdraw(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('token', type=str, required=True,
                 help='Provide valid token')
        args = parser.parse_args(strict=True)
        current_app.logger.info("withdraw api args %r" % args)
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        try:
            data = jwt.decode(args['token'], scorepesa_cfgs['encrption_key'],
                 algorithms=['HS256'])
            current_app.logger.info("RAW Data api args %r" % data)
            scorepesa = Scorepesa(current_app.logger)
            msisdn = data.get('user').get('msisdn')
            current_app.logger.info("Extracted user number: %r" % msisdn)
            profile_data =\
             scorepesa.get_user_profile_data(msisdn)
            current_app.logger.info("profile data %r" % profile_data)
            scorepesa.inbox_id = None
            if profile_data:
                message = {"msisdn": data.get('user').get('msisdn'),
                     "message": "api withdraw request", "inbox_id": None}
                text_dict = ['', data.get('user').get('amount')]
                current_app.logger.info("message data %r:::%r::%r"
                 % (message, text_dict, text_dict))
                result, _code = scorepesa.process_withdrawal(message, text_dict)
                current_app.logger.info("withdraw result response %r" % result)
            else:
                _code = 421
                result = "Invalid token provided."
        except JWTError as e:
            current_app.logger.error("Exception %r " % e)
            _code = 421
            result = "Invalid token provided."
        resp = make_response(json.dumps(result), _code)
        return resp


class Balance(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('token', type=str, required=True,
                 help='Provide valid token')
        args = parser.parse_args(strict=True)
        current_app.logger.info("balance api args %r" % args)
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        try:
            _code = 200
            data = jwt.decode(args['token'], scorepesa_cfgs['encrption_key'],
                 algorithms=['HS256'])
            current_app.logger.info("balance req data %r" % data)
            scorepesa = Scorepesa(current_app.logger)
            message = {"msisdn": data.get("msisdn")}

            bal, bonus = scorepesa.get_account_balance(message)

            profile_id = scorepesa.get_msisdn_profile_id(data.get("msisdn"))
            
            current_app.logger.info(
                "scorepesa balance api got msisdn's [][] {0} [] profile [] {1}"\
                .format(data.get("msisdn"), profile_id))
            scorepesapoint = ScorepesaPoint(current_app.logger, profile_id)

            points = scorepesapoint.get_balance()

            result = {"balance": bal, "bonus": bonus, 
                "total": float(bal)+float(bonus), "points": points}
        except JWTError as e:
            current_app.logger.error("Exception %r " % e)
            _code = 400
            result = "Invalid balance token provided."
        resp = make_response(json.dumps(result), _code)
        return resp

class BetDetail(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('token', type=str, required=True,
                 help='Provide valid token')
        args = parser.parse_args(strict=True)
        current_app.logger.info("balance api args %r" % args)
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        try:
            _code = 200
            data = jwt.decode(args['token'], scorepesa_cfgs['encrption_key'],
                 algorithms=['HS256'])
            current_app.logger.info("balance req data %r" % data)
            scorepesa = Scorepesa(current_app.logger)
            bet_id, bet_amount, is_valid = scorepesa.get_bet_details(data)

            current_app.logger.info(
                "scorepesa bet_check_api api got bet_id's [][] {0} [] msisdn [] {1}"\
                .format(data.get("bet_id"), data.get('msisdn')))

            result = {"bet_amount": bet_amount, "bet_id": bet_id, 
                "is_valid": is_valid, "msisdn": data.get('msisdn')}
            has_account = scorepesa.get_profile_setting(data.get('msisdn'))
            account_no_is_friend = 0
            if bet_id == 0 and has_account == 0:
                account_no_is_friend = scorepesa.get_profile_setting(data.get('bet_id'))
            result['has_account'] = str(has_account)
            result['account_no_is_friend'] = str(account_no_is_friend)
        except JWTError as e:
            current_app.logger.error("Exception %r " % e)
            _code = 400
            result = "Invalid balance token provided."
        resp = make_response(json.dumps(result), _code)
        return resp


class UssdMatch(Resource):

    def __init__(self):
        '''
        DOC:::Incase in future we need redis
        self.redis_c = RedisCore(current_app.logger, 
        current_app.config['redis_host'], current_app.config['redis_port'], 
        current_app.config['db'], current_app.config['redis_passwd'])
        '''
        self.log = current_app.logger
        self.log.info("ussd match init()........")
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('token', type=str, required=True, 
            help='Provide valid token')
        self.args = self.parser.parse_args(strict=True)
        self.log.info(
            "incomming ussd match request got args.... {0}.."\
            .format(self.args,))

    def __del__(self):
        self.log.info("destroy ussd match obj....")
        if self.parser:
           self.parser = None
        if self.args:
           self.args =None

    def get(self, reqparam):
        self.log.info("received API request ::: {0} ::::".format(reqparam))
        result = self.router(self.args, reqparam)
        self.log.info("returning got result as ... {0}".format(result))
        return result 

    def router(self, args, reqparam):
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        redis_cfgs = LocalConfigParser.parse_configs("SCOREPESAREDIS")
        data = jwt.decode(args['token'], scorepesa_cfgs['encrption_key'], 
            algorithms=['HS256'])
        self.log.info("router extracted request data..... %r" % data)
        result = []
        #routes
        if reqparam == 'detail':
            result =self.process_match_detail(data)
        elif reqparam == 'search':
            result =self.process_match_search(data)
        elif reqparam == 'highlight':
            result =self.process_ussd_match_highlights(data)
        elif reqparam == 'fetch':
            result =self.process_ussd_matches(data)
        elif reqparam == 'sport': 
            result =self.process_sport_details(data)
        elif reqparam == 'sportMatch':
            result =self.process_sport_matches(data)
        elif reqparam == 'userExist':
            result =self.check_user_exists_scorepesa(data)
        elif reqparam == 'topleague':
            result =self.fetch_scorepesa_top_league(data)
        elif reqparam == 'topleaguematch':
            result =self.fetch_scorepesa_top_league_match(data)
        elif reqparam == 'jpmatch':
            result = self.fetch_jp_matches();

        return result

    def process_ussd_matches(self, data):
        try:
            _code = 200
            '''
            data = jwt.decode(args['token'], scorepesa_cfgs['encrption_key'],
                 algorithms=['HS256'])
            '''
            self.log.info("process_ussd_matches request data..... %r" % data)
            scorepesa = ScorepesaUssd(self.log)
            message = {"msisdn": data.get('msisdn'),
                       "profile_id": data.get('profile_id')}
            result = scorepesa.daily_match_sport(message)
            self.log.info("fetched process_ussd_matches from db ... {0}"\
                .format(result))
        except JWTError as e:
            self.log.error("Exception on process_ussd_matches %r " % e)
            _code = 400
            result = "Invalid match token provided."
        resp = make_response(json.dumps(result), _code)
        return resp

    def process_ussd_match_highlights(self, data):
        print "OVERWOKING 21", 'process_ussd_match_highlights', data
        try:
            _code = 200
            self.log.info(
                "process_ussd_match highlight request data..... %r" % data)
            scorepesa = ScorepesaUssd(self.log)
 
            message = {"msisdn": data.get('msisdn'), 
                "profile_id": data.get('profile_id')}
            result = scorepesa.daily_match_highlight(message)
            self.log.info(
                "fetched process_ussd_match highlights from db ... {0} ==> "\
                .format(result))
        except Exception, e:
            self.log.error(
                "Exception on process_ussd_matches highlight %r " % e)
            _code = 400
            result = "Invalid match token provided."
        resp = make_response(json.dumps(result), _code)
        return resp

    def check_user_exists_scorepesa(self, data):
        try:
            _code = 200
            self.log.info("check user exists request data..... %r" % data)
            scorepesa = ScorepesaUssd(self.log)
            message = {"msisdn": data.get('msisdn')}
            result = scorepesa.ussd_check_user_exists(message)
            self.log.info("fetched user existence ... {0}"\
                .format(json.dumps(result)))
        except Exception, e:
            self.log.error("Exception on check user exists %r " % e)
            _code=400
            result = "Invalid match token provided."
        return make_response(json.dumps(result), _code)

    def process_sport_details(self, data):
        try:
            _code = 200
            self.log.info("process_ussd_match sport detail request data..... %r" % data)
            scorepesa = ScorepesaUssd(self.log)
            message = {"msisdn": data.get('msisdn'), "profile_id": data.get('profile_id')}
            result = scorepesa.ussd_match_sport_ids(message)
            self.log.info("fetched process_ussd_match sport detail from db ... {0}".format(result))
        except JWTError as e:
            self.log.error("Exception on process_ussd_matches sport detail %r " % e)
            _code = 400
            result = "Invalid match token provided."
        resp = make_response(json.dumps(result), _code)
        return resp

    def process_sport_matches(self, data):
        try:
            _code = 200
            self.log.info(
                "process_ussd_match sport request data..... %r" % data)
            scorepesa = ScorepesaUssd(self.log)
            message = {"msisdn": data.get('msisdn'), 
                "profile_id": data.get('profile_id'), 
                "sport_id": data.get('sport_id'), 
                "sub_type_id": data.get('sub_type_id')}
            result = scorepesa.daily_match_sport(message)
            #self.log.info("fetched process ussd sport match from db ... %r" % result)
        except JWTError as e:
            self.log.error("Exception on process_ussd_matches sport %r " % e)
            _code = 400
            result = "Invalid match token provided."
        resp = make_response(result, _code)
        return resp

    def process_match_detail(self, data):
        try:
            _code = 200
            self.log.info(
                "process ussd match detail request data..... %r" % data)
            scorepesa = ScorepesaUssd(self.log)
            message = {"parent_match_id": data.get('parent_match_id'), 
                "game_id": data.get('game_id'), "pick": data.get('pick')}
            parent_match_id = data.get('parent_match_id')
            sub_type = data.get('sub_type_id')
            result = scorepesa.get_match_details(data.get('game_id'), 
                data.get('pick'), sub_type, parent_match_id)
        except JWTError as e:
            self.log.error("Exception on process_match_detail %r " % e)
            _code = 400
            result = "Invalid match detail token provided."
        resp = make_response(json.dumps(result), _code)
        return resp

    def process_match_search(self, data):
        '''
        parser = reqparse.RequestParser()
        parser.add_argument('token', type=str, required=True,
                 help='Provide valid token')
        args = parser.parse_args(strict=True)
        
        current_app.logger.info("match search detail api args %r" % args)
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        '''
        self.log.info("match search detail api data.... %r" % data)
        try:
            _code = 200
            '''
            data = jwt.decode(args['token'], scorepesa_cfgs['encrption_key'],
                 algorithms=['HS256'])
            current_app.logger.info("match search detail data %r" % data)
            '''
            scorepesa = ScorepesaUssd(self.log)
            search_term = data.get('search_term')
            msisdn = data.get('msisdn')
            result = scorepesa.search_for_match(search_term, msisdn)
        except JWTError as e:
            self.log.error("Exception on search match ...%r " % e)
            _code = 400
            result = "Invalid match detail token provided."
        resp = make_response(json.dumps(result), _code)
        return resp

    def fetch_scorepesa_top_league_match(self, data):
        try:
            _code = 200
            self.log.info(
                "process_ussd_match top league request data..... %r" % data)
            scorepesa = ScorepesaUssd(self.log)
            message = {"msisdn": data.get('msisdn'), 
                "profile_id": data.get('profile_id'), 
                "league_id": data.get('league_id'), 
                "sport_id": data.get('sport_id')}
            result = scorepesa.fetch_top_league_matches(message)
            self.log.info(
                "fetched process_ussd_match to league from db ... {0}"\
                .format(result))
        except Exception, e:
            self.log.error(
                "Exception on process_ussd_matches top league %r " % e)
            _code = 400
            result = "Invalid match token provided."
        resp = make_response(json.dumps(result), _code)
        return resp

    def fetch_jp_matches(self):
        try:
            _code = 200
            self.log.info(
                "process_ussd_match JP  games REQUEST")
            scorepesa = ScorepesaUssd(self.log)
            result = scorepesa.jackpot_games()
            self.log.info(
                "fetched process_ussd_match to league from db ... {0}"\
                .format(result))
        except Exception, e:
            self.log.error(
                "Exception on processing USSD matches %r " % e)
            _code = 400
            result = "Invalid match token provided."
        resp = make_response(json.dumps(result), _code)
        return resp

    def fetch_scorepesa_top_league(self, data):
        try:
            _code = 200
            self.log.info(
                "process_ussd_match top league detail request data..... %r" 
                % data)
            scorepesa = ScorepesaUssd(self.log)
            message = {"msisdn": data.get('msisdn'), 
                "profile_id": data.get('profile_id')}
            result = scorepesa.ussd_match_top_leagues(message)
            self.log.info(
                "fetched process_ussd_match top league detail from db => {0}"\
                .format(result))
        except JWTError as e:
            self.log.error(
                "Exception on process_ussd_matches top league detail %r " % e)
            _code = 400
            result = "Invalid match token provided."
        resp = make_response(json.dumps(result), _code)
        return resp


class JackpotMatches(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('token', type=str, required=True,
                 help='Provide valid token')
        args = parser.parse_args(strict=True)
        current_app.logger.info("jackpot matches api args %r" % args)
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        try:
            _code = 200
            data = jwt.decode(args['token'], scorepesa_cfgs['encrption_key'],
                 algorithms=['HS256'])
            current_app.logger.info("jp games data %r" % data)
            scorepesa = Scorepesa(current_app.logger)
            message = {"msisdn": data.get('msisdn'), "profile_id": None,
                 "jptype": data.get('jp_type')}
            result = scorepesa.jackpot_matches(message)
        except JWTError as e:
            current_app.logger.error("Exception %r " % e)
            _code = 400
            result = "Invalid JP matches token provided."
        resp = make_response(json.dumps(result), _code)
        return resp


class BingwaMatches(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('token', type=str, required=True,
                 help='Provide valid token')
        args = parser.parse_args(strict=True)
        current_app.logger.info("Bingwa matches api args %r" % args)
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        try:
            _code = 200
            data = jwt.decode(args['token'], scorepesa_cfgs['encrption_key'],
                 algorithms=['HS256'])
            current_app.logger.info("bingwa games data %r" % data)
            scorepesa = Scorepesa(current_app.logger)
            message = {"msisdn": data.get('msisdn'), "profile_id": None,
                 "jptype": data.get('jp_type')}
            result = scorepesa.jackpot_matches(message)
        except JWTError as e:
            current_app.logger.error("Exception %r " % e)
            _code = 400
            result = "Invalid bingwa matches token provided."
        resp = make_response(json.dumps(result), _code)
        return resp


class ScorepesaApiSendSms(Resource):
    def post(self):
        queue_name = "SCOREPESA_SENDSMS"
        exchange_name = "SCOREPESA_SENDSMS_EX"
        routing_key = "SCOREPESA_SENDSMS_KEY"
        parser = reqparse.RequestParser()
        parser.add_argument('msisdn', type=int, required=True,
             help='Provide recipient mobile number')
        parser.add_argument('message', required=True,
             help='Provide message text')
        parser.add_argument('message_type', required=True,
             help='Provide message type i.e BULK or MO')
        parser.add_argument('short_code', required=True,
             help='Provide source address for the message, i.e SCOREPESA or 101010')
        parser.add_argument('correlator', required=True,
             help='Optional message traceId if expecting delivery report '\
                'back, else set null.')
        parser.add_argument('link_id', required=True,
             help='Optional message Link Id this is for MO messages only, '\
                'else set null.')
        parser.add_argument('exchange', required=False,
             help='You might need this to route message .. dup')

        args = parser.parse_args(strict=True)
        current_app.logger.info("sendsms args %r" % args)
        message = {"msisdn": args['msisdn'], 
            "message": urllib.unquote_plus(urllib.unquote_plus(args['message'])),
            "access_code": args['short_code'], "correlator": args['correlator'], 
            "message_type": args['message_type'], "link_id": args['link_id'], 
            "exchange":queue_name}
        pub = SendSmsPublisher(queue_name, queue_name)
        result = {"message": "failed"}
        if pub.publish(message, queue_name):
            result = {"message": "success"}
        resp = make_response(json.dumps(result), 200)
        return resp


class ScorepesaTestApiSendSms(Resource):
    def post(self):
        queue_name = "profile_SCOREPESA_RETRY_QUEUE"
        exchange_name = "profile_SCOREPESA_RETRY_QUEUE"
        routing_key = "SCOREPESA_SENDSMS_KEY"
        parser = reqparse.RequestParser()
        parser.add_argument('msisdn', type=int, required=True,
             help='Provide recipient mobile number')
        parser.add_argument('message', required=True,
             help='Provide message text')
        parser.add_argument('message_type', required=True,
             help='Provide message type i.e BULK or MO')
        parser.add_argument('short_code', required=True,
             help='Provide source address for the message, i.e '
                'SCOREPESA or 29008')
        parser.add_argument('correlator', required=True,
             help='Optional message traceId if expecting delivery report '\
                  'back, else set null.')
        parser.add_argument('link_id', required=True,
             help='Optional message Link Id this is for MO messages only, '\
                'else set null.')
        args = parser.parse_args(strict=True)
        current_app.logger.info("sendsms args %r" % args)
        message = {"msisdn": args['msisdn'], "message": args['message'],
             "access_code": args['short_code'], "correlator":
                  args['correlator'], "message_type": args['message_type'],
                   "link_id": args['link_id']}
        pub = SendSmsPublisher(queue_name, exchange_name)
        result = {"message": "failed"}
        if pub.publish(message, routing_key):
            result = {"message": "success"}
        resp = make_response(json.dumps(result), 200)
        return resp


class ScorepesaAppVersion(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('token', type=str, required=True,
                 help='Provide valid token')
        args = parser.parse_args(strict=True)
        current_app.logger.info("Bingwa matches api args %r" % args)
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        try:
            _code = 200
            data = jwt.decode(args['token'], scorepesa_cfgs['encrption_key'],
                 algorithms=['HS256'])
            current_app.logger.info("bingwa games data %r" % data)
            scorepesa = Scorepesa(current_app.logger)
            message = {"msisdn": data.get('msisdn'), "profile_id": None,
                 "jptype": data.get('jp_type')}
            result = scorepesa.jackpot_matches(message)
        except JWTError as e:
            current_app.logger.error("Exception %r " % e)
            _code = 400
            result = "Invalid bingwa matches token provided."
        resp = make_response(json.dumps(result), _code)
        return resp

class ScorepesaAppRegDevice(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('token', type=str, required=True,
                 help='Provide valid token')
        args = parser.parse_args(strict=True)
        current_app.logger.info("Device registration detail %r" % args)
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESAAPP")
        try:
            http_code = 200
            data = jwt.decode(args['token'], scorepesa_cfgs['encrption_key'],
                 algorithms=['HS256'])
            current_app.logger.info("device registeration data..... %r" % data)
            helper = Helper(current_app.logger)
            message = {"msisdn": data.get('msisdn'), "profile_id": None,
                 "jptype": data.get('jp_type')}
            result = helper.register_device_identity(message)
        except JWTError as e:
            current_app.logger.error("Exception on device registration.... %r " % e)
            http_code = 400
            result = "Invalid request."
        resp = make_response(json.dumps(result), http_code)
        return resp


class SpecialsBet(Resource):
    def post(self):
        message = request.get_json()
        current_app.logger.info(
            "Received scorepesa specials bet request %r" % message)
        if message:
            status, response = self.api_special_bet(message)
        else:
            status, response = 421, 'Bad request'
        current_app.logger.info(
            "scorepesa specials api response message:: %r" % response)
        return response, status

    def api_special_bet(self, message):
        scorepesa = Scorepesa(current_app.logger)
        #options looks like [GAME,190, 1, 250]
        #game id#pic#gameid#pick ....#amount
        profile_id = message.get('profile_id')
        slips = message.get('slip')
        possible_win = message.get('possible_win')
        app_name = message.get('app_name') if message.get('app_name') else 'API_WEB'
        bet_specials = 2 if message.get('live_bet') == 2 else 2
         
        amount = message.get('stake_amount')
        amount = format(abs(float(amount)))
        #bet_total_odds = message.get('bet_total_odds')
        if len(slips) < 1:
            return 421, "Bet failed, please select a game pick to bet on."
        balance, bonus_balance = scorepesa.get_account_balance(message)
        #bonus_balance = scorepesa.get_bonus_balance(message)
        if amount < float(scorepesa.scorepesa_configs["min_amount"]):
            return 421,'Sorry but your bet amount KES %0.2f is less that '\
                'minimum allowed of KES %s. Please try again. www.scorepesa.co.ke' % \
                (amount, scorepesa.scorepesa_configs["min_amount"])

        if amount < 1:
            return 421, "Scorepesa minimum stake amount is KES %0.2f and above." % \
                (float(scorepesa.scorepesa_configs["min_amount"]))

        if(float(amount) > (float(balance) + float(bonus_balance))):
                return 421, "Cannot place bet as current balance %0.2f, "\
                    "bonus %0.2f is less than minimum stake amount."  % \
                    (float(balance), bonus_balance)

        if len(slips) > int(scorepesa.scorepesa_configs['max_match_bet']):
            return 421, "The maximum number of teams in a multibet is %s."\
                " Kindly revise your selection and try again." % \
                float(scorepesa.scorepesa_configs['max_match_bet'])

        bet_slips = []
        bet_total_odd = 1
        gameid_picklist = []
        betmessage_string = None
        game_id_check_list = []
        try:
            current_app.logger.info("Scorepesa specials RAW API PICK %r" % (slips))
            for bet_slip in slips:
                current_app.logger.info(
                    "Scorepesa specials PICKED VALUES %r" % (bet_slip))
                game_id = scorepesa.get_game_id(bet_slip.get('parent_match_id'))
                parent_match_id = bet_slip.get('parent_match_id')
                outcome_exist = scorepesa.check_outcome_exists(
                    bet_slip.get('parent_match_id'))
                current_app.logger.info(
                    "Scorepesa specials checking outcome if exists %r" 
                    % (outcome_exist))
                if outcome_exist:
                    return 200, "Scorepesa specials Game ID " + game_id + " has "\
                        "expired. Please remove the match and try again."
                current_app.logger.info("Game OK %r" % (bet_slip))
                pick = bet_slip.get('pick_key')
                try:
                    special_bet_value = bet_slip.get('special_bet_value')
                except:
                    special_bet_value = None
                current_app.logger.info(
                    "Scorepesa specials set special bet value %r" 
                    % (special_bet_value))
                gameid_picklist += "#" + str(game_id) + "#" + pick
                sub_type_id = bet_slip.get('sub_type_id', None)
                current_app.logger.info(
                    "scorepesa specials parsing params : %s, %s, %s, %s" 
                    % (game_id, pick, amount, sub_type_id))

                # Check/Validate Odds specials 
                bspecials = ScorepesaSpecials(current_app.logger)
                invalid_slip, response = bspecials.validate_bet_slip_odds(
                    game_id, pick, amount, sub_type_id, special_bet_value)
                current_app.logger.info(
                    "scorepesa specials getting invalid slip %s, %r ", 
                    response, invalid_slip)

                if invalid_slip:
                    return 421, response
                bleague_event_odd = response

                uniquePick = "{0}~{1}".format(game_id, special_bet_value)

                if uniquePick in game_id_check_list:
                    return 421, "Duplicate pick in bet request, please ensure"\
                        " one pick for each market."

                if len(slips) == 1:
                    invalid_single_bet_message = scorepesa.invalid_single_bet_message(
                        profile_id, parent_match_id, amount)
                    if invalid_single_bet_message:
                        return 421, invalid_single_bet_message

                #append betslip meta data for placing bet with
                bet_slips.append({"parent_match_id":
                     bleague_event_odd.get("parent_match_id"),
                    "pick": bleague_event_odd.get("odd_key"),
                     "odd_value": float(bleague_event_odd.get("odd_value")),
                    "sub_type_id": bleague_event_odd.get("sub_type_id"),
                    "special_bet_value": special_bet_value,
                     "live_bet": bet_specials
                     })


                current_app.logger.info("Scorepesa specials Collected Bet SLIP: "\
                    "%r ::%r" % (bet_slips, bleague_event_odd.get("odd_value")))
                bet_total_odd = bet_total_odd * \
                    float(bleague_event_odd.get("odd_value"))
                current_app.logger.info(
                    "Scorepesa specials BET TOTAL ODD :: %r" % bet_total_odd)

                game_id_check_list.append(uniquePick)

        except Exception, e:
            current_app.logger.info(
                "Error fetching match_bet odds: %r :: %r " % (e, profile_id))
            return 421, "Sorry we are unable to create your scorepesa specials"\
                " bet right now, please try again later."

        bet_total_odd = bet_total_odd or 1

        possible_win = float(bet_total_odd) * float(amount)
        invalid_bet_message = scorepesa.invalid_bet_message(profile_id, 
            amount, possible_win)

        if invalid_bet_message:
            return 421, invalid_bet_message

        fullbet_string = ''.join(gameid_picklist)
        betmessage_string = fullbet_string[1:] + "#" + amount
        if possible_win > float(scorepesa.scorepesa_configs['max_bet_possible_win']):
            possible_win = float(scorepesa.scorepesa_configs['max_bet_possible_win'])

        try:
            amount = Decimal(amount)
        except Exception, e:
            current_app.logger.info(
                "scorepesa specials cannot cast amount to decimal: %r " % e)
            amount = 0

        current_app.logger.info(
            'Scorepesa specials READING PROFILE ID: %r' % profile_id)
        bet_message = betmessage_string
        bet_id = scorepesa.place_bet(profile_id, bet_message, amount, 
            bet_total_odd, possible_win, bet_slips,
            live_bet=bet_specials, app=app_name)

        if not bet_id:
            current_app.logger.info(
                'Unable to place api scorepesa specials bet for profile:: %r' % \
                 profile_id)
            return 421, "Sorry we are unable to create your scorepesa specials"\
                " bet right now, please try again later."
        current_app.logger.info('Scorepesa specials SAVED BET ID: %r' % bet_id)
        current_balance, bonus_balance = scorepesa.get_account_balance(message)

        bet_message = 201, "Bet ID %s, %s. Possible win Ksh %0.2f. "\
            "bal is Ksh %0.2f. Bonus bal Ksh %0.2f." % \
            (bet_id, betmessage_string, possible_win, current_balance, 
            bonus_balance)

        #referal bonus adjust after bet
        if str(profile_id) not in \
            scorepesa.scorepesa_bonus_cfgs['referral_test_whitelist'].split(','):
            if scorepesa.scorepesa_configs['award_referral_bonus'] == '1':
                referral_bonus = ReferralBonus(current_app.logger, profile_id)
                referral_bonus.award_referral_bonus_after_bet(
                    profile_id, amount, bet_total_odd)

        return bet_message


class XposeAPI(Resource):

    def __init__(self):
        self.log = current_app.logger
        self.log.info("XposeAPI init()........")
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('token', type=str, required=True, 
            help='Provide valid token')
        self.args = self.parser.parse_args(strict=True)
        self.log.info(
            "incomming XposeAPI request got args.... {0}..".format(self.args,))

    def __del__(self):
        self.log.info("destroy XposeAPI obj....")
        if self.parser:
           self.parser = None
        if self.args:
           self.args =None

    def post(self, reqparam):
        self.log.info(
            "received POST XposeAPI API request ::: {0} ::::".format(reqparam))
        result = self.router(self.args, reqparam)
        self.log.info(
            "returning got POST XposeAPI result as ... {0}".format(result))
        return result

    def router(self, args, reqparam):
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        data = jwt.decode(args['token'], scorepesa_cfgs['encrption_key'], 
            algorithms=['HS256'])
        self.log.info(
            "router extracted XposeAPI request data..... %r" % data)
        result = None
        #routes
        if reqparam == 'refer':
            result = self.process_api_refer_friend(data)
        else:
            self.log.info(
                "oops could not understand ignored....XposeAPI"
                " request data [] {0} []".format(data))
            return result
        return result

    def process_api_refer_friend(self, data):
        try:
            _code = 200
            self.log.info(
                "process api refer a friend request data..... %r" % data)

            scorepesa = Scorepesa(current_app.logger)

            txt_msg = "ACCEPT#{0}".format(data.get("user").get('friend_msisdn'))

            message = {"msisdn": str(data.get("user").get('referrer_msisdn')),
             "message": txt_msg, "profile_id": data.get("user").get('profile_id')}

            text_dict = txt_msg.split("#")

            result, new = scorepesa.process_bonus_request(message, text_dict)

            self.log.info(
                "processed api refer a friend request ... {0}".format(result))
        except JWTError as e:
            self.log.error(
                "Exception on process refer a friend api %r " % e)
            _code = 400
            result = "Invalid token provided."
        resp = make_response(result, _code)
        return resp

