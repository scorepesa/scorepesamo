import json
from flask import request, make_response, render_template, Response, send_from_directory
from flask_restful import Resource, reqparse
from flask import current_app
from decimal import Decimal
import requests
from utils import LocalConfigParser, Helper
from jose.exceptions import JWTError
from jose import jwt
import urllib
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from functools import wraps
import shutil
import os
import os.path
from bcrypt import hashpw, gensalt, checkpw
import re
from Scorepesa import Scorepesa
import re
import random

def get_code():
    return '{:04}'.format(random.randrange(1, 10**3))

def get_network_from_msisdn_prefix(msisdn):
    try:
        regexs = {
              "SAFARICOM": '^(?:254|\+254|0)?(7(?:(?:[12][0-9])|(?:0[0-9])|(9[0-9])|(4[0-9]))[0-9]{6})$',
              "AIRTEL": '^(?:254|\+254|0)?(7(?:(?:[3][0-9])|(?:5[0-6])|(8[5-9]))[0-9]{6})$',
              "ORANGE": '^(?:254|\+254|0)?(77[0-6][0-9]{6})$',
              "EQUITEL": '^(?:254|\+254|0)?(76[34][0-9]{6})$'
            }

        for rgx_key, rgx_val in regexs.iteritems():
           rs = re.match(rgx_val, str(msisdn.strip()))
           if rs:
              operator = rgx_key.upper()
        return operator
    except Exception, e:
        return None
def send_notification(msisdn, outbox_id, network, message):
    url = LocalConfigParser.parse_configs("BULK")["url"]
    try:
        sms_payload = {
            "PhoneNumber":msisdn,
            "ticketNumber":outbox_id,
            "network":network,
            "language":"sw",
            "Text":message,
            }
        current_app.logger.info("Calling URL FOR BET ACTION: (%s, %r) " % (url, sms_payload))
        output = requests.get(url, params=sms_payload, timeout=30)
        current_app.logger.info("Found result from sdp call: (%s) " % (output.text, ))
    except Exception as e:    # This is the correct syntax
        current_app.logger.error("Exception attempting to send "\
            "BULK MPESA message : %r :: %r " % (message, e))
 


def clean_msisdn(msisdn):
    if not msisdn:
        return None
    _msisdn = re.sub(r"\s+", '', msisdn)
    res = re.match('^(?:\+?(?:[1-9]{3})|0)?([0-9]{9})$', _msisdn)
    if res:
      return "254" + res.group(1)
    return None

def _tostr(text):
    import unicodedata
    try:
        text = re.sub(r'[^\x00-\x7F]+',' ', text)
        if type(text) == str:
	    text = unicode(text)
        return unicodedata.normalize('NFKD', text).encode('ascii','ignore')
    except Exception, e:
        return ""

def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.
    """
    current_app.logger.info("scorepesa app received creds....{0}::{1}".format(username, password))
    scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESAAPP")
    passwd=scorepesa_cfgs['api_password']
    user=scorepesa_cfgs['api_username']
    #current_app.logger.info("config creds ...{0}::{1}".format(user, passwd))

    return str(username) == str(user) and str(password) == str(passwd)

def authenticate():
    """Sends a 401 response that enables basic auth"""

    res = json.dumps({"msg": 'Access denied.'})
    return Response(res, 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        current_app.logger.info(" scorepesa app auth request got ...{0}".format(auth))
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

def prepare_match_data(games_result):
    datas = []
    for result in games_result:
        game_id, home_team, away_team,  competition_name, sport_name, category, odds, match_id, parent_match_id, \
            start_time, match_time, sbv, priority, side_bets = result
        odds_dict = {}
        for o in odds.split(','):
            #current_app.logger.info("Matches reading o ==> %s" % o)
            o = _tostr(o)
            sub_type_id = o.split('^')[0]
            o = o.split('^')[1]
            special_bet_value = o.split('#')[0]
            #print "matches ==>", o
            o = o.split('#')[1]
            sub_type = o.split('|')[0]
            real_odd = {o.split('|')[1].split('=')[0] : o.split('|')[1].split('=')[1],
                'special_bet_value':special_bet_value, 'sub_type_id':sub_type_id}
            if sub_type in odds_dict:
                odds_dict[sub_type].append(real_odd)
            else:
                odds_dict[sub_type] = [real_odd]

        datas.append({'game_id':game_id,
             'home_team':_tostr(home_team),
             'competition_name':_tostr(competition_name),
             'sport_name':sport_name,
             'side_bets':side_bets,
             'category':_tostr(category),
             'away_team':_tostr(away_team),
             'odds':odds_dict,
             'match_id':match_id,
             'parent_match_id':parent_match_id,
             'start_time':start_time.strftime("%Y-%m-%d %H:%M"),
             'match_time':match_time,
             'priority':priority})
    return datas

class AndroidBalance(Resource):
    def params(self):
        parser = reqparse.RequestParser()
        parser.add_argument('token', type=str)

        return parser.parse_args()

    def post(self):
        data = self.params()
        helper = Helper(current_app.logger)
        profile_id = helper.validate_token(data.get('token')) 
        if not profile_id:
            message = {"status":401, "error":{"status":401, "message":"Unauthorized"}}
        else:
            scorepesa = Scorepesa(current_app.logger)
            balance, bonus = scorepesa.get_account_balance({'profile_id':profile_id})
            message = {"status":200, "data":{"balance":balance, 'bonus':bonus, 'points':0}}
        resp = make_response(json.dumps(message), 200)
        return resp 

class BetDetails(Resource):

    def params(self):
        parser = reqparse.RequestParser()
        parser.add_argument('token', type=str)
        parser.add_argument('bet_id', type=int)
        parser.add_argument('limit', type=int)
        parser.add_argument('page', type=int)
        return parser.parse_args()

    def post(self):
        data = self.params()
        helper = Helper(current_app.logger)
        bet_data, slip_data = helper.get_bet_details(data.get('token'), data.get('bet_id'), data.get('page'), data.get('limit'))
        datas = []
        created, bet_id, total_matches, jackpot_bet_id, total_odd, bet_message, bet_amount, possible_win, status = bet_data
        bet_details = {'created':created, 'bet_id':bet_id, 
                    'total_matches':total_matches, 'jackpot_bet_id':jackpot_bet_id,
                    'total_odd':float(total_odd), 'bet_message':bet_message, 
                    'possible_win':float(possible_win), 'status':status,
                    'bet_amount':float(bet_amount)}
        for slip in slip_data:
            
            current_app.logger.info("Slip %r" % slip)
            created, bet_id, sub_type_id, odd_value, bet_amount, possible_win,status, win, game_id, start_time, away_team, home_team, bet_pick, bet_type, winning_outcome  = slip
            slip_value = {
                'created':created, 'bet_id':bet_id, 'sub_type_id':sub_type_id, 
                'odd_value':float(odd_value), 'bet_amount':float(bet_amount), 
                'possible_win':float(possible_win),  'status':status, 'win':win, 
                'game_id':game_id, 'start_time':start_time, 
                'away_team':away_team, 'home_team':home_team, 'bet_type':bet_type,
                'bet_pick':bet_pick, 'winning_outcome':winning_outcome}
            datas.append(slip_value)
        result = {'data':datas, 'meta':{'bet_info':bet_details}}
        resp = make_response(json.dumps(result), 200)
        return resp

class MyBets(Resource):

    def params(self):
        parser = reqparse.RequestParser()
        parser.add_argument('token', type=str)
        parser.add_argument('limit', type=int)
        parser.add_argument('page', type=int)
        return parser.parse_args()

    def post(self):
        data = self.params()
        helper = Helper(current_app.logger)
        data = helper.get_mybets(data.get('token'), data.get('page'), data.get('limit'))
        datas = []
        for d in data:
            created, bet_id, total_matches, jackpot_bet_id, total_odd, bet_message, bet_amount, possible_win, status = d
            value = {'created':created, 'bet_id':bet_id, 
                    'total_matches':total_matches, 'jackpot_bet_id':jackpot_bet_id,
                    'total_odd':float(total_odd), 'bet_message':bet_message, 
                    'possible_win':float(possible_win), 'status':status,
                    'bet_amount':float(bet_amount)}
            datas.append(value)
        resp = make_response(json.dumps(datas), 200)
        return resp


class Login(Resource):
    def post(self):
        response = self.process_response()
        resp = make_response(json.dumps(response), response['status'])
        return resp 

    def params(self):
        parser = reqparse.RequestParser()
        parser.add_argument('msisdn', type=str)
        parser.add_argument('password', type=str)
        parser.add_argument('remember', type=int)

        return parser.parse_args()


    def process_response(self):
	data = self.params()
        current_app.logger.info("Login requesrt eceived: %r" % data)
        msisdn =  clean_msisdn(data.get('msisdn'))
        if not msisdn:
            return {"status":401, "message":"Invalid username and or password", 'token':'' }
        password = data.get("password")
        if not password:
            return {"status":401, "message":"Invalid username and or password", 'token':''}
        helper = Helper(current_app.logger)
        profile_data = helper.get_profile_data(msisdn) 
        if not profile_data:
            return {'status':401, "message":"Invalid username and or password", 'token':''}
        real_pass = profile_data[1];
        if not checkpw(password, real_pass):
            return {"status":401, "message":"Invalid username and or password", 'token':''}
        token = helper.create_hash_token( msisdn, profile_data[0], remember=data.get('remember'))
        if not token:
            return {"status":401, "message":"Could not create token", 'token':''}
         
        return {"status":200, "user":{'token':token, 
               'profile_id':str(profile_data[0]), 
               'balance':float(profile_data[3]),
               'msisdn':msisdn,
               'bonus':float(profile_data[4])}}

class Sports(Resource):
    def get(self):
        helper = Helper(current_app.logger)
        sports = helper.get_active_sports()
        datas = []
        for sport in sports:
            sport_id, sport_name = sport
            datas.append({"sport_name":sport_name, 'sport_id':sport_id})
        return make_response(json.dumps(datas), 200)

    def params(self):
        parser = reqparse.RequestParser()
        parser.add_argument('id', type=int)
        parser.add_argument('limit', type=int)
        parser.add_argument('page', type=int)

        return parser.parse_args()

    def post(self):
        data = self.params()
        helper = Helper(current_app.logger)
        sport_id = data.get('id')
        page = data.get('page', 1)
        limit = data.get('limit', 10)
        categories = helper.get_active_categories(sport_id, page, limit)
        datas = []
        for cat in categories:
            category_id, category_name = cat
            cat_data = {'category_id':category_id, 
                 'category_name':category_name.decode('unicode_escape').encode('utf-8')}
            competitions = helper.get_competitions(category_id)
            c_datas = []
            for c in competitions:
                 competition_id, competition_name = c
                 c_datas.append({'competition_id':competition_id, 
                     'competition_name':competition_name.decode('unicode_escape').encode('utf-8')})
            cat_data['competitions'] = c_datas

            datas.append(cat_data)

        return make_response(json.dumps(datas), 200)

class CompetitionSport(Resource):

    def params(self):
        parser = reqparse.RequestParser()
        parser.add_argument('id', type=int)
        parser.add_argument('limit', type=int)
        parser.add_argument('page', type=int)

        return parser.parse_args()

    def post(self):
        data = self.params()
        helper = Helper(current_app.logger)
        limit = data.get('limit', 10) or 10
        page = data.get('page', 1) or 1
        competition_id = data.get('id', None)
        games_result = helper.get_competition_matches(competition_id, page, limit)
        datas =  prepare_match_data(games_result)
        return make_response(json.dumps(datas), 200)

class JackpotAndroidMatches(Resource):
    def params(self):
        pass

    def get(self):
        helper = Helper(current_app.logger)
        games, jp_result = helper.get_jackpot_matches()
        datas = []
        for result in games:
            pos, sub_type_id, odds, game_id, match_id, start_time,\
                parent_match_id, away_team, home_team = result

            datas.append({'pos':pos, 'sub_type_id':sub_type_id, 'odds_keys':odds,
                'game_id':game_id, 'match_id':match_id, 'start_time':start_time.strftime("%Y-%m-%d %H:%M"),
                'parent_match_id':parent_match_id, 'away_team':away_team,
                'home_team':home_team})
        resp = {
		"data":datas, 
                'meta':{'jackpot_event_id':jp_result[0], 
			'jackpot_type':jp_result[1], 
			'bet_amount':float(jp_result[2]), 
			'jackpot_amount':float(jp_result[3]), 
			'total_games':jp_result[4],
                        'name':'Correct Score Jackpot',
                        'type':'correctscore'
		      }
               }
        return make_response(json.dumps(resp), 200)

class AndroidCode(Resource):
    def params(self):
        parser = reqparse.RequestParser()
        parser.add_argument('mobile', type=str)
        return parser.parse_args()

    def post(self):
        helper = Helper(current_app.logger)
        data = self.params()
        current_app.logger.info("Android code request with data => %r" % data)
        msisdn = clean_msisdn(data.get('mobile'))
        network = get_network_from_msisdn_prefix(msisdn)
        helper = Helper(current_app.logger)
        code, profile_id = helper.get_existing_code(msisdn)
        if code:
            profile_data = helper.get_profile_data(msisdn)
            token = helper.create_hash_token( msisdn, profile_data[0], remember=0)

            message = "Namba yako ya uthibitisho wa akaunti yako ya Scorepesa"\
               " ni %s. Ingiza namba kwenye tovuti kukamilisha mchakato huu" % (code, )
            outbox_id = helper.create_outbox(profile_id, msisdn, network, message)
            send_notification(msisdn, outbox_id, network, message)
            message = {'success':{'status':200, 'id':profile_id, 
                "message": "Verification code has been sent to your phone", 'code':code}, 'token':token}
        else:
            message = {'error':{'status':401, "message":"Verification code could not be resent. Kindly check your mobile number and try again"}}
        return make_response(json.dumps(message), 200)



class AndroidVerify(Resource):
    def params(self):
        parser = reqparse.RequestParser()
        parser.add_argument('mobile', type=str)
        parser.add_argument('code', type=str)
        return parser.parse_args()

    def post(self):
        helper = Helper(current_app.logger)
        data = self.params()
        msisdn = clean_msisdn(data.get('mobile'))
        code = data.get('code')
        ok = helper.verify_code(msisdn, code)
        if ok:
             profile_data = helper.get_profile_data(msisdn)
             token = helper.create_hash_token( msisdn, profile_data[0], remember=0)
        if ok and token:
            message = {'success':{'status':201, "message": "Verification code has been sent to your phone"}, 'token':token}
        else:
            message = {'error':{'status':401, "message":"Verification failed. Kindly check code and try again or click resend code to receive new code"}}
        return make_response(json.dumps(message), 200)

class SignupAndroid(Resource):
    def params(self):
        parser = reqparse.RequestParser()
        parser.add_argument('password', type=str)
        parser.add_argument('msisdn', type=str)
        parser.add_argument('mobile', type=str)
        parser.add_argument('token', type=str)
        parser.add_argument('password', type=str)
        return parser.parse_args()

    def generate_pass_str(self, password):
        salt = gensalt()
        password_hashed = hashpw(password, salt)
        return password_hashed

    def post(self):
        data = self.params()
        msisdn = data.get('msisdn', 0)
        if not msisdn or msisdn == 0:
            msisdn = data.get('mobile')

        if not msisdn or msisdn==0:
            message = {"success":{"status":400, "message":"Invalid phone number"}}
            return make_response(json.dumps(message), 200)
        scorepesa = Scorepesa(current_app.logger)
        profile_id, new = scorepesa.create_profile({"msisdn":msisdn}, 0)
        helper = Helper(current_app.logger)
        password_set = False
        if new:
            code = get_code()
            password = None
            #self.generate_pass_str(data.get('password'))
            message = "Namba yako ya uthibitisho wa akaunti yako ya Scorepesa"\
               " ni %s. Ingiza namba kwenye tovuti kukamilisha mchakato huu" % (code, )
            network = get_network_from_msisdn_prefix(msisdn)
            helper.create_profile_settings(profile_id, code, password)
            outbox_id = helper.create_outbox(profile_id, msisdn, network, message)
            send_notification(msisdn, outbox_id, network, message)
        elif data.get('password'):
            password = self.generate_pass_str(data.get('password'))
            password_set = helper.set_password(profile_id, password)

        if profile_id > 0 and new:
            message = {"success":{"status":201, "message":"Account created success"}}
        elif password_set:
            message = {"message": "Account password set successfully", "status":201}
        elif profile_id == -1:
            message = {"success":{
		"status":500, 
		"message":"Encountered problem trying to create profile, Please try again later"
		}
	    }
        else:
            message = {"success":{"status":200, 
		"message":"Account alreay exists, proceed to login and play with scorepesa.com"}}
        
        return make_response(json.dumps(message), 200)
                

class Matches(Resource):

    def get(self):
        helper = Helper(current_app.logger)
        tab = request.args.get('tab', 'Mechi Kali')
        limit =request.args.get('limit', 7) or 7
        page = request.args.get('page', 1) or 1 
        match_id = request.args.get('id', None) 
        games_result = helper.get_matches(tab, page, limit, match_id)
        datas =  prepare_match_data(games_result)
        return make_response(json.dumps(datas), 200)


class ScorepesaAppVersion(Resource):
    def get(self):
        current_app.logger.info("Scorepesa mobile app check version request....")
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESAAPP")
        try:
            http_code = 200
            helper = Helper(current_app.logger)
            result = helper.scorepesa_app_check_current_version()
            current_app.logger.info("scorepesa app api version response .....{0}".format(result))
        except Exception as e:
            current_app.logger.error("Exception on version check ..... %r " % e)
            http_code = 400
            result = "Invalid request."
        resp = make_response(json.dumps(result), http_code)
        return resp

    def post(self):
        #version_number
        data = request.get_json()
        current_app.logger.info("Scorepesa mobile app change version api args %r" % data)
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESAAPP")
        try:
            http_code = 200
            current_app.logger.info("scorepesa app api change version data %r" % data)
            helper = Helper(current_app.logger)
            message = {"vnum": data.get('version_number')}
            result = helper.scorepesa_app_update_version(message)
            current_app.logger.info("mobile change version response... {0}".format(result))
            if not result:
               http_code = 500
               result='ERROR'
        except JWTError as e:
            current_app.logger.error("Exception on version check ..... %r " % e)
            http_code = 400
            result = "Invalid request."
        resp = make_response(json.dumps(result), http_code)
        return resp

class ScorepesaAppRegDevice(Resource):
    def post(self):
        #user_id,device_id,fcm_token,update_token=True/False
        data = request.get_json()
        current_app.logger.info("Scorepesa mobile app device register api args %r" % data)
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESAAPP")
        scorepesa_bonus_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        try:
            http_code = 200
            current_app.logger.info("device registeration data..... %r" % data)
            helper = Helper(current_app.logger)
            result = helper.scorepesa_app_register_device(data)
            current_app.logger.info("mobile device registration response... {0}".format(result))
            if not result:
               http_code = 500
               result='ERROR'       
            if result and data.get("update_token") is False:
                #award bonus
                if (helper.award_bonus_to_profile(data)):
                    #sent app notification/sms
                    data['msg']="Congratulations! you have been awarded a bonus worth KES {0}. Now win big with.".format(scorepesa_bonus_cfgs['registration_bunus_amount'])
                    data['sc']="101010"
                    helper.push_notification(data)
                    helper.send_message(data)                
        except JWTError as e:
            current_app.logger.error("Exception on device registration.... %r " % e)
            http_code = 400
            result = "Invalid request."
        resp = make_response(json.dumps(result), http_code)
        return resp

class ScorepesaAppDownload(Resource):
    def get(self, ops='android'):
        #args = request.args
        current_app.logger.info("download request....{0}".format(ops))
        if not ops:
            return  "Bad request", 400
        scorepesa_app_cfgs = LocalConfigParser.parse_configs("SCOREPESAAPP")
        directory = scorepesa_app_cfgs['scorepesa_app_directory']
        if ops == "android": #args.get("os") == "android":
           android_apk_name = scorepesa_app_cfgs['android_app_name']
           file_ext = scorepesa_app_cfgs['android_app_extension']
        else:
           return make_response("Sorry request failed.", 200)
        filename='{0}.{1}'.format(android_apk_name, file_ext)
        #check if requested file exists if not pull first
        current_app.logger.info("got download details....{0}:::{1}".format(directory, filename))

        fpath = "{0}/{1}".format(directory, filename)
        if not os.path.isfile(fpath):
            self.pull_apk_for_download_first()
        result = send_from_directory(directory, filename, as_attachment=True)
        '''
         TODO:

            record downloads count
        '''
        
        return result

    def post(self, ops='android'):
         try:
            current_app.logger.info("download request....{0}".format(ops))
            if not ops:
                return  "Bad request", 400
            scorepesa_app_cfgs = LocalConfigParser.parse_configs("SCOREPESAAPP")
            app_directory = scorepesa_app_cfgs['scorepesa_app_directory']

            if ops == "android": #args.get("os") == "android":
                android_apk_name = scorepesa_app_cfgs['android_app_name']
                file_ext = scorepesa_app_cfgs['android_app_extension']
            else:
                return make_response("Sorry request failed.", 200)
            filename='{0}.{1}'.format(android_apk_name, file_ext)
            url = scorepesa_app_cfgs['app_download_url'] # user provides url in query string

            try:
                #rotate any existing app apk first for new one
                shutil.move("{0}/{1}".format(app_directory, filename), "{0}/{1}.{2}".format(app_directory, filename, datetime.now().strftime('%Y%m%d%H%M%S')))
            except Exception, e:
                 current_app.logger.error("Ignore Exception on rotating app apk.... %r " % e)
 
            r = requests.get(url, stream=True)
            with open(filename, 'wb') as f:
                shutil.copyfileobj(r.raw, f) 

            try:
                shutil.move("{0}".format(filename), "{0}/{1}".format(app_directory, filename))
            except Exception, e:
                current_app.logger.error("Exception on moving to app apk folder.... %r " % e)
                #remove the copied file to re-copy to avoid confusion on rotation
                try:
                   os.unlink(filename)    
                except Exception, e:
                   current_app.logger.error("Ignoring Exception on removing copied to app apk.... %r " % e)
                   pass
                return False
            return True
         except Exception, e:
            current_app.logger.error("Exception on copying remote(web1) app apk to distribute for user download api.... %r " % e)
            return False

    def pull_apk_for_download_first(self):
          url = "http://localhost:8008/scorepesaApp/android"
          response = requests.request("POST", url)


class Xposed(Resource):

    def __init__(self):
        self.log = current_app.logger
        self.log.info("mobile apis Xposed init()........")
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('token', type=str, required=True, help='Provide valid token')
        self.args = self.parser.parse_args(strict=True)
        self.log.info("mobile apis incomming Xposed request got args.... {0}..".format(self.args,))

    def __del__(self):
        self.log.info("mobile apis destroy Xposed obj....")
        if self.parser:
           self.parser = None
        if self.args:
           self.args =None

    def get(self, reqparam):
        self.log.info("mobile apis received GET Xposed API request ::: {0} ::::".format(reqparam))
        result = self.router(self.args, reqparam)
        self.log.info("mobile apis returning got POST Xposed result as ... {0}".format(result))
        return result

    def router(self, args, reqparam):
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        data = jwt.decode(args['token'], scorepesa_cfgs['encrption_key'], algorithms=['HS256'])
        self.log.info("mobile apis router extracted Xposed request data..... %r" % data)
        result = None
        #routes
        if reqparam == 'outbox':
            result = self.process_api_outbox(data)
        else:
            self.log.info("mobile apis oops could not understand ignored....Xposed request data [] {0} []".format(data))
            return result
        return result

    def process_api_outbox(self, data):
        try:
            _code = 200
            self.log.info("process api outbox request data..... %r" % data)

            helper = Helper(current_app.logger)

            message = {"msisdn": data.get("user").get('referrer_msisdn'), "profile_id": data.get("user").get('profile_id')}

            result = helper.get_outbox_messages(message)

            self.log.info("mobile api got outbox messages response returning ...")
        except JWTError as e:
            self.log.error("Exception on fetch outbox messages api %r " % e)
            _code = 400
            result = "Invalid token provided."
        resp = make_response(result, _code)
        return resp
