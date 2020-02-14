import ConfigParser
import os
from db.schema import Db, ProfileBonu, Transaction, BonusTrx
from datetime import datetime, timedelta
from sqlalchemy.sql import text as sql_text
from decimal import Decimal
from sqlalchemy.exc import IntegrityError
import urllib
import requests
import json
import re

class LocalConfigParser(object):

	def __init__():
		pass

	@staticmethod
	def parse_configs(section = None):
		try:
			cur_dir = os.path.dirname(os.path.realpath(__file__))
			filename = os.path.join(cur_dir, 'configs/configs.ini')
                        #filename = os.path.join(cur_dir, 'configs/configs_local.ini')
			cparser = ConfigParser.ConfigParser()
			cparser.read(filename)
			config_dic = {}
			section = section or 'DB'
			options = cparser.options(section)
			for option in options:
				try:
					config_dic[option] = cparser.get(section, option)
					if config_dic[option] == -1:
						print "Reading config Invalid section ", option, section
				except:
					print {"Exception": option}
					config_dic[option] = None
			return config_dic
		except Exception as e:
		    print {"blunder opening configuration ": e}
		    return {}


class Helper():

    def __init__(self, logger, connection=None):
         self.logger=logger
         self.db = None
         self.db_configs = LocalConfigParser.parse_configs('DB')
         self.get_db_session()
         self.connection=connection
         #no passed connection create
         if self.db and self.connection is None:
            self.connection = self.db.engine.connect()
         self.scorepesa_configs = LocalConfigParser.parse_configs('SCOREPESA')
         self.logger.info("helper class init() complete......")

    def __del__(self):
         self.logger.info("helper class cleanup job......")
         self.logger=None
         self.db=None
         self.db_configs=None
         self.scorepesa_configs=None
         if self.connection:
             self.connection=None

    def get_db_session(self):
        self.db = Db(self.db_configs['host'], self.db_configs['db_name'],
            self.db_configs['username'],
            self.db_configs['password'], socket=self.db_configs['socket'], port=self.db_configs['port'])

        return self.db

    def clean_msisdn(self, msisdn):
        if not msisdn:
            return None
        _msisdn = re.sub(r"\s+", '', msisdn)
        res = re.match('^(?:\+?(?:[1-9]{3})|0)?([0-9]{9})$', _msisdn)
        if res:
           return "255" + res.group(1)
        return None

    def create_speed_dial_history(self, data):
         try:
            data['msisdn'] = self.get_msisdn_for_profile(data.get("profile_id"))
            self.logger.info("creating speeddial history.... {0}".format(data))
            sql="INSERT INTO speed_dial_history (profile_id, msisdn, source, header_info, created) VALUES(:profile_id, :msisdn, :source, :header_info, :created)"
            params = {"profile_id": data.get("profile_id"), "msisdn": data.get("msisdn"), "source":data.get("source"), "header_info": data.get("header_data"), "created":datetime.now()}
            self.connection.execute(sql_text(sql), params)
            #create speed dial profile
            self.create_speed_dial_profile(data)
            return True
         except Exception, exk:
            self.logger.error("Exception on create speed dial history ::: {0}".format(exk))
            return False

    def create_speed_dial_profile(self, data):
         try:
            data['msisdn'] = self.get_msisdn_for_profile(data.get("profile_id"))
            self.logger.info("creating speeddial profile.... {0}".format(data))
            sql="INSERT IGNORE INTO speed_dial_profile (profile_id, msisdn, date_created) VALUES(:profile_id, :msisdn, :created)"
            params = {"profile_id": data.get("profile_id"), "msisdn": data.get("msisdn"), "created":datetime.now()}
            self.connection.execute(sql_text(sql), params)
            return True
         except Exception, exk:
            self.logger.error("Exception on create speed dial profile ::: {0}".format(exk))
            return False

    def create_outbox(self, profile_id, msisdn, network, message):
        sql = "insert into outbox set outbox_id=null, shortcode=null, "\
            " network=:net, profile_id=:pid, linkid=null, date_created=now(),"\
            " date_sent=now(), retry_status=0, modified=now(), text=:message, "\
            " msisdn=:msisdn, sdp_id=null"     
        result = self.connection.execute(sql_text(sql), {'pid': profile_id, 
            'net':network, 'msisdn':msisdn, 'message':message})
        insert_id = result.lastrowid if result else ''
        return insert_id

    def get_existing_code(self, msisdn):
        sql = "select verification_code, profile.profile_id from profile_settings inner join "\
            " profile using(profile_id) where profile.msisdn=:msisdn"
        result =  self.connection.execute(sql_text(sql), {"msisdn":msisdn}).fetchone()
        if result and result[0]:
            return result[0], result[1]
        return None, None
        
    def verify_code(self, msisdn, code):
        sql = "select verification_code from profile_settings inner join profile using(profile_id)"\
            " where msisdn=:msisdn and verification_code = :code"
        result = self.connection.execute(sql_text(sql), {"msisdn":msisdn, 'code':code}).fetchone()
        return result and result[0]
    
    def set_password(self, profile_id, password):
        sql = "update profile_settings set password = :password where profile_id=:profile_id limit 1"
        params = {'profile_id':profile_id, 'password':password}
        result = self.connection.execute(sql_text(sql), params)
        return True

    def create_profile_settings(self, profile_id, code, password):
        sql = " insert into profile_settings set  profile_setting_id=null, "\
            " profile_id=:pfid, status=1, verification_code=:code, created_at=now(),"\
            " updated_at=now(), password=null"
        result = self.connection.execute(sql_text(sql), {'pfid': profile_id, 
            'code':code, 'pass':password})
        return result.lastrowid if result else None
         

    def get_profile_data(self, msisdn):
        query = "select p.profile_id, ps.password, p.status, pb.balance, pb.bonus_balance from profile p "\
            "inner join profile_settings ps using(profile_id) left join "\
            "profile_balance pb on pb.profile_id = p.profile_id where msisdn=:msisdn"
        result = self.connection.execute(sql_text(query), {'msisdn': msisdn}).fetchone()
        if result and result[0]:
            # Just in case you dont have a mobile profile we do one for you, ha!
            sql = "insert ignore into mobile_app_profile (id,profile_id, msisdn,"\
                " device_id, token_id, status, app, date_created, modified) values "\
                " (null, :profile_id, :msisdn, null, null, 1, 'Scorepesa', now(), now())"
            resp = self.connection.execute(sql_text(sql), {'msisdn': msisdn, 
                'profile_id':result[0]})
            return result
        return None

    def get_competitions(self, category_id):
        sql = "select competition_id, competition_name from competition "\
            " where category_id=:cat_id order by competition_name asc"
        
        params = {'cat_id':category_id}
        _result = self.connection.execute(sql_text(sql), params).fetchall()
        return _result

    def get_active_categories(self, sport_id, page=1, limit=10):
        offset = (int(page)-1 )*int(limit)
        sql = "select  c.category_id, c.category_name from category c "\
            " where  c.sport_id=:sport_id order by c.category_name asc "\
            " limit %s, %s " % (offset, limit)
        params = {'sport_id':sport_id}
        _result = self.connection.execute(sql_text(sql), params).fetchall()
        return _result

    def get_active_sports(self):
        sql = "select sport_id, sport_name from sport where sport_id in (79, 85) "
        _result = self.connection.execute(sql_text(sql)).fetchall()
        return _result

    def get_jackpot_matches(self):
        jp_sql= "SELECT jackpot_event_id, jackpot_type, bet_amount, jackpot_amount, total_games FROM jackpot_event WHERE "\
            "jackpot_type = :st and status='ACTIVE' ORDER BY 1 DESC LIMIT 1"
        jp_result = self.connection.execute(sql_text(jp_sql),{'st':2}).fetchone()
        games = []
        if jp_result:
            sql = "select j.game_order as pos, e.sub_type_id, "\
                "group_concat(odd_key) as correctscore, "\
                "m.game_id, m.match_id, m.start_time, m.parent_match_id, m.away_team, "\
                "m.home_team from jackpot_match j inner join `match` m on "\
                "m.parent_match_id = j.parent_match_id inner join event_odd e on "\
                "e.parent_match_id = m.parent_match_id where j.jackpot_event_id=:jpid "\
                " and e.sub_type_id=:sbid group by j.parent_match_id order by pos"

            games = self.connection.execute(sql_text(sql), {'sbid':45, 'jpid':jp_result[0]}).fetchall()
        return games, jp_result


    
    def get_competition_matches(self, competition_id, page=1, limit=10):
        order_by = " m.start_time asc, m.priority desc, c.priority desc "
        offset = (int(page)-1)*int(limit) 
        sub_type_filter = " and o.sub_type_id = 1 "

        games_sql = "select m.game_id, m.home_team, m.away_team, c.competition_name, s.sport_name, c.category, group_concat("\
            " concat(o.sub_type_id, '^', o.special_bet_value, '#', ot.name, '|', o.odd_key, '=', o. odd_value)) as odds, "\
            " m.match_id, m.parent_match_id,  m.start_time, date_format(m.start_time, '%s')match_time, "\
            " o.special_bet_value sbv, m.priority, (select count(distinct sub_type_id) from "\
            " event_odd where parent_match_id=m.parent_match_id) side_bets from `match` m inner join event_odd o on "\
            " m.parent_match_id=o.parent_match_id inner join odd_type ot on "\
            " (ot.sub_type_id = o.sub_type_id and ot.parent_match_id = o.parent_match_id) "\
            " inner join competition c on "\
            " c.competition_id=m.competition_id inner join sport s on s.sport_id=c.sport_id "\
            " where m.status=:status and m.start_time > now() "\
            " and c.competition_id=:comp_id %s group by m.parent_match_id "\
            " having odds is not null order by %s limit %s,%s " % ('%H:%i', sub_type_filter, order_by, offset, limit,)

        params = {'status':1, 'comp_id':competition_id}
        games_result = self.connection.execute(sql_text(games_sql), params).fetchall()
        return games_result
        
    def get_matches(self,tab='Mechi Kali', page=1, limit=10, match_id=None):
        tab = tab if tab in ['Mechi Kali', 'Zijazo', 'Kesho'] else 'Mechi Kali'
        start_date = " now() "
        order_by = " m.priority desc, c.priority desc, m.start_time asc "
        if tab == 'Zijazo':
            order_by = " m.start_time asc, m.priority desc, c.priority desc "
        elif tab == 'Kesho':
            order_by = " m.start_time asc, m.priority desc, c.priority " 
            start_date = " now() + interval 1 day "
        offset = (int(page)-1)*int(limit)
        if match_id is not None:
            try:
            	sub_type_filter = " and m.match_id = %d " % (int(match_id),)
            except:
                sub_type_filter = " and o.sub_type_id = 1 "
        else:
            sub_type_filter = " and o.sub_type_id = 1 "


        games_sql = "select m.game_id, m.home_team, m.away_team, c.competition_name, s.sport_name, c.category, group_concat("\
            " concat(o.sub_type_id, '^', o.special_bet_value, '#', ot.name, '|', o.odd_key, '=', o. odd_value)) as odds, "\
            " m.match_id, m.parent_match_id,  m.start_time, date_format(m.start_time, '%s')match_time, "\
            " o.special_bet_value sbv, m.priority, (select count(distinct sub_type_id) from "\
            " event_odd where parent_match_id=m.parent_match_id) side_bets from `match` m inner join event_odd o on "\
            " m.parent_match_id=o.parent_match_id inner join odd_type ot on "\
            " (ot.sub_type_id = o.sub_type_id and ot.parent_match_id = o.parent_match_id) "\
            " inner join competition c on "\
            " c.competition_id=m.competition_id inner join sport s on s.sport_id=c.sport_id "\
            " where m.status=:status and m.start_time > %s "\
            " and c.sport_id=:sport_id %s group by m.parent_match_id"\
            " having odds is not null order by %s limit %s,%s " % ('%H:%i',start_date, sub_type_filter, order_by, offset, limit,)
        params = {'status':1, 'sport_id':79}
        games_result = self.connection.execute(sql_text(games_sql), params).fetchall()
        return games_result

    def create_hash_token(self, msisdn, profile_id, remember=False):
        import hashlib
        dk = hashlib.sha224("%s-%s-%s" % (msisdn, 'I got to let the avalanche REST!', profile_id))
        token = dk.hexdigest()
        expiry = ' now() + interval 1 day ' if remember else ' now() + interval 30 day ' 
        sql = "insert into android_auth set id=null, profile_id=:pfid, token=:token, expiry=%s" % expiry
        result = self.connection.execute(sql_text(sql), 
		{'pfid': profile_id, 'token':token})

        insert_id = result.lastrowid
        return token if insert_id else None 
        
    def validate_token(self, token):
        sql = "select profile_id from android_auth a "\
           " where token = :token and expiry > now() order by a.created desc limit 1"
        result =  self.connection.execute(sql_text(sql), {'token':token}).fetchone()
        if result:
            return result[0]
        return False

    def get_bet_details(self, token, bet_id, page=1, limit=10):
        profile_id = self.validate_token(token)
        if not profile_id:
            return []
        offset = (page-1)*limit
        sql = "select date_format(b.created, '%s'), b.bet_id, (select count(*) from bet_slip "\
            " where bet_id=b.bet_id) as total_matches, jackpot_bet_id, total_odd,"\
            " bet_message, bet_amount, possible_win, b.status from bet b left join jackpot_bet j "\
            " on j.bet_id = b.bet_id where b.bet_id=:bet_id" % ('%Y-%m-%d %H:%i',)
        self.logger.info("%s, %s" % (sql, bet_id))
        result =  self.connection.execute(sql_text(sql), {'bet_id':bet_id}).fetchone()
        slip_sql = "select date_format(bs.created, '%y-%m-%d %H-%i'), bs.bet_id, "\
            " bs.sub_type_id, bs.odd_value, b.bet_amount, b.possible_win, bs.status, "\
	    " bs.win, m.game_id, date_format(m.start_time, '%y-%m-%d %H-%i'), "\
	    " m.away_team, m.home_team, bs.bet_pick, if (bs.total_games > 1, 'Multibet', 'Single'),"\
	    " (select group_concat(winning_outcome) from outcome o where  "\
	    " o.parent_match_id = bs.parent_match_id and o.sub_type_id = bs.sub_type_id"\
	    " and o.special_bet_value = bs.special_bet_value and o.is_winning_outcome=1)winning_outcome"\
            " from bet_slip bs inner join bet b using(bet_id) inner join `match` m "\
	    " on m.parent_match_id = bs.parent_match_id where bet_id=:bet_id"
	slip_result = self.connection.execute(sql_text(slip_sql), {'bet_id':bet_id}).fetchall()
        return result, slip_result

    def get_mybets(self, token, page=1, limit=10):
        profile_id = self.validate_token(token)
        if not profile_id:
            return []
        offset = (page-1)*limit
        sql = "select date_format(b.created, '%s'), b.bet_id, (select count(*) from bet_slip "\
            " where bet_id=b.bet_id) as total_matches, jackpot_bet_id, total_odd,"\
            " bet_message, bet_amount, possible_win, b.status from bet b left join jackpot_bet j "\
            " on j.bet_id = b.bet_id where profile_id=:pfid order by b.created "\
            " desc limit %s, %s" % ('%Y-%m-%d %H:%i', offset, limit)
        self.logger.info("%s, %s" % (sql, profile_id))
        result =  self.connection.execute(sql_text(sql), {'pfid':profile_id}).fetchall()
        return result

    def get_msisdn_for_profile(self, profile_id):
          msisdn = None
          sqlQ = "select msisdn from profile where profile_id=:profile"
          result = self.connection.execute(sql_text(sqlQ), {'profile': profile_id}).fetchone()
          if result and result[0]:
             msisdn = result[0]
          self.logger.info("got profile {0} for msisdn {1} :: sql {2} :: result :: {3}".format(profile_id, msisdn, sqlQ, result))
          return msisdn

    def scorepesa_app_check_current_version(self, app='Scorepesa'):
          current_version = 1.0
          Q = "select current_version from mobile_app_version where app_name=:app"
          result = self.connection.execute(sql_text(Q), {'app': app}).fetchone()
          if result and result[0]:
              current_version = result[0]
          self.logger.info("returning current app version ...... {0}".format(current_version))
          return {"version":current_version}

    '''
      Required token,device_id,profile_id
    '''
    def scorepesa_app_register_device(self, data):
         try:
            data['msisdn'] = self.get_msisdn_for_profile(data.get("user_id"))
            self.logger.info("update scorepesa app version data.... {0}".format(data))
            isql="INSERT INTO mobile_app_profile (profile_id, msisdn, device_id, "\
                "token_id, date_created) VALUES(:profile_id, :msisdn, :device_id,"\
                " :token_id, :date_created) ON DUPLICATE KEY UPDATE token_id=:token_id,"\
                " profile_id=:profile_id, msisdn=:msisdn, device_id=:device_id"

            params = {"profile_id": data.get("user_id"), 
                "msisdn": data.get("msisdn"), "device_id": data.get("device_id"), 
                "token_id": data.get("fcm_token"), "date_created":datetime.now()}

            self.connection.execute(sql_text(isql), params)
            return {"registered":True}
         except Exception, exk:
            self.logger.error("Exception on create mobile app profile ::: {0}".format(exk))
            return {"registered":False}

    def scorepesa_app_update_version(self, data):
         try:
            self.logger.info("update scorepesa app version data.... {0}".format(data))
            vsql="INSERT INTO mobile_app_version (current_version, created_at)"\
                " VALUES(:current_version, :created_at) ON DUPLICATE KEY UPDATE"\
                " current_version=:current_version"
            params = {"current_version": data.get("vnum"), "created_at": datetime.now()}
            self.connection.execute(sql_text(vsql), params)
            return {"updated":True}
         except Exception, exk:
            self.logger.error("Exception on create mobile app version ::: {0}".format(exk))
            return {"updated":False}

    def push_notification(self, data, push_title='Scorepesa!'):
        try:
            self.logger.info("push notification request :::: {0}".format(data))
            url = "https://fcm.googleapis.com/fcm/send"
            scorepesa_key = "AIzaSyCHcZ9ZbJT5o4ScoSnNnGx74-RuIyoLxSA"
  
            if data.get("fcm_token") is None:
               #query token for msisdn/profile            
               #data['msisdn'] = self.get_msisdn_for_profile(data.get("profile_id"))
               token = self.get_profile_fcm_token(data.get("profile_id"))
               if token is None:
                   self.logger.info("push notification on empty token... {0}... ignored".format(token))
                   return True
               data["fcm_token"] = token

            sender = "SCOREPESA"
            dtime = datetime.now()
            message = data.get("msg")
            to = data.get("fcm_token")

            payload = {
             "to" : "{0}".format(to),
             "notification" : {
               "body" : "{0}".format(message),
               "title" : "{0}".format(push_title),
               "icon" : "B!"
             },
             "data" : {
                "message" : "{0}".format(message),
                "sender"  : "{0}".format(sender),
                "timestamp": "{0}".format(dtime)
             }
            }

            self.logger.info("push notification payload ::: {0}".format(payload))
            headers = {
              'content-type': "application/json",
              'authorization': "key={0}".format(scorepesa_key),
            }
            json_text = json.dumps(payload)
            response = requests.post(url, data=json_text, timeout=30, headers=headers, verify=False)
            self.logger.info("push notification response ::: {0}:::{1}".format(response, response.text))
            return True
        except Exception, e:
            self.logger.error("Exception on pushing notification ::: {0}".format(e))
            return False

    def get_profile_fcm_token(self, profile_id):
          token = None
          sqlQ = "select token_id from mobile_app_profile where profile_id=:profile order by modified desc limit 1"
          result = self.connection.execute(sql_text(sqlQ), {'profile': profile_id}).fetchone()
          if result and result[0]:
             token = result[0]
          self.logger.info("got token {0} for profile {1} :: sql {2} :: result :: {3}".format(token ,profile_id, sqlQ, result))
          return token

    def get_outbox_messages(self, data):
          sLimit = 20
          resp = []

          self.logger.info("received outbox query detail ::: {0}".format(data))

          if data.get("profile_id") is not None:
             if data.get("msisdn") is None:
                 msisdn = self.get_msisdn_for_profile(data.get("profile_id"))
                 self.logger.info("got msisdn from profile_id :: {0} :: {1}".format(data.get("profile_id"), msisdn))
             else:
                 msisdn = data.get("msisdn")

             sqlQ = "select outbox_id, text, msisdn, date_created as created from outbox where msisdn=:msisdn order by outbox_id desc limit {0}".format(sLimit)
             result = self.connection.execute(sql_text(sqlQ), {'msisdn': msisdn}).fetchall()

             if result:
                for res in result:
                   outbox_id, text, MSISDN, created = res
                   resp.append({"msisdn": MSISDN, "message": text, "outbox_id": outbox_id, "created": created.strftime('%Y-%m-%d %H:%M:%S')})

             self.logger.info("got outbox response :: for msisdn :: {0} :: sql :: {1}".format(msisdn, sqlQ))

          return json.dumps(resp)

    def send_message(self, data):
        try:           
            message = data.get("msg")
            message_type = 'BULK'
            short_code = data.get("sc")
            correlator = ''
            link_id = ''
            msisdn = data.get("msisdn")
            payload = urllib.urlencode({"message": message, "msisdn":msisdn, "message_type":message_type, "short_code":short_code, "correlator":correlator, "link_id":link_id})
            res = self.call_send_sms_api(payload)
            self.logger.info("got send sms response ::: {0}".format(res))
            if res:
               return True
            return False
        except Exception, e:             
            self.logger.error("Exception on sending sms ::: {0}".format(e))
            return False

    def call_send_sms_api(self, payload):
        try:
            url = "http://127.0.0.1:8008/sendsms"
            headers = {
               'content-type': "application/x-www-form-urlencoded",
            }
            self.logger.info("helper send sms payload:: {0}".format(payload))
            response = requests.request("POST", url, data=payload, headers=headers)
            self.logger.info("helper send sms response :::: {0}".format(response))
            return response
        except Exception, e:
            self.logger.error("Exception on call send sms api ::: {0}".format(e))
            return False

    def award_bonus_to_profile(self, data):
        connection = self.connection.connect()
        msisdn = data.get("msisdn")
        profile_id = data.get("user_id")
        if not profile_id:
           #return error
           return False
        try:
            profile_bonus_dict = {
                "profile_id": profile_id,
                "referred_msisdn":msisdn,
                "bonus_amount":self.scorepesa_configs['registration_bunus_amount'],
                "status":'CLAIMED',
                "expiry_date": datetime.now()+timedelta(days =1),
                "created_by":'app_download_bonus',
                "bet_on_status": 1,
                "date_created": datetime.now(),
                "updated":datetime.now()
            }
            trans = connection.begin()

            result_proxy = connection.execute(ProfileBonu.__table__.insert(), profile_bonus_dict)
            profile_bonus_id = result_proxy.inserted_primary_key
            self.logger.info('App download bonus creatd OK on device registration %s ' % msisdn)

            profileUpdate = """INSERT INTO profile_balance(profile_id, "\
                "balance, bonus_balance, transaction_id, created) VALUES (:pf, 0, "\
                " :amount, :trx_id, NOW()) ON DUPLICATE KEY UPDATE  bonus_balance = "
                " (bonus_balance+%0.2f)""" % (float(self.scorepesa_configs['registration_bunus_amount']), )

            connection.execute(sql_text(profileUpdate), 
                {'pf': profile_id, 'amount': self.scorepesa_configs['registration_bunus_amount'], 'trx_id': -1})

            trans.commit()
            self.logger.info('App download bonus claimed OK on device registration %s ' % msisdn)
            return True
        except Exception, ex:
            trans.rollback()
            self.logger.error("Exception on award app device registration bonus ::: {0}".format(e))
            return False

    def helper_credit_debit_transaction(self, profile_id, reference_id, transaction_type, source, amount, bonus=0): 
        connection = self.connection.connect()
        try:
           #amount to be credit/debit
           bet_on_balance=float(amount)
           bet_on_bonus = float(bonus)

           if transaction_type == "CREDIT":
               transaction_type = 1
           else:
               transaction_type = 0

           #if bonus deduction create bonus transaction as well
           
           trx_debit_dict = {
                "profile_id": profile_id,
                "account": "%s_%s" % (profile_id, 'VIRTUAL'),
                "iscredit": transaction_type,
                "reference": "{0}-{1}".format(reference_id, source),
                "amount": bet_on_balance,
                "created_by": source,
                "created": datetime.now(),
                "modified": datetime.now()
           }
           trans = connection.begin()

           if float(bonus) > 0.0:
               bonus_trx_dict = {
                  "profile_id":profile_id,
                  "profile_bonus_id":profile_bonus_id,
                  "account":"%s_%s" % (profile_id, 'VIRTUAL'),
                  "iscredit":transaction_type,
                  "reference":"{0}-{1}".format(reference_id, source),
                  "amount":bet_on_bonus,
                  "created_by":source,
                  "created":datetime.now(),
                  "modified":datetime.now()
               }
               connection.execute(BonusTrx.__table__.insert(), bonus_trx_dict)         

           trxd = connection.execute(Transaction.__table__.insert(), trx_debit_dict)
           trxd_id = trxd.inserted_primary_key[0]

           #update profile_balance
           bu_Q = "update profile_balance set balance=(balance-{0}), bonus_balance=(bonus_balance-{1}) where profile_id=:profile_id limit 1".format(bet_on_balance, bet_on_bonus)
           if transaction_type == 1:
              #credit cash/bonus
              bu_Q = "update profile_balance set balance=(balance+{0}), bonus_balance=(bonus_balance+{1}) where profile_id=:profile_id limit 1".format(bet_on_balance, bet_on_bonus)

           connection.execute(sql_text(bu_Q),{'profile_id': profile_id})

           trans.commit()

           return trxd_id
        except Exception, ex:
           trans.rollback()
           self.logger.error("Exception helper credit debit transaction {0}".format(ex))
           raise

