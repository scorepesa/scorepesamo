from utils import LocalConfigParser
from Scorepesa import Scorepesa
from sqlalchemy.sql import text as sql_text
import re

class ScorepesaUssd(Scorepesa):

    APP_NAME = 'scorepesa_ussd_app'
    SELECTED_MARKETS = "1, 45,29, 18, 18, 9, 8, 15, 10, 16,162, 139, 83, 60"

    def __init__(self, logger):
          super(ScorepesaUssd, self).__init__(Scorepesa(logger))
          self.logger.info("[+] now scorepesa ussd init()..")
          self.scorepesa_ussd_cfgs = LocalConfigParser.parse_configs("SCOREPESAUSSD")

    def __del__(self):
          self.logger.info("[-] Destroy scorepesa ussd obj..")  
          self.scorepesa_ussd_cfgs=None

    def daily_games(self, message, ignore_filter=False):
        sLimit = self.redis_configs['games_query_limit']
        starttime_interval = self.scorepesa_ussd_cfgs["sport_match_starttime_interval"]
        if self.scorepesa_configs['soccer_sport_id']:
            sport_id = self.scorepesa_configs['soccer_sport_id']
        else:
            sport = self.db_session.query(Sport).filter_by(sport_name='Soccer').first()
            sport_id = sport.sport_id if sport else 1

        #profile_id, new = self.create_profile(message, 1)

        game_requests_Q ="""select match_id from game_request where profile_id=:profile_id
            and created > :today """

        todays_list = False

        if not ignore_filter:
            todays_list_games = self.db.engine.execute(
                  sql_text(game_requests_Q),
                  {'profile_id':profile_id, 'today': time.strftime("%Y-%m-%d")}).fetchall()

            todays_list =  ",".join([str(result[0]) for result in todays_list_games ])
            self.logger.info("[+] todays list of  already requested games [][] %s [+] %r [][]" % (todays_list, todays_list_games))
            if not todays_list:
                todays_list = "0"

        games_sql = "select m.game_id, m.home_team, m.away_team, group_concat("\
            "concat(o.special_bet_value, '#', ot.name, '|', o.odd_key, '=', o. odd_value)) as odds, "\
            "m.match_id, m.parent_match_id,  m.start_time, time(m.start_time)match_time, "\
            "o.special_bet_value sbv from `match` m inner join event_odd o on "\
            "m.parent_match_id=o.parent_match_id inner join odd_type ot on "\
            " (ot.sub_type_id = o.sub_type_id and o.parent_match_id = m.parent_match_id) "\
            " inner join competition c on c.competition_id"\
            "=m.competition_id where m.status=:status and m.start_time > now()"\
            "and m.start_time <= date_add(now(), interval {2} hour) and m.match_id"\
            " not in ({0}) and c.sport_id=:sport_id and o.sub_type_id=:sub_type_id "\
            "and o.max_bet=1 group by m.parent_match_id having odds is not null"\
            "order by m.priority desc, c.priority desc, m.start_time asc limit {1}"\
           .format(todays_list, sLimit, starttime_interval)

        games_result = self.db.engine.execute(
                sql_text(games_sql),
                {'status':1, 'game_requests':todays_list, 'sport_id':sport_id, 'sub_type_id':self.default_sub_type_id}).fetchall()

        self.logger.info("[+] todays daily matches sql [][] %s [+] %r [][]" % ( \
            games_sql, {'status':1, 'game_requests':todays_list, 'sport_id':sport_id, \
            'sub_type_id':self.default_sub_type_id}))
        datas = []
        for result in games_result:
            odds_dict =[]
            game_id, home_team, away_team, odds, match_id, parent_match_id, start_time, match_time, sbv = result
            for o in odds.split(','):
                o = self._tostr(o)
                special_bet_value = o.split('#')[0]
                o =  o.split('#')[1]
                sub_type = o.split('|')[0]
                real_odd = {o.split('|')[1].split('=')[0] : o.split('|')[1].split('=')[1],
                    'special_bet_value':special_bet_value}
                if sub_type in odds_dict:
                    odds_dict[sub_type].append(real_odd)
                else:
                    odds_dict[sub_type] = [real_odd]

            datas.append({'game_id':game_id, 'home_team':self._tostr(home_team), 'away_team':self._tostr(away_team), 'odds':odds_dict,
                'match_id':match_id, 'parent_match_id':parent_match_id, 'start_time':start_time.strftime("%Y-%m-%d %H:%M")})


        return datas

    def prep_return_match_result(self, match_data, message, ignore_filter=True):
        return match_data

    def deleted_prep_return_match_result(self, match_data, message, ignore_filter=True):
        try:
           games_result = match_data
           game_str = ""
           served_match_ids = []
           for _game_entry in games_result:
              game = _game_entry
              self.logger.info("[+] USSD MATCH prepared result gameId[][] %r [+] %r [][]" % (game, self.default_sub_type_id))
              game_id = game[0]
              home_team = game[1]
              away_team = game[2]
              odds = game[3]
              parent_match_id = game[5]
              served_match_ids.append(game[4])

              game_str = game_str + "%s:%s-%s(%s)#" % (
                        "{0}_{1}".format(game_id, parent_match_id),
                        home_team if len(home_team) < 11 else home_team[:9]+'*',
                        away_team if len(away_team) < 11 else away_team[:9]+'*',
                        odds
              )
         

              testing = False
              if self.scorepesa_ussd_cfgs['ussd_development_game_string'] == "1":
                  testing = True
                  if str(message.get('msisdn')) in self.scorepesa_ussd_cfgs['ussd_test_whitelist'].split(','):
                       testing = False
 
              if not testing:
                 game_str = game_str + "%s~%s|%s:%s-%s(%s)#" % (
                      game.start_time,
                      game.match_time,
                      "{0}_{1}".format(game_id, parent_match_id),
                      home_team if len(home_team) < 11 else home_team[:9]+'*',
                      away_team if len(away_team) < 11 else away_team[:9]+'*',
                      odds
                 )
           self.logger.info("[+] USSD PREPARED GAME STRING [][] %r [][]" % game_str)

           if not ignore_filter:
               ignore=True
              #self.save_user_games(served_match_ids, profile_id)

           response = game_str #unicode(game_str)
           self.logger.info("[+] USSD API prepare result GAME STRING RESPONSE [][] %r []" % response)

           return response
        except Exception, e:
           return None

    def daily_match_highlight(self, message, ignore_filter=True):
        sLimit = self.redis_configs['games_query_limit']
        starttime_interval = self.scorepesa_ussd_cfgs["sport_match_starttime_interval"]
        if self.scorepesa_configs['soccer_sport_id']:
            sport_id = self.scorepesa_configs['soccer_sport_id']
        else:
            sport = self.db_session.query(Sport).filter_by(sport_name='Soccer').first()
            sport_id = sport.sport_id if sport else 1

        #profile_id, new = self.create_profile(message, 1)

        game_requests_Q ="""select match_id from game_request where profile_id=:profile_id
            and created > :today """

        todays_list = False

        if not ignore_filter:
            todays_list_games = self.db.engine.execute(
                  sql_text(game_requests_Q),
                  {'profile_id':profile_id, 'today': time.strftime("%Y-%m-%d")}).fetchall()

            todays_list =  ",".join([str(result[0]) for result in todays_list_games ])
            self.logger.info("[+] todays list of  already requested highlights "
                "games [][] %s[+]%r [][]" % (todays_list, todays_list_games))
            if not todays_list:
                todays_list = "0"

        games_sql = "select m.game_id, m.home_team, m.away_team, group_concat("\
            "concat(o.special_bet_value, '#', ot.name, '|', o.odd_key, '=', o. odd_value)) as odds, "\
            "m.match_id, m.parent_match_id,  m.start_time, time(m.start_time)match_time, "\
            "o.special_bet_value sbv, m.priority from `match` m inner join event_odd o on "\
            "m.parent_match_id=o.parent_match_id inner join odd_type ot on "\
            " (ot.sub_type_id = o.sub_type_id and ot.parent_match_id = o.parent_match_id) "\
            " inner join competition c on "\
            "c.competition_id=m.competition_id where m.status=1 and m.start_time > now()"\
            " and c.sport_id=:sport_id and o.sub_type_id in ({0}) group by m.parent_match_id"\
            " having odds is not null order by m.priority desc, c.priority desc, m.start_time asc limit {1}"\
           .format(self.SELECTED_MARKETS, sLimit)
        print games_sql

        params = {'status':1, 'sport_id':sport_id}

        results = self.db.engine.execute(sql_text(games_sql), params).fetchall()
        self.logger.info("[+] todays highlight matches sql [][] %s [+] %r [][]" % (games_sql, params))
        #print "RESULTS HERE", results
        datas = []
        for result in results:
            game_id, home_team, away_team, odds, match_id, parent_match_id, \
                start_time, match_time, sbv, priority = result
            odds_dict = {}
            for o in odds.split(','):
                o = self._tostr(o)
                special_bet_value = o.split('#')[0]
                o = o.split('#')[1]
                sub_type = o.split('|')[0]
                real_odd = {o.split('|')[1].split('=')[0] : o.split('|')[1].split('=')[1], 
                    'special_bet_value':special_bet_value} 
                if sub_type in odds_dict:
                    odds_dict[sub_type].append(real_odd)
                else:
                    odds_dict[sub_type] = [real_odd]

	    datas.append({'game_id':game_id, 'home_team':self._tostr(home_team), 
                 'away_team':self._tostr(away_team), 'odds':odds_dict, 
		 'match_id':match_id, 'parent_match_id':parent_match_id, 
                 'start_time':start_time.strftime("%Y-%m-%d %H:%M"), 'priority':priority})

        return datas

    def _tostr(self, text):
        import unicodedata
        try:
            text = re.sub(r'[^\x00-\x7F]+',' ', text)
            if type(text) == str:
                text = unicode(text)
            return unicodedata.normalize('NFKD', text).encode('ascii','ignore')
        except Exception, e:
            return ""


    def search_for_match(self, search_term, msisdn, subtype=10):
        try:
            self.logger.info("[+] bet match searchterm [][] {0} [+] msisdn [][] {1}"\
                .format(search_term, msisdn))
            if search_term and msisdn:
                 search_term = "%{0}%".format(search_term)
                 sql = "select m.game_id, m.home_team, m.away_team, group_concat("\
                     "concat(o.special_bet_value, '#', ot.name, '|', o.odd_key, '=', o. odd_value)) as odds, "\
                     "m.match_id, m.parent_match_id,  m.start_time, time(m.start_time)"\
                     "match_time, o.special_bet_value sbv from `match` m inner join "\
                     "event_odd o on m.parent_match_id=o.parent_match_id inner join "\
                     "odd_type ot on (ot.sub_type_id = o.sub_type_id  and ot.parent_match_id "\
                     "= o.parent_match_id) inner join competition "\
                     " c on c.competition_id=m.competition_id where m.status=1 and "\
                     "m.start_time > now() and c.sport_id=79 and o.sub_type_id in ({0}) "\
                     " and (m.away_team like :away_team or m.home_team like :home_team "\
                     "or c.competition_name like :compname) group by m.parent_match_id "\
                     "having odds is not null order by m.priority desc, c.priority desc, m.start_time asc"\
                     " limit {1}".format(self.SELECTED_MARKETS, 20)

                 dpars = {'pmid': search_term, 'gmid':search_term, 'home_team': search_term, 
                     "away_team": search_term, "compname":search_term, 
                     "category":search_term, "subtype":subtype}

                 self.logger.info("[+] search match query [] {0} [] parameters [] {1} [] [+]".format(sql, dpars))
            else:
                return []

            results = self.db.engine.execute(sql_text(sql), dpars).fetchall()
            datas = []
            for result in results:
                game_id, home_team, away_team, odds, match_id, parent_match_id, start_time, match_time, sbv = result
                odds_dict = {}
                for o in odds.split(','):
                    o = self._tostr(o)
                    special_bet_value = o.split('#')[0]
                    o = o.split('#')[1]
                    print 0, "READING THEM"
                    sub_type = o.split('|')[0]
                    real_odd = {o.split('|')[1].split('=')[0] : o.split('|')[1].split('=')[1], 
                        'special_bet_value':special_bet_value}
                    if sub_type in odds_dict:
                        odds_dict[sub_type].append(real_odd)
                    else:
                        odds_dict[sub_type] = [real_odd]

	        datas.append({'game_id':game_id, 'home_team':self._tostr(home_team),
                   'away_team':self._tostr(away_team), 'odds':odds_dict, 
		   'match_id':match_id, 'parent_match_id':parent_match_id, 
                   'start_time':start_time.strftime("%Y-%m-%d %H:%M"), 'sbv':sbv})

            return datas
        except Exception, e:
            self.logger.error("[x] Exception match search [][] %r [-] %s [-] %s [][]" % (e, msisdn, search_term))
            return []

    def get_match_details(self, game_id, pick, sub_type=1, parent_match_id=None):
        try:
            self.logger.info("[+] get_match_details data....[][] {0} "\
                "[+] {1} [+] {2} [+] {3} [][]".format(game_id, pick, sub_type, parent_match_id))
            if parent_match_id:
                 sql = "select sub_type_id, odd_key as pick_key, odd_value,"\
                    " m.parent_match_id, special_bet_value from event_odd e inner join `match` "\
                    "m on e.parent_match_id=m.parent_match_id where "\
                    "m.parent_match_id=:pmid and e.sub_type_id=:sub_type and odd_key=:pick"

                 dpars = {'sub_type': sub_type, 'pick':pick, 'pmid':parent_match_id}
            else:
                 sql = "select sub_type_id, odd_key as pick_key, odd_value, "\
                     "m.parent_match_id, special_bet_value from event_odd e inner join `match` m on "\
                     "e.parent_match_id=m.parent_match_id where m.game_id=:gmid and "\
                     "e.sub_type_id=:sub_type and odd_key=:pick"

                 dpars = {'sub_type': sub_type, 'gmid':game_id, 'pick':pick}
            result=self.db.engine.execute(sql_text(sql), dpars).fetchone()
            if result:
               sub_type_id, pick_key, odd_value, parent_match_id, special_bet_value = result
            else:
               sub_type_id, pick_key, odd_value, parent_match_id, special_bet_value =\
                   sub_type, pick, None, parent_match_id, ''

            data={"sub_type_id": sub_type_id, "pick_key":pick_key, 'special_bet_value':special_bet_value, 
                "odd_value":odd_value, "parent_match_id":parent_match_id}
            self.logger.info("[+] bet match detail fetch [][] %s [+] %r "\
                "[+] %r [+] %r [][]" % (sql, result, dpars, data))
            return data
        except Exception, e:
            self.logger.error("[x] Exception match detail fetch [][] %r [] %s [][]" % (e, sql))
            return {}

    def ussd_match_sport_ids(self, data):
        try:
            #profile_id, new = self.create_profile(data, 1)

            prematch_markets = self.scorepesa_ussd_cfgs['scorepesa_ussd_sport_markets']
            ex_sport_id = self.scorepesa_ussd_cfgs['exclude_sport_ids']
 
            self.logger.info("[+] get_match_details sport id data "\
                "[][] {0} [+] {1} [][]".format(data, prematch_markets))

            sql = "select s.sport_id, sport_name, e.sub_type_id as market"\
               " from sport s inner join competition c on (c.sport_id=s.sport_id)"\
               " inner join `match` m on m.competition_id=c.competition_id "\
               " inner join event_odd e on e.parent_match_id=m.parent_match_id"\
               " where e.sub_type_id in({0}) and s.sport_id not in({1}) "\
               " group by s.sport_id, e.sub_type_id"\
               .format(prematch_markets, ex_sport_id)

            result=self.db.engine.execute(sql_text(sql)).fetchall()
            data = []
            if result:
               for res in result:
                   sport_id, sport_name, market = res
                   data.append({"sport_id": sport_id, "sport_name": sport_name, 
                       "sub_type_id": market})
            else:
               sport_id, sport_name, market = 14, "soccer", 10
               data={"sport_id": sport_id, "sport_name": sport_name, "sub_type_id": market}

            self.logger.info("[+] bet match detail ussd match sport "\
                 "details[][] %s [+] %r [+] %r [][]" % (sql, result, data))
            return data
        except Exception, e:
            self.logger.error("[x] Exception match sport details[][] %r [][]" % (e))
            return None

    def daily_match_sport(self, data):      
        sLimit = self.redis_configs['games_query_limit']
        sport_id = data.get("sport_id")
        market_id = data.get("sub_type_id")
        ignore_filter=True
        starttime_interval = self.scorepesa_ussd_cfgs["sport_match_starttime_interval"]
        #set default  
        if sport_id is None:
            sport_id=79

        if market_id is None:
            market_id = self.default_sub_type_id

        self.logger.info("[+] sport match [+] sport {0} [+ market +] {1} "
           "[ starttimeinterval ] {2} [][]".format(sport_id, market_id, starttime_interval))

        #profile_id, new = self.create_profile(data, 1)

        games_sql = "select m.game_id, m.home_team, m.away_team, group_concat("\
            "concat(ot.name, '|', o.odd_key, '=', o. odd_value)) as odds, "\
            "m.match_id, m.parent_match_id,  m.start_time, time(m.start_time)match_time, "\
            "o.special_bet_value sbv from `match` m inner join event_odd o on "\
            "m.parent_match_id=o.parent_match_id inner join odd_type ot on "\
            " (ot.sub_type_id = o.sub_type_id and ot.parent_match_id = o.parent_match_id) "\
            " inner join competition c on "\
            "c.competition_id=m.competition_id where m.status=1 and m.start_time > now()"\
            " and c.sport_id=:sport_id and o.sub_type_id in ({0}) "\
            "and m.start_time <= date_add(now(), interval {1} hour)"\
            " group by m.parent_match_id "\
            " having odds is not null order by m.priority desc, c.priority desc, m.start_time asc limit {2}"\
           .format(self.SELECTED_MARKETS, starttime_interval, sLimit)


        params = {'status':1, 'sport_id':sport_id, 'market':self.default_sub_type_id}

        results = self.db.engine.execute(sql_text(games_sql), params).fetchall()

        self.logger.info("[+] todays sport matches sql [][] %s [+] %r [][]" % (games_sql, params))

        datas = []
        for result in results:
	    game_id, home_team, away_team, odds, match_id, \
                parent_match_id, start_time, match_time, sbv = result
	    odds_dict = {}
	    for o in odds.split(','):
                o = self._tostr(o)
	        sub_type = o.split('|')[0]
	        real_odd = {o.split('|')[1].split('=')[0] : o.split('|')[1].split('=')[1]}
	        if sub_type in odds_dict:
		    odds_dict[sub_type].append(real_odd)
	        else:
		    odds_dict[sub_type] = [real_odd]

	    datas.append({'game_id':game_id, 'home_team':self._tostr(home_team), 
                 'away_team':self._tostr(away_team), 'odds':odds_dict,
                 'match_id':match_id, 'parent_match_id':parent_match_id, 
                 'start_time':start_time.strftime("%Y-%m-%d %H:%M"), 'sbv':sbv})
        print "DATAS", datas
        return datas

    def ussd_check_user_exists(self, message):
        try:
            userQ = "select profile_id from profile where msisdn=:value"
            msisdn = message.get("msisdn")
            userExist = self.db.engine.execute(sql_text(userQ), {'value': msisdn}).fetchone()
            self.logger.info("[+] user exists result [][][] {0} [][]".format(userExist))
            if userExist:                
                self.logger.info("Found user exists already... return response...")
                return {"userExist": True}
            else:
                profile_id, new = self.create_profile(message, 1)
                return {"userExist": False}
        except Exception, e:
            self.logger.error("[-] Exception on ussd_check_user_exists {0}".format(e))
            return {"userExist": "error"}

    def ussd_match_top_leagues(self, data):
        try:
            top_league_sport_id = self.scorepesa_ussd_cfgs['top_league_sport_id']

            self.logger.info("[+] 2222222222 get_league_details top leagues data [+] {0}"\
                " [+] for sportId [+] {1} [][]".format(data, top_league_sport_id))

            sql = "select s.sport_id, s.sport_name, c.competition_id as league_id,"\
                " c.competition_name as league_name, c.priority "\
                " from sport s inner join competition c on (c.sport_id=s.sport_id) "\
                " where s.sport_id={0} group by c.competition_id order by c.priority"\
                " desc limit 10"\
               .format(top_league_sport_id)

            result=self.db.engine.execute(sql_text(sql)).fetchall()
            data = []
            if result:
               for res in result:
                   sport_id, sport_name, league_id, league_name, priority = res
                   data.append({"sport_id": sport_id, "sport_name": sport_name, 
                      "league_id": league_id, "league_name": league_name, "priority":priority})
            else:
               sport_id, sport_name, league_id, league_name, priority = 79, "soccer", None, None, 0
               data={"sport_id": sport_id, "sport_name": sport_name, 
                   "league_id": league_id, "league_name": league_name, "priority": priority}

            self.logger.info("[+] soccer ussd top leagues detail [][]"\
                 " %s [+] %r [+] %r [][]" % (sql, result, data))
            return data
        except Exception, e:
            self.logger.error("[x] Exception soccer top league details[][] %r [][]" % (e))
            return []
    def jackpot_games(self):
        jp_sql= "SELECT jackpot_event_id FROM jackpot_event WHERE "\
            "jackpot_type = :st AND status = 'ACTIVE' ORDER BY 1 DESC LIMIT 1"
        jp_result = self.db.engine.execute(sql_text(jp_sql),{'st':2}).fetchone()
        games = []
        if jp_result:
            sql = "select j.game_order as pos, e.sub_type_id, "\
                "group_concat(odd_key) as correctscore, "\
                "m.game_id, m.match_id, m.start_time, m.parent_match_id, m.away_team, "\
                "m.home_team from jackpot_match j inner join `match` m on "\
                "m.parent_match_id = j.parent_match_id inner join event_odd e on "\
                "e.parent_match_id = m.parent_match_id where j.jackpot_event_id=:jpid "\
                " and e.sub_type_id=:sbid group by j.parent_match_id order by pos"

            games = self.db.engine.execute(sql_text(sql), {'sbid':45, 'jpid':jp_result[0]}).fetchall()
        
        datas = []
        for result in games:
            pos, sub_type_id, odds, game_id, match_id, start_time,\
                parent_match_id, away_team, home_team = result

            datas.append({'pos':pos, 'sub_type_id':sub_type_id, 'odds_keys':odds, 
                'game_id':game_id, 'match_id':match_id, 'start_time':start_time.strftime("%Y-%m-%d %H:%M"), 
                'parent_match_id':parent_match_id, 'away_team':away_team, 
                'home_team':home_team, 'jackpot_event_id':jp_result[0]})
        return datas
    

    def fetch_top_league_matches(self, data):
        sLimit = self.redis_configs['games_query_limit']
        sport_id = data.get("sport_id")
        market_id = data.get("sub_type_id")
        league_id = data.get("league_id")
        ignore_filter=True
        print "STARTIFNG fetch_top_league_matches "
        starttime_interval = self.scorepesa_ussd_cfgs["sport_match_starttime_interval"]
        #set default  
        if sport_id is None:
            sport_id=self.scorepesa_ussd_cfgs['top_league_sport_id']

        picks = "'1', 'x', '2', 'yes', 'no', 'over', 'under','1x','12','x2'"
        markets = "1, 29"
        if sport_id == int(self.scorepesa_ussd_cfgs["motorsport_id"]):
            markets = "20"
            picks = "'1','2'"    

        self.logger.info("[+] sport match [+] sport {0} [+ market +] {1}"\
            " [starttimeinterval ] {2} [][]"\
	    .format(sport_id, market_id, starttime_interval))        

        games_sql ="select m.game_id, m.home_team, m.away_team, "\
            " group_concat(concat(ot.name, '|', o.odd_key, '=', o. odd_value)) as odds,"\
            " m.match_id, m.parent_match_id,  m.start_time, time(m.start_time)match_time,"\
            " o.special_bet_value sbv from `match` m inner join event_odd o "\
            " on m.parent_match_id=o.parent_match_id inner join odd_type ot "\
            " on (ot.sub_type_id = o.sub_type_id and ot.parent_match_id = o.parent_match_id)"\
            " inner join competition c on "\
            " c.competition_id=m.competition_id where m.status=1 and "\
            " m.start_time > now() and c.sport_id=:sport_id and o.sub_type_id"\
            " in ({0}) and m.competition_id=:comp_id group by m.parent_match_id "\
            " having odds is not null order by m.start_time asc limit {1}"\
            .format(self.SELECTED_MARKETS, sLimit)

        params = {'status':1, 'sport_id':sport_id, 'market':markets, 'comp_id': league_id}

        games_result = self.db.engine.execute(sql_text(games_sql), params).fetchall()

        self.logger.info("[+] todays top league matches sql [][] %s [+] %r [][]" % (games_sql, params))

        datas = []

        for result in games_result:
            game_id, home_team, away_team, odds, match_id, parent_match_id,\
                start_time, match_time, sbv = result

            odds_dict = {}
	    for o in odds.split(','):
                o = self._tostr(o)
	        sub_type = o.split('|')[0]
	        real_odd = {o.split('|')[1].split('=')[0] : o.split('|')[1].split('=')[1]}
	        if sub_type in odds_dict:
		    odds_dict[sub_type].append(real_odd)
	        else:
		    odds_dict[sub_type] = [real_odd]

            datas.append({'game_id':game_id, 'home_team':self._tostr(home_team), 
                  'away_team':self._tostr(away_team), 'odds':odds_dict,
                  'match_id':match_id, 'parent_match_id':parent_match_id, 
                  'start_time':start_time.strftime("%Y-%m-%d %H:%M"), 'sbv':sbv})

        return datas
