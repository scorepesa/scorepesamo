from utils import LocalConfigParser
from Scorepesa import Scorepesa
from sqlalchemy.sql import text as sql_text
from datetime import datetime, timedelta


class ScorepesaSpecials(Scorepesa):

    def __init__(self, logger):
          super(ScorepesaSpecials, self).__init__(Scorepesa(logger))
          self.logger.info("[+] now scorepesa specials init()..")
          self.scorepesa_ussd_cfgs = LocalConfigParser.parse_configs("SCOREPESASPECIALS")

    def __del__(self):
          self.logger.info("[-] Destroy scorepesa specials obj..")
          self.scorepesa_ussd_cfgs=None

    def validate_bet_slip_odds(self, game_id, pick, amount, sub_type_id=None, special_bet_value=None):
         sub_type_id = sub_type_id if sub_type_id else self.default_sub_type_id
         self.logger.info("Extracting sub_type_id %r" % sub_type_id)        

         if not special_bet_value:
             special_bet_value = ''        

         eventQ = """select m.match_id, m.bet_closure, c.max_stake, o.parent_match_id, o.special_bet_value, o.odd_value, o.odd_key, o.sub_type_id, m.away_team, m.home_team from bleague_event_odd o inner join `match` m on m.parent_match_id = o.parent_match_id inner join competition c on c.competition_id = m.competition_id where o.odd_key = :pick and m.game_id=:game_id and o.sub_type_id = :sub_type_id and o.special_bet_value = :sp_bet_value """
         params = {'pick':pick, 'game_id':game_id,'sub_type_id':sub_type_id,'sp_bet_value': special_bet_value}        

         self.logger.info("ScorepesaSpecials Query1 SQL Details %r Params %r" % (eventQ, params))
         event_odd = self.db.engine.execute(sql_text(eventQ), params).fetchone()        

         if not event_odd:
             eventQ="""select m.match_id, m.bet_closure, c.max_stake, o.parent_match_id, o.special_bet_value, o.odd_value, o.odd_key, o.sub_type_id, m.away_team, m.home_team from bleague_event_odd o inner join `match` m on m.parent_match_id = o.parent_match_id inner join competition c on c.competition_id = m.competition_id inner join odd_key_alias oa on(oa.sub_type_id = o.sub_type_id and oa.odd_key = o.odd_key and oa.special_bet_value=o.special_bet_value) where oa.odd_key_alias = :pick and m.game_id=:game_id """
             params = {'pick':pick, 'game_id':game_id}

         event_odd = self.db.engine.execute(sql_text(eventQ), params).fetchone()

         self.logger.info("ScorepesaSpecials Query2 SQL Details %r Params %r" % (eventQ, params))        

         if not event_odd:
             return True, "Sorry, incorrect pick (%s) for GAMEID %s. For single bet send GAMEID#PICK#AMOUNT \
or for multibet GAMEID#PICK#GAMEID#PICK#AMOUNT to 101010. T&C apply." % (pick, game_id)

         match_id, bet_closure, max_stake, parent_match_id, special_bet_value, odd_value, odd_key, sub_type_id, home_team, away_team = event_odd

         if not match_id:
             return True, "Sorry, incorrect GAMEID: %s. For single bet send GAMEID#PICK#AMOUNT \
or for multibet GAMEID#PICK#GAMEID#PICK#AMOUNT to 101010 .www.scorepesasports.co.tz.T&C apply." % (game_id, )

         if bet_closure < datetime.now():
             return True, "%s-%s match bet time has expired. Send GAME to 101010 to get \
more games .www.scorepesasports.co.tz.T&C apply." % (home_team, away_team)

         if max_stake > 0:
            if float(amount) > float(max_stake):
                return True, "Stake amount for GAME ID %s exceeds the maximum allowed. You can only stake \
upto Kshs %0.2f for this Game ID." % (game_id, float(max_stake))

         return False, {"match_id":match_id, "bet_closure":bet_closure,
                   "max_stake":max_stake, "parent_match_id":parent_match_id,
                   "special_bet_value":special_bet_value, "odd_value":odd_value,
                   "odd_key":odd_key, "sub_type_id":sub_type_id}
