#!/usr/bin/env python
from flask import Flask
import logging
import logging.handlers
from flask_restful import Api
from utils import LocalConfigParser
import os


#shokabetsports api classes
from api import Bet, Balance, ScorepesaApiSendSms,\
 TelegramRequest, MQRequest, Withdraw, JackpotBet, ScorepesaTestApiSendSms,\
 JackpotMatches, BingwaMatches, ProfileMgt, BetCancel, ScorepesaPointJackpot,\
 ScorepesaPointAward, ScorepesaReferral, BonusPromo, JpBonusAward, TransactionMgt, UssdMatch,\
 SpecialsBet, XposeAPI, BetDetail

#virtuals api classes
from virtuals_api import BetVirtual

#casino api classes
from casino_api import ReserveFunds, CreditRequest, ConfirmPaymentRequest,\
 ConfirmRequest, CancelPaymentRequest, CancelRequest, ReSettleRequest, UserFundsRequest,\
 SessionCheckRequest, PlayerDetail

#mobile app api classes
from mobile_api import (ScorepesaAppVersion, ScorepesaAppRegDevice, 
	ScorepesaAppDownload, Xposed, Matches, Sports, 
	CompetitionSport, JackpotAndroidMatches, SignupAndroid, Login, 
        AndroidBalance, MyBets, BetDetails, AndroidVerify, AndroidCode)

cur_dir = os.path.dirname(__file__)

filename = '/var/log/scorepesa-mo/scorepesa_mo_api.log'
redis_cfgs = LocalConfigParser.parse_configs("SCOREPESAREDIS")

#attributes
app = Flask(__name__)

app.config['redis_host'] = redis_cfgs['redis_host']
app.config['redis_port'] = redis_cfgs['redis_port']
app.config['db']=redis_cfgs['redis_db']
app.config['redis_passwd']=redis_cfgs['redis_password']

api = Api(app)

#Scorepesa URIs
app.add_url_rule('/bet', view_func=Bet.as_view('bet'))
app.add_url_rule('/balance', view_func=Balance.as_view('balance'))
app.add_url_rule('/bet-detail', view_func=BetDetail.as_view('bet-detail'))
app.add_url_rule('/bet_virtual', view_func=BetVirtual.as_view('bet_virtual'))
api.add_resource(JackpotBet, '/jp/bet')
api.add_resource(ScorepesaApiSendSms, '/sendsms')
api.add_resource(TelegramRequest, '/tlg_bot')
api.add_resource(MQRequest, '/mq_request')
api.add_resource(Withdraw, '/macatm')
api.add_resource(ScorepesaTestApiSendSms, '/testInj')
#api.add_resource(UssdMatch, '/ussd/match')
api.add_resource(UssdMatch, '/ussd/match/<string:reqparam>')
api.add_resource(JackpotMatches, '/jp_matches')
api.add_resource(BingwaMatches, '/bingwa_matches')
api.add_resource(ProfileMgt, '/profilemgt')
api.add_resource(BetCancel, '/bet_cancel')
api.add_resource(ScorepesaPointJackpot, '/free/jp')
api.add_resource(ScorepesaPointAward, '/award/shokabetpoints')
api.add_resource(ScorepesaReferral, '/referral/award')
api.add_resource(BonusPromo, '/bonus_promo/award')
api.add_resource(JpBonusAward, '/jp_bonuses/award')
api.add_resource(TransactionMgt, '/dc/trx')
api.add_resource(SpecialsBet, '/specials/bet')
api.add_resource(XposeAPI, '/shokabet/<string:reqparam>')
api.add_resource(Xposed, '/misc/<string:reqparam>')
#virtuals URIs
api.add_resource(BetVirtual, '/bet_virtual/bet')

#Android APILS
api.add_resource(Matches, '/v1/matches')
api.add_resource(Sports, '/v1/sports')
api.add_resource(CompetitionSport, '/v1/sports/competition')
api.add_resource(JackpotAndroidMatches, '/v1/matches/jackpot')
api.add_resource(SignupAndroid, '/v1/signup')
api.add_resource(Login, '/v1/login')
api.add_resource(AndroidBalance, '/v1/balance')
api.add_resource(MyBets, '/v1/mybets')
api.add_resource(BetDetails, '/v1/betdetails')
api.add_resource(AndroidVerify, '/v1/verify')
api.add_resource(AndroidCode, '/v1/code')
#casino URIs
api.add_resource(ReserveFunds, '/reserve')
api.add_resource(CreditRequest, '/credit')
api.add_resource(ConfirmPaymentRequest, '/confirmPayment')
api.add_resource(ConfirmRequest, '/confirm')
api.add_resource(CancelPaymentRequest, '/cancelPayment')
api.add_resource(CancelRequest, '/cancel')
api.add_resource(ReSettleRequest, '/reSettle')
api.add_resource(UserFundsRequest, '/userFunds')
api.add_resource(SessionCheckRequest, '/sessionCheck')
api.add_resource(PlayerDetail, '/playerDetail')

#Mobile app URIs
api.add_resource(ScorepesaAppVersion, '/checkVersion')
api.add_resource(ScorepesaAppRegDevice, '/registerDevice')
api.add_resource(ScorepesaAppDownload, '/shokabetApp/<string:ops>')

log_formatter = logging.Formatter(
    "%(asctime)s %(levelname)-8s %(name)-5s %(filename)s:%(lineno)d:%("
    "funcName)-10s %(message)s", datefmt="%m-%d-%y %H:%M:%S")

app.logger.setLevel(logging.DEBUG)
handler = logging.handlers.SysLogHandler(address = '/dev/log')
handler.setFormatter(log_formatter)
app.logger.addHandler(handler)


#handler2 = logging.handlers.RotatingFileHandler(filename,
#    maxBytes=1000*1024*1024, backupCount=100)
handler2 = logging.handlers.TimedRotatingFileHandler(filename, 
    when='midnight', interval=1, backupCount=100, encoding=None, 
    delay=False, utc=False)
handler2.setFormatter(log_formatter)
app.logger.addHandler(handler2)

if __name__ == '__main__':
    #app.run(port=5000, host='0.0.0.0')
    app.run(port=5000)

