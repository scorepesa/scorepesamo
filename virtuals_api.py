import json
from flask import request, make_response
from flask_restful import Resource, reqparse
from SendSmsPublisher import SendSmsPublisher
from ScorepesaVirtual import ScorepesaVirtual
from flask import current_app
from decimal import Decimal
import requests
from utils import LocalConfigParser
from jose.exceptions import JWTError
from jose import jwt
import urllib
from datetime import datetime
from sqlalchemy.exc import IntegrityError


class BetVirtual(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('token', type=str, required=True, help='Provide token')
        args = parser.parse_args(strict=True)
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        current_app.logger.info("JWT Virtuals bet api args :: {0} :configs: {1}".format(args, scorepesa_cfgs['encrption_key']))
        response = 'Bad Request'
        status = 421
        try:
            data = jwt.decode(args['token'], scorepesa_cfgs['encrption_key'], algorithms=['HS256'])
            message_json = data.get('user')
            current_app.logger.info("virtual bet data extracted :::{0} ::: {1}".format(message_json, data))
            if message_json:
               res = "Sorry virtual is not available now. Please check again later."
               response, status = self.virtuals_api_bet(message_json)
        except JWTError as e:
            current_app.logger.error("Virtual Bet token exception %r " % e)
        resp = make_response(response, status)
        current_app.logger.info("jwt virtual bet api response message:: %r :: %r ::returned::%r" % (response, status, resp))
        return resp

    def virtuals_api_bet(self, message):
        scorepesaVirtual = ScorepesaVirtual(current_app.logger)
        profile_id = message.get('profile_id')

        current_app.logger.info("virtuals bet api profile_id... {0}".format(profile_id))
        if profile_id is None or profile_id=='':
            return "Bet failed, please try again later.", 421
        if int(profile_id) < 0:
            return "Bet failed, please try again later.", 421

        if scorepesaVirtual.check_account_freeze(None, profile_id):
            return scorepesaVirtual.scorepesa_configs['account_bet_block_msg'], 421

        slips = message.get('slip')
        possible_win = message.get('possible_win')
        app_name = message.get('app_name') if message.get('app_name') else 'API_WEB'
        amount = message.get('stake_amount')
        amount = abs(float(amount))

        current_app.logger.info("virtuals stake amount ...{0}".format(amount))

        if float(amount) < 1.0:
            return "Bet failed, please try again later.", 421
        if len(slips) < 1:
            return "Bet failed, please select a game pick to bet on.", 421
        current_app.logger.info("virtuals bet amount::: {0} ::message:: {1} ::profile:: {2}:::".format(amount, message, profile_id))

        response, status = scorepesaVirtual.place_virtual_bet(message)
        current_app.logger.info("virtual api response .....{0}:::status::{1}".format(response, status))
        return response, status
