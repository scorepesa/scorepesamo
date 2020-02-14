import json
from flask import request, make_response,Response
from flask_restful import Resource, reqparse
from SendSmsPublisher import SendSmsPublisher
from ScorepesaCasino import ScorepesaCasino
from flask import current_app
from decimal import Decimal
import requests
from utils import LocalConfigParser
from jose.exceptions import JWTError
from jose import jwt
import urllib
from datetime import datetime
from sqlalchemy.exc import IntegrityError
import json
from functools import wraps

def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.
    """
    current_app.logger.info("received creds....{0}::{1}".format(username, password))
    scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESACASINO")
    passwd=scorepesa_cfgs['luckysix_api_password']
    user=scorepesa_cfgs['luckysix_api_user']
    #current_app.logger.info("config creds ...{0}::{1}".format(user, passwd))
 
    return str(username) == str(user) and str(password) == str(passwd)

def authenticate():
    """Sends a 401 response that enables basic auth"""

    res = json.dumps({"status": "ERROR", "balance": '', "currency": '', "msg": 'Access denied.'})
    return Response(res, 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        current_app.logger.info("auth request got ...{0}".format(auth))
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated


class ReserveFunds(Resource):
    @requires_auth
    def post(self):       
        try:
           message = request.get_json()
           current_app.logger.info('Reserve funds post body: {0} ::: '.format(message))
        except Exception as ex:
           current_app.logger.error("invalid request...{0}".format(ex))
           message=None

        if not message:
           response={"status": "ERROR", "balance": '', "currency": '', "msg": 'Failed!'}
           return  response, 200

        session_id=None
        #validate token
        tpToken = message.get('tpToken')
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        try:
            data = jwt.decode(tpToken, scorepesa_cfgs['encrption_key'], algorithms=['HS256'])
            profile_id = data.get('user').get('id')
        except JWTError as e:
            current_app.logger.error("session token exception %r " % e)
            response={"status": "INVALID_USER_TOKEN", "balance": '', "currency": '', "msg": 'Failed!'}
            return  response, 200

        if not profile_id:
            response={"status": "INVALID_USER_TOKEN", "balance": '', "currency": '', "msg": 'Failed!'}
            return  response, 200

        message['request_name']='ReserveFunds'
        message['profile_id']=profile_id
        current_app.logger.info("Received reserve funds request:::: {0}".format(message))
        casino = ScorepesaCasino(current_app.logger)
        aggregator_id, trx_id, status, balance = casino.create_seven_aggregator_request(message, request_type=0)
        msg_status="OK"
        currency="KES"
        bal=balance
        msg=""
        current_app.logger.info("class response .... {0}::{1}::{2}::{3}".format(aggregator_id, trx_id, status, balance))        
        if not aggregator_id or not trx_id:
            msg_status="ERROR"
            currency=""
            bal=""
            msg="Failed."
            status=200
        if balance == 0:
           msg="Insufficient balance"
           msg_status="INSUFFICIENT_FUNDS"
        if balance == -1:
           msg="User not found."
           msg_status="USER_SUSPENDED"
        if balance == -2:
           msg="Not Allowed."
           msg_status="200"
           
        response={"status": msg_status, "balance": bal, "currency": currency, "msg": msg}
           
        return  response, status        


class CreditRequest(Resource):
    @requires_auth
    def post(self):
        try:
           message = request.get_json()
           current_app.logger.info('credit request message received.... {0}'.format(message))
        except Exception as ex:
           current_app.logger.error("invalid request...{0}".format(ex))
           message=None

        if not message:
           response={"status": "ERROR", "balance": '', "currency": '', "msg": 'Failed!'}
           return  response, 200
        
        message['request_name']='CreditRequest'
        current_app.logger.info("Received credit request:::: {0}".format(message))
        casino = ScorepesaCasino(current_app.logger)
        aggregator_id, trx_id, status, balance = casino.create_seven_aggregator_request(message, request_type=1)
        msg_status="OK"
        currency="KES"
        bal=balance
        msg=""
        current_app.logger.info("class response .... {0}::{1}::{2}::{3}".format(aggregator_id, trx_id, status, balance))
        if not aggregator_id or not trx_id:
            msg_status="ERROR"
            currency=""
            bal=""
            msg="Failed."
            status=200
        else:
            if message.get("autoApprove")==True:
                #call flag transaction as complete since no confirmation after
                status='completed'
                payment_id = message.get("paymentId")
                trx_id=message.get("transactionId")
                casino.flag_payment_as_confirmed(payment_id, trx_id, status=status)

        response={"status": msg_status, "balance": bal, "currency": currency, "msg": msg}
        return  response, 200


class ConfirmPaymentRequest(Resource):
    @requires_auth
    def post(self):
        try:
           message = request.get_json()
           current_app.logger.info('confirm payment request message received.... {0}'.format(message))
        except Exception as ex:
           current_app.logger.error("invalid request...{0}".format(ex))
           message=None

        if not message:
           response={"status": "ERROR", "balance": '', "currency": '', "msg": 'Failed!'}
           return  response, 200

        message['request_name']='ConfirmPaymentRequest'
        current_app.logger.info("Received confirm payment request:::: {0}".format(message))
        casino = ScorepesaCasino(current_app.logger)
        response, status = casino.confirm_payment(message)
        current_app.logger.info("class response .... {0}::{1}::::".format(response, status))
        response=response
        return  response, status


class ConfirmRequest(Resource):
    @requires_auth
    def post(self):
        try:
           message = request.get_json()
           current_app.logger.info('confirm request message received.... {0}'.format(message))
        except Exception as ex:
           current_app.logger.error("invalid request...{0}".format(ex))
           message=None

        if not message:
           response={"status": "ERROR", "balance": '', "currency": '', "msg": 'Failed!'}
           return  response, 200
         
        message['request_name']='ConfirmRequest'
        current_app.logger.info("Received confirm request:::: {0}".format(message))
        casino = ScorepesaCasino(current_app.logger)
        response, status = casino.confirm_trx(message)
        current_app.logger.info("class response .... {0}::{1}::::".format(response, status))
        response=response
        return  response, status


class CancelRequest(Resource):
    @requires_auth
    def post(self):
        try:
           message = request.get_json()
           current_app.logger.info('cancel request message received.... {0}'.format(message))
        except Exception as ex:
           current_app.logger.error("invalid request...{0}".format(ex))
           message=None

        if not message:
           response={"status": "ERROR", "balance": '', "currency": '', "msg": 'Failed!'}
           return  response, 200

        session_id=None
        #validate token
        tpToken = message.get('tpToken')
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        try:
            data = jwt.decode(tpToken, scorepesa_cfgs['encrption_key'], algorithms=['HS256'])
            session_id = data.get('session_id', '')
        except JWTError as e:
            current_app.logger.error("session token exception %r " % e)

        if not session_id:
           response={"status": "INVALID_USER_TOKEN", "balance": '', "currency": '', "msg": 'Failed!'}
           return  response, 200

        message['request_name']='CancelRequest'
        current_app.logger.info("Received cancel request:::: {0}".format(message))
        casino = ScorepesaCasino(current_app.logger)
        payment_id = message.get("paymentId")
        trx_id = message.get("transactionId")       
        response, status = casino.do_payment_cancel(payment_id, trx_id)
        current_app.logger.info("class response .... {0}::{1}::::".format(response, status))
        return  response, status


class CancelPaymentRequest(Resource):
    @requires_auth
    def post(self):
        try:
           message = request.get_json()
           current_app.logger.info('cancel payment request message received.... {0}'.format(message))
        except Exception as ex:
           current_app.logger.error("invalid request...{0}".format(ex))
           message=None

        if not message:
           response={"status": "ERROR", "balance": '', "currency": '', "msg": 'Failed!'}
           return  response, 200

        session_id=None
        #validate token
        tpToken = message.get('tpToken')
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        try:
            data = jwt.decode(tpToken, scorepesa_cfgs['encrption_key'], algorithms=['HS256'])
            session_id = data.get('session_id', '')
        except JWTError as e:
            current_app.logger.error("session token exception %r " % e)

        if not session_id:
           response={"status": "INVALID_USER_TOKEN", "balance": '', "currency": '', "msg": 'Failed!'}
           return  response, 200

        message['request_name']='CancelPaymentRequest'
        current_app.logger.info("Received cancel payment request:::: {0}".format(message))
        casino = ScorepesaCasino(current_app.logger)
        payment_id = message.get("paymentId")
        response, status = casino.do_payment_cancel(payment_id)
        current_app.logger.info("class response .... {0}::{1}::::".format(response, status))
        return  response, status


class ReSettleRequest(Resource):
    @requires_auth
    def post(self):
        try:
           message = request.get_json()
           current_app.logger.info('resettle request message received.... {0}'.format(message))
        except Exception as ex:
           current_app.logger.error("invalid request...{0}".format(ex))
           message=None

        if not message:
           response={"status": "ERROR", "balance": '', "currency": '', "msg": 'Failed!'}
           return  response, 200

        session_id=None
        #validate token
        tpToken = message.get('tpToken')
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        try:
            data = jwt.decode(tpToken, scorepesa_cfgs['encrption_key'], algorithms=['HS256'])
            session_id = data.get('session_id', '')
        except JWTError as e:
            current_app.logger.error("session token exception %r " % e)

        if not session_id:
           response={"status": "INVALID_USER_TOKEN", "balance": '', "currency": '', "msg": 'Failed!'}
           return  response, 200

        message['request_name']='ReSettleRequest'
        current_app.logger.info("Received re-settle request:::: {0}".format(message))
        casino = ScorepesaCasino(current_app.logger)
        payment_id = message.get("paymentId")
        trx_id = message.get("transactionId")
        response, status = casino.do_resettle(payment_id, trx_id)
        current_app.logger.info("class response .... {0}::{1}::::".format(response, status))
        return  response, status


class UserFundsRequest(Resource):
    @requires_auth
    def get(self):
        try:
           message = request.get_json()
           current_app.logger.info('userfunds request message received.... {0}'.format(message))
        except Exception as ex:
           current_app.logger.error("invalid request...{0}".format(ex))
           message=None

        if not message:
           response={"status": "ERROR", "balance": '', "currency": '', "msg": 'Failed!'}
           return  response, 200

        session_id=None
        #validate token
        tpToken = message.get('tpToken')
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        try:
            data = jwt.decode(tpToken, scorepesa_cfgs['encrption_key'], algorithms=['HS256'])
            session_id = data.get('session_id', '')
        except JWTError as e:
            current_app.logger.error("session token exception %r " % e)

        if not session_id:
           response={"status": "INVALID_USER_TOKEN", "balance": '', "currency": '', "msg": 'Failed!'}
           return  response, 200

        message['request_name']='UserFundsRequest'
        current_app.logger.info("Received user funds request:::: {0}".format(message))
        casino = ScorepesaCasino(current_app.logger)
        profile_id = message.get('user','')
        response, status = casino.get_user_funds(profile_id)
        current_app.logger.info("class response .... {0}::{1}::::".format(response, status))
        return  response, status


class PlayerDetail(Resource):
    @requires_auth
    def get(self):
        try:
           current_app.logger.info('Headers: %s', request.headers)
           current_app.logger.info('Body: {0} :: session: {1} '.format(request.args, request.args.get("sessionId")))       
           #sessionId=$sessionId&foreignId=$foreignId&clubUuid=$clubUuid
           #parser = reqparse.RequestParser()
           #parser.add_argument('sessionId', type=str, required=True, help='Required missing')
           #parser.add_argument('foreignId', type=int, required=True, help='Required missing')
           #parser.add_argument('clubUuid', type=str, required=True, help='Required missing')
           args = request.args#parser.parse_args()
           scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
           current_app.logger.info("player detail api args :: {0} :configs: {1} ::session: {2}".format(args, scorepesa_cfgs['encrption_key'], args.get("sessionId")))
           response = u'Invalid request.'
           status = 421
        except Exception as ex:
           current_app.logger.error("invalid request...{0}".format(ex))
           args=None

        if not args or not args.get('sessionId'):
           response={"status": "ERROR", "balance": '', "currency": '', "msg": 'Failed!'}
           return  response, 200

        #validate token
        tpToken = args.get('sessionId')
        scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
        try:
            data = jwt.decode(tpToken, scorepesa_cfgs['encrption_key'], algorithms=['HS256'])
            user = data.get('user')
            profile_id = user.get('id')
        except JWTError as e:
            current_app.logger.error("session token exception %r " % e)
            response={"status": "INVALID_USER_TOKEN", "balance": '', "currency": '', "msg": 'Failed!'}
            return  response, 200

        if not profile_id:
           response={"status": "INVALID_USER_TOKEN", "balance": '', "currency": '', "msg": 'Failed!'}
           return  response, 200
        try:
           args = args.to_dict()
           args['request_name']='PlayerDetailRequest'
           current_app.logger.info("Received player detail request:::: {0}".format(args))
           casino = ScorepesaCasino(current_app.logger)
           profile_id = args['foreignId']
           response, status = casino.get_player_detail(profile_id)
           current_app.logger.info("player detail class response .... {0}::{1}::::".format(response, status))
        except Exception as Ex:
           current_app.logger.error("exception processing player detail...{0}".format(Ex))
           response={"status": "ERROR", "balance": '', "currency": '', "msg": 'Failed'}
           status=200
        return  response, status

class SessionCheckRequest(Resource):
    @requires_auth
    def get(self):
        try:
           current_app.logger.info('Headers: %s', request.headers)
           current_app.logger.info('Body: {0} :: session: {1} '.format(request.args, request.args.get("sessionId")))
           #parser = reqparse.RequestParser()
           #current_app.logger.info('Got parser: %r', parser)
           #parser.add_argument('sessionId', type=str, required=True, help='Required missing')
           #parser.add_argument('foreignId', type=int, required=True, help='Required missing')
           #parser.add_argument('clubUuid', type=str, required=True, help='Required missing')
           args = request.args#parser.parse_args()
           current_app.logger.info('extracted arguments: {0}::session:{1}'.format(args, args['sessionId']))
           scorepesa_cfgs = LocalConfigParser.parse_configs("SCOREPESA")
           current_app.logger.info("check session api args :: {0} :configs: {1}".format(args, scorepesa_cfgs['encrption_key']))
        except Exception as ex:
           current_app.logger.error("check session invalid request...{0}".format(ex))
           args=None

        if not args or not args['sessionId']:
           response={"isValid": False}
           return  response, 200

        if not args['sessionId'] or not args['foreignId'] or not args['clubUuid']:
           response={"isValid": False}
           return  response, 200
   
        #validate token
        tpToken = args['sessionId'] 
        try:
            data = jwt.decode(tpToken, scorepesa_cfgs['encrption_key'], algorithms=['HS256'])
            profile_id = data.get('user').get('id')
            if int(profile_id) != int(args['foreignId']):
                response={"isValid": False}
                return  response, 200
        except JWTError as e:
            current_app.logger.error("session token check exception %r " % e)
            response={"isValid": False}
            return  response, 200

        response = {"isValid": True}
        current_app.logger.info("check session success response :: {0}".format(response)) 
        return  response, 200
