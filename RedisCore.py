import redis


class RedisCore:
    def __init__(self, logger, host, port, db, pazzwd):
        self.redis_o = redis.StrictRedis(host=host, port=port, db=db, password=pazzwd)
        self.logger = logger

    def __del__(self):
        self.logger=None
        if self.redis_o:
           self.redis_o=None

    def push(self, key, data, cperiod):
        try:
            cperiod = int(cperiod)
            self.redis_o.set(key, data, ex=cperiod) #cache 1 hour
            self.logger.info("pushed to redis cache data[] {0}..key[]{1}..period[]{2}".format(data, key, cperiod))
        except Exception, e:
            self.logger.error("Exception pushing data to cache...{0}".format(e))

    def pop(self, key):
        try:
            data = self.redis_o.get(key)
            self.logger.info("Sought cached data ....{0}".format(data))
            return data
        except Exception, e:
            self.logger.error("Exception seeking cache...{0}".format(e))

