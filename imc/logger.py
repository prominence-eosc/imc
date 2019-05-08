import logging

class CustomAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return 'id=%s %s' % (self.extra['id'], msg), kwargs
