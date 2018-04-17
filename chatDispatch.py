import logging
 
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream
from tornado.tcpserver import TCPServer
from tornado.web import Application
from tornado.web import RequestHandler
from tornado.web import asynchronous
from tornado.options import options, parse_command_line, define
from tornado.httpclient import AsyncHTTPClient
from tornado.httpclient import HTTPRequest
from tornado.web import HTTPError

import struct
import json
import urllib
from datetime import datetime

import aConst
import mimetypes
from cStringIO import StringIO
import random




class ConnectionManager(object):
    def __init__(self):
        self.clientArray = {}
        self.debug = True

    def getAllConnection(self):
        return self.clientArray
    
    def getConnection(self,userID):
        if self.clientArray.has_key(userID):
            connection = self.clientArray[userID]
            if connection.closed():
                self.clientArray.pop(userID)
                return None
            return self.clientArray[userID]
        else:
            return None
        
    def setConnection(self,userID,connection):
        self.clientArray[userID] = connection
        if self.debug:
            self._printInfo()

    def closeUserConnection(self,userID):
        if self.clientArray.has_key(userID):
            connection = self.clientArray[userID]
            self.clientArray.pop(userID)
            if not connection.closed():
                connection.close()

    def closeRequestConnection(self,request):
        keys = self.clientArray.keys()
        for key in keys:
            if request == self.clientArray[key]:
                self.clientArray.pop(key)
                if not request.closed():
                    request.close()
                return key
        return None

    def _printInfo(self):
        pass
        #logging.info('connections:')
        #logging.info(str(self.clientArray))


connectionManager = ConnectionManager()



class TCPDataStream(object):
    def __init__(self,*arg,**kwargs):
        if arg.__len__() == 1:
            self.data = arg[0]
            self.userID = None
            self.toUserID = None
            self.length = None
            self.jsonLength = None
            self.cmd = None
            self.version = None
        elif arg.__len__() >= 4:
            self.data = None
            self.userID = arg[0]
            self.toUserID = arg[1]
            self.length = arg[2]
            self.jsonLength = arg[3]
            self.cmd = arg[4]
            self.version = arg[5]
            
    @staticmethod
    def getHeaderLength():
        return 20

    def toDict(self):
        di = {}
        di['userID'] = self.userID
        di['toUserID'] = self.toUserID
        return di

    def parseHeader(self):
        try:
            (self.userID,self.toUserID,self.length,self.jsonLength,self.cmd,self.version) = struct.unpack('iiiihh',self.data)
        except Exception,e:
            logging.error("parseHeader exception: %s",e)
            return False
        #logging.info('parseHeader header:(%s,%s,%s,%s,%s,%s)',str(self.userID),str(self.toUserID),str(self.length),str(self.jsonLength),str(self.cmd),str(self.version))
        return True

    def packHeader(self):
        try:
            self.data = struct.pack('iiiihh',self.userID,self.toUserID,self.length,self.jsonLength,self.cmd,self.version)
        except Exception,e:
            logging.error('packHeader exception: %s',e)
            return False
        #logging.info('packHeader header:(%s,%s,%s,%s,%s,%s)',str(self.userID),str(self.toUserID),str(self.length),str(self.jsonLength),str(self.cmd),str(self.version))
        return True

    def getHeaderData(self):
        return self.data

    def getHeaderCmd(self):
        return self.cmd
    
    def getHeaderUserID(self):
        return self.userID

    def getHeaderToUserID(self):
        return self.toUserID
    
    def getHeaderJsonLength(self):
        return self.jsonLength
    
    def getBodyLength(self):
        return self.length

    def getVersion(self):
        return self.version
    

class TCPDispatch(object):
    def __init__(self, header=None,body=None,onWriteCallBack=None,onCloseCallBack=None):
        self.onWriteCallBack = onWriteCallBack
        self.onCloseCallBack = onCloseCallBack
        if header == None:
            self.header = None
            self.body = None
            return
        self.header = header
        if body and self.header.getHeaderJsonLength() <= body.__len__() and self.header.getHeaderJsonLength() > 0:
            _jsonBody = body[:self.header.getHeaderJsonLength()]
            #logging.info('TCPDispatch init _jsonBody : %s',_jsonBody)
            try:
                self.body = json.loads(_jsonBody)
            except Exception,e:
                logging.error('TCPDispatch init json.lods() exception : %s',e)
                onCloseCallBack()
                return
        elif self.header.getHeaderJsonLength() == 0:
            pass
        else:
            logging.error('TCPDispatch init jsonLength error!')
            onCloseCallBack()
            return 
        
    def dispatch(self):
        #logging.info('TCPDispatch dispatch..........')
        cmd = self.header.getHeaderCmd()
        if self.header.getHeaderUserID() == 0:
	    cmd = 1000
        if cmd == aConst.tcpCmd['message']:
            body = self.body
            header = self.header.toDict()

            body1 = dict(body.items() + header.items())
            body1['selfUserID'] = self.header.getHeaderToUserID()
            di = {}
            di['value'] = json.dumps(body1)
            di['cmd'] =  aConst.httpCmd['message']
            self._sendHttpToChatProcess(self.header.getHeaderToUserID(),di)
        
        elif cmd == aConst.tcpCmd['messageRead']:
            body = self.body
            header = self.header.toDict()
            
            body2 = dict(body.items() + header.items())
            di = {}
            di['value'] = json.dumps(body2)
            di['cmd'] =  aConst.httpCmd['messageRead']
            self._sendHttpToChatProcess(self.header.getHeaderToUserID(),di)

        elif cmd == aConst.tcpCmd['messageReceive']:
            body = self.body
            header = self.header.toDict()
            body1 = dict(body.items() + header.items())
            di = {}
            di['value'] = json.dumps(body1)
            di['cmd'] =  aConst.httpCmd['messageReceive']
            self._sendHttpToChatProcess(self.header.getHeaderUserID(),di)

            
        elif cmd == aConst.tcpCmd['keepAlive']:
            body = {'userID':self.header.getHeaderUserID(),'server':options.responseHttpServer+":"+str(options.responseHttpPort)}
            di = {}
            di['value'] = json.dumps(body)
            di['cmd'] =  aConst.httpCmd['keepAlive']
            self._sendHttpToChatProcess(self.header.getHeaderUserID(),di)

        elif cmd == aConst.tcpCmd['login']:
            body = {'userID':self.header.getHeaderUserID(),'server':options.responseHttpServer+":"+str(options.responseHttpPort)}
            di = {}
            di['value'] = json.dumps(body)
            di['cmd'] =  aConst.httpCmd['login']
            self._sendHttpToChatProcess(self.header.getHeaderUserID(),di)
        
            
    def on_response(self, response):
        if response.error:
            print response
            print response.code
            print response.error
            raise HTTPError(500)
        #logging.info('on_response message : ' + response.body)
        if self.header == None:
            return
        
        cmd = self.header.getHeaderCmd()
        
        if cmd == aConst.tcpCmd['message']:
            #logging.info('on_response message')
            value = response.body
            try:
                value = json.loads(value)
            except Exception,e:
                logging.error("on_response message json exception: %s",e)
                self.onCloseCallBack()
                return

            selfUserID = value['selfUserID']
            
            #logging.info('on_response message result + ' + str(value['result']))
            if value['result'] == aConst.returnType['requestSuccess']:
                _body = {'status':aConst.messageStatus['received']}
                _body['seq'] = value['seq']
                _body['createTime'] = value['createTime']
                _body = json.dumps(_body)
                _body = struct.pack(str(_body.__len__())+'s',_body)
                _header = TCPDataStream(self.header.getHeaderUserID(),self.header.getHeaderToUserID(),_body.__len__(),_body.__len__(),aConst.tcpCmd['messageReply'],self.header.getVersion())
                if _header.packHeader():
                    _data = _header.getHeaderData()
                    _data = _data + _body
                    self.onWriteCallBack(_data)
                else:
                    logging.error('on_reponse message returnFail')
                    self.onCloseCallBack()
            else:
                logging.error('on_reponse message returnFail')
                self.onCloseCallBack()
                
        
        elif cmd == aConst.tcpCmd['messageRead']:
            #logging.info('on_response messageRead')
            value = response.body
            try:
                value = json.loads(value)
            except Exception,e:
                logging.error("on_response messageRead json exception: %s",e)  
                self.onCloseCallBack()
                return
            #logging.info('on_response messageRead result + ' + str(value['result']))
            if value['result'] == aConst.returnType['requestSuccess']:
                pass
            else:
                logging.error('on_reponse messageRead returnFail')
                self.onCloseCallBack()

        elif cmd == aConst.tcpCmd['messageReceive']:
            #logging.info('on_response messageReceive')
            value = response.body
            try:
                value = json.loads(value)
            except Exception,e:
                logging.error("on_response messageReceive json exception: %s",e)  
                self.onCloseCallBack()
                return
            #logging.info('on_response messageReceive result + ' + str(value['result']))
            if value['result'] == aConst.returnType['requestSuccess']:
                pass
            else:
                logging.error('on_reponse messageReceive returnFail')
                self.onCloseCallBack()

        elif cmd == aConst.tcpCmd['keepAlive']:
            #logging.info('on_response keepAlive')
            value = response.body
            try:
                value = json.loads(value)
            except Exception,e:
                logging.error("on_response keepAlive json exception: %s",e)  
                self.onCloseCallBack()
                return
            #logging.info('on_response keepAlive result + ' + str(value['result']))
            if value['result'] == aConst.returnType['requestSuccess']:
                _body = {'status':aConst.returnType['requestSuccess']}
                _body = json.dumps(_body)
                _body = struct.pack(str(_body.__len__())+'s',_body)
                _header = TCPDataStream(self.header.getHeaderUserID(),self.header.getHeaderToUserID(),_body.__len__(),_body.__len__(),aConst.tcpCmd['keepAlive'],self.header.getVersion())
                if _header.packHeader():
                    _data = _header.getHeaderData()
                    _data = _data + _body
                    self.onWriteCallBack(_data)
                else:
                    logging.error('on_reponse keepAlive returnFail')
                    self.onCloseCallBack()
            else:
                logging.error('on_reponse keepAlive returnFail')
                self.onCloseCallBack()

        elif cmd == aConst.tcpCmd['login']:
            #logging.info('on_response login')
            value = response.body
            try:
                value = json.loads(value)
            except Exception,e:
                logging.error("on_response login json exception: %s",e)  
                self.onCloseCallBack()
                return
            #logging.info('on_response login result + ' + str(value['result']))
            if value['result'] == aConst.returnType['requestSuccess']:
                pass
            else:
                logging.error('on_reponse login returnFail')
                self.onCloseCallBack()
        

    def sendHttpOffline(self,userID):
        body = {'userID':userID}
        di = {'value':json.dumps(body),'cmd':aConst.httpCmd['offline']}
        self._sendHttpToChatProcess(userID,di)
        
                    
    def _sendHttpToChatProcess(self,*arg,**kwargs):
        if arg.__len__() == 2:
            userID = arg[0]
            data = arg[1]
            encodedValue = urllib.urlencode(data)
            http = AsyncHTTPClient()
            server = self.userIDToChatProcessServer(userID)
            request = HTTPRequest(server,method='POST',body=encodedValue,use_gzip=False) 
            http.fetch(request,callback=self.on_response)

            
    def userIDToChatProcessServer(self,userID):
        #return options.chatProcessServer
        #fix me
        """
        chatProcessServer =   ['http://127.0.0.1:7500/',
                               'http://127.0.0.1:7501/',
                               'http://127.0.0.1:7502/',
                               'http://127.0.0.1:7503/',
                               'http://127.0.0.1:7504/',
                               'http://127.0.0.1:7505/',
                               'http://127.0.0.1:7506/',
                               'http://127.0.0.1:7507/',
                               ]
        """
        chatProcessServer =   ['http://127.0.0.1:7500/']
        index = random.randint(0, chatProcessServer.__len__() - 1)
        server = chatProcessServer[index]
        return server

    

    
class ChatConnection(object):
    def __init__(self, stream, address):
        #logging.info('receive a new connection from %s', address)
        self.stream = stream
        self.address = address
        self.header = None
        self.stream.set_close_callback(self._onStreamClose)

        headerLength = TCPDataStream.getHeaderLength()
        self.stream.read_bytes(headerLength,self._onReadFirstHeader)
        
    def _onReadFirstHeader(self,data):
        #logging.info('onReadFirstHeader  from %s data:%s', self.address,data)
        self.header = TCPDataStream(data)
        if self.header.parseHeader():
            connectionManager.setConnection(self.header.getHeaderUserID(),self.stream)
            if self.header.getBodyLength() > 0:
                self.stream.read_bytes(self.header.getBodyLength(),self._onReadBody)
            else:
                tcpDispatch = TCPDispatch(self.header,None,self._onWriteCallBack,self._onStreamClose)
                tcpDispatch.dispatch()
                headerLength = TCPDataStream.getHeaderLength()
                self.stream.read_bytes(headerLength,self._onReadHeader)
        else:
            logging.error('onReadFirstHeader stream close from %s',self.address)
            self.stream.close()

    def _onReadHeader(self, data):
        #logging.info('onReadHeader from %s data:%s', self.address,data)
        self.header = TCPDataStream(data)        
        if self.header.parseHeader():
            if self.header.getBodyLength() > 0:
                self.stream.read_bytes(self.header.getBodyLength(),self._onReadBody)
            else:
                if self.header.getHeaderCmd() != aConst.tcpCmd['login']:
                    tcpDispatch = TCPDispatch(self.header,None,self._onWriteCallBack,self._onStreamClose)
                    tcpDispatch.dispatch()
                headerLength = TCPDataStream.getHeaderLength()
                self.stream.read_bytes(headerLength,self._onReadHeader)
        else:
            logging.error('onReadHeader stream close from %s',self.address)
            self.stream.close()
            
            
    def _onReadBody(self,data):
        #logging.info('onReadBody from %s data length:%s', self.address,str(data.__len__()))
        headerLength = TCPDataStream.getHeaderLength()
        self.stream.read_bytes(headerLength,self._onReadHeader)

        tcpDispatch = TCPDispatch(self.header,data,self._onWriteCallBack,self._onStreamClose)
        tcpDispatch.dispatch()

        
    def _onWriteCallBack(self,data):
        if not self.stream.closed():
            self.stream.write(data)
                
    def _onStreamClose(self):
        #logging.info('onClose() %s', self.address)
        if self.header == None:
            return

        if self.header.getHeaderUserID() != None:
            tcpDispatch = TCPDispatch()
            tcpDispatch.sendHttpOffline(self.header.getHeaderUserID())
            connectionManager.closeUserConnection(self.header.getHeaderUserID())
        else:
            userID = connectionManager.closeRequestConnection(self.stream)
            if userID:
                tcpDispatch = TCPDispatch(None,None,self._onWriteCallBack,self._onStreamClose)
                tcpDispatch.sendHttpOffline(userID)
            else:
                logging.error('onClose error on userID')


class messageForwardHandler(RequestHandler):
    def post(self):        
        value = self.get_argument('value', '')
        #logging.info('messageForwardHandler messageForward : '+value)
        value = json.loads(value)        
        toUserID = value['toUserID']
        userID = value['userID']
        conn = connectionManager.getConnection(toUserID)
        if conn == None:
            logging.error('messageForwardHandle connectionManager')
            di = {"result":aConst.returnType["requestFail"]}    
            self.write(json.dumps(di))
            return
        di = {"result":aConst.returnType["requestSuccess"]}    
        self.write(json.dumps(di))

        body = {}
        body['seq'] = value['seq']
        body['messageType'] = value['messageType']
        body['createTime'] = value['createTime']
        body['messageType2'] = value['messageType2']
        body['content'] = value['content']
        #logging.info('messageForwardHandle content.__len__() == '+str(value['content'].__len__()))
        body = json.dumps(body)
        body = struct.pack(str(body.__len__())+'s',body)
        header = TCPDataStream(userID,toUserID,body.__len__(),body.__len__(),aConst.tcpCmd['messageForward'],1)
        if header.packHeader():
            _header = header.getHeaderData()
            _header = _header + body
            conn.write(_header)
        
          

                
class messageReadForwardHandler(RequestHandler):
    def post(self):        
        value = self.get_argument('value', '')
        #logging.info('messageReadForwardHandler messageForward : '+value)
        value = json.loads(value)        
        toUserID = value['toUserID']
        userID = value['userID']
        conn = connectionManager.getConnection(toUserID)
        if conn == None:
            logging.error('messageReadForwardHandler connectionManager')
            di = {"result":aConst.returnType["requestFail"]}    
            self.write(json.dumps(di))
            return
        di = {"result":aConst.returnType["requestSuccess"]}    
        self.write(json.dumps(di))

        body = {'status':aConst.messageStatus['read']}
        body['seq'] = value['seq']
        body = json.dumps(body)
        body = struct.pack(str(body.__len__())+'s',body)
        header = TCPDataStream(userID,toUserID,body.__len__(),body.__len__(),aConst.tcpCmd['messageReply'],1)
        if header.packHeader():
            _header = header.getHeaderData()
            _header = _header + body
            conn.write(_header)



                
        
                
class MainHandler(RequestHandler):
    def get(self):
        self.write('connectionManager:')
        connections = connectionManager.getAllConnection()
        for key in connections.keys():
            self.write(str(key) + '\n')
    def post(self):
        connections = connectionManager.getAllConnection()
        for key in connections.keys():
            self.write(str(key) + '\n')


class WebServer(Application):
    def __init__(self):
        handlers = [
            (r"/", MainHandler),
            (r"/messageForward",messageForwardHandler),
            (r"/messageReadForward",messageReadForwardHandler),
        ]
        settings = dict(
            debug = True,
            gzip = True,
        )
        Application.__init__(self, handlers, **settings)


class ChatServer(TCPServer):
    def __init__(self, io_loop=None, ssl_options=None, **kwargs):
        TCPServer.__init__(self, io_loop=io_loop, ssl_options=ssl_options, **kwargs)

    def handle_stream(self, stream, address):
        ChatConnection(stream, address)


define("tcpPort", default=8888,help="",type=int)
define("responseHttpPort",default=8889,help="",type=int)
define("responseHttpServer",default="http://127.0.0.1",help="",type=str)




def main():
    options.parse_command_line()
    AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
    chatServer = ChatServer()
    chatServer.listen(options.tcpPort)    
    webServer = WebServer()
    webServer.listen(options.responseHttpPort)
    IOLoop.instance().start()


 
if __name__ == '__main__':
    main()
    """
    import urllib2
    data = {'cmd':'login'}
    data = urllib.urlencode(data)
    response = urllib2.urlopen('http://127.0.0.1:6100/', data,timeout=10)
    print response
    """
