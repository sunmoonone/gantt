'''
Created on May 27, 2015

@author: sunmoonone
'''
import web
import os
from common import utils
from common.http import RequestHandler

from settings import settings

class MediaController(RequestHandler):
    auth_access_map=('**','anon')

    def GET(self, *args):
        ext = args[0]
        
        if(ext == "js"):
            web.header("Content-Type", "application/javascript")
        if(ext == "html"):
            web.header("Content-Type", "text/html")
        elif(ext == "css"):
            web.header("Content-Type", "text/css")
        elif(ext == "png"):
            web.header("Content-Type", "image/png")
        elif(ext == "jpg"):
            web.header("Content-Type", "image/jpg")
        elif(ext == "gif"):
            web.header("Content-Type", "image/gif")
        elif(ext == "svg"):
            web.header("Content-Type", "image/svg+xml")
        elif(ext == "woff"):
            web.header("Content-Type", "application/x-font-woff")
        elif(ext == "woff2"):
            web.header("Content-Type", "application/x-font-woff")
        elif(ext == "ttf"):
            web.header("Content-Type", "font/ttf")
        elif(ext == "otf"):
            web.header("Content-Type", "font/opentype")
        elif(ext == "eot"):
            web.header("Content-Type", "application/vnd.ms-fontobject")

#         return web.NotFound('No longer serve static files')
        return utils.file_get_contents(os.path.abspath(settings.web.static_dir) + web.ctx.path)
        
        