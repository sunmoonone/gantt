'''
Created on 2016年10月16日

@author: sunmoonone
'''

class Task(object):
    pass
    
    
#     CREATE TABLE `gantt_tasks` (
#   `id` int(11) NOT NULL AUTO_INCREMENT,
#   `text` varchar(255) NOT NULL,
#   `start_date` datetime NOT NULL,
#   `duration` int(11) NOT NULL DEFAULT 0,
#   `progress` float NOT NULL DEFAULT 0,
#   `sortorder` int(11) NOT NULL DEFAULT 0,
#   `parent` int(11) NOT NULL,
#   PRIMARY KEY (`id`)
# );

class Link(object):
    pass
    
#     CREATE TABLE `gantt_links` (
#   `id` int(11) NOT NULL AUTO_INCREMENT,
#   `source` int(11) NOT NULL,
#   `target` int(11) NOT NULL,
#   `type` varchar(1) NOT NULL,
#   PRIMARY KEY (`id`)
# );