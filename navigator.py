import os
import datetime
from praw.models import MoreComments
from praw.models.comment_forest import CommentForest

from copy import copy
import json

import writer
import praw_object_data as pod

class Navigator(object):
    """the navigator class will parse a Reddit thread according to the options given"""
    @pod.retry_if_broken_connection
    def __init__(self, thread, opts):
        self.thread = thread
        self.opts = opts
        self.authors = set()
        if opts.grabauthors:
            if self.thread.author is not None:
                self.authors.add(self.thread.author)
        #accumulators
        self.traversed_comments = 0
        self.deleted_comments = 0
        self.infinite_loop_counter = 0
        self.child_counter = [0 for _ in opts.pattern]
        #general
        if not opts.skip_comments:
            self.comments = thread.comments.list()
            #remove non-root elements of comments
            idx = 0
            while idx < len(self.comments):
                if isinstance(self.comments[idx], MoreComments):
                    new_comments = self.expand_if_forest(self.comments.pop(idx).comments())
                    self.comments = inject(self.comments, new_comments, idx)
                if not self.comments[idx].is_root:
                    try:
                        self.comments.pop(idx)
                    except:
                        #print self.comments
                        print idx
                        raise
                else:
                    idx += 1
            self.comment_tree = [[] for _ in opts.pattern]
            self.comment_tree[0] = self.comments
        self.start_time = datetime.datetime.now()
        self.data = {'thread':{},'comments':[]}
        self.position = [-1 for _ in opts.pattern]
        self.position[0] = 0
        self.current_level = 0
        self.data = {'thread':{}, 'comments':{}}
        #direction
        #'B' = begin (then D|S|E)
        #'D' = downwards (then D|S|U|E)
        #'U' = upwards (then S|U|E)
        #'S' = side (then U|E)
        #'E' = end
        self.direction = 'B'
        self.get_thread_info()
        self.is_active = True
        if self.thread.num_comments==0:
            self.direction='E'
            self.move_one()
            self.is_active=False
        else:
            self.direction = 'D'

    @pod.retry_if_broken_connection
    def navigate(self):
        """function that should be called once object is created"""
        #check if there are any comments
        #begin main sequence

        while self.is_active:
            if self.direction in ['D','S']:
                self.get_comment_data()
            self.move_one()
        self.store_all_data()

    def get_comment_branch(self):
        return self.comment_tree[self.current_level]

    def get_comment(self, pop=False):
        if not pop:
            return self.get_comment_branch()[self.get_level_position()]
        else:
            #print self.get_comment_branch()
            #print self.get_comment()
            #print self.position
            #print self.direction
            return self.get_comment_branch().pop(self.get_level_position())

    def get_level_position(self):
        return self.position[self.current_level]

    def assign_comment_branch(self, val, level_offset=0):
        self.comment_tree[self.current_level + level_offset] = val

    def expand_if_forest(self, replies):
        if isinstance(replies, MoreComments):
            replies = replies.comments().list()
        if isinstance(replies, CommentForest):
            return replies.list()
        else:
            return replies
    
    def move_one(self):
        """movement of a single space and direction-changing"""
        if self.direction in ['D','S']:
            if self.can_move_down():
                self.child_counter[self.current_level] += 1
                self.comment_tree[self.current_level + 1]\
                    = self.expand_if_forest(self.get_comment().replies)
                self.current_level += 1
                self.position[self.current_level] = 0
                self.direction = 'D'
            elif self.can_move_sideways():
                if self.current_level <> 0:
                    self.child_counter[self.current_level - 1] += 1
                self.assign_child_counter()
                self.position[self.current_level] += 1
                self.direction = 'S'
            elif self.can_move_up():
                self.assign_child_counter()
                self.assign_comment_branch([])
                self.position[self.current_level] = -1
                self.current_level -=1
                self.direction = 'U'
            else:
                self.direction = 'E'
                self.is_active = False
                self.assign_child_counter()
        elif self.direction == 'U':
            self.assign_child_counter()
            if self.can_move_sideways():
                self.position[self.current_level] +=1
                if self.current_level <> 0:
                    self.child_counter[self.current_level - 1] += 1
                self.direction = 'S'
            elif self.can_move_up():
                self.position[self.current_level] = -1
                self.assign_comment_branch([])
                self.current_level -= 1
            else:
                self.direction = 'E'
                self.is_active = False
        if self.direction == 'E':
            #self.assign_child_counter()
            self.is_active = False
        if self.direction not in ['U','E']:
            self.check_morecomments()

    def assign_child_counter(self):
        #print self.position, self.child_counter[self.current_level]
        comment = self.get_comment()
        comment_id = comment.id
        if comment_id==u'_':
            print self.position
            self.check_morecomments()
        else:
            self.data['comments'][comment_id].update({'nreplies':
                                                      self.child_counter[self.current_level]})
        #reset
        self.child_counter[self.current_level] = 0

    def check_morecomments(self):
        if isinstance(self.get_comment_branch(), CommentForest):
            #print 'deforesting at an odd time'
            self.assign_comment_branch(self.get_comment_branch().list())
            #print type(self.get_comment_branch())
        if isinstance(self.get_comment(), MoreComments):
            comment = self.get_comment(pop=True)
            self.assign_comment_branch(inject(self.get_comment_branch(),
                                              self.expand_if_forest(comment.comments()),
                                              self.get_level_position()))
            return True
        return False
                
    def can_move_up(self):
        return self.current_level <> 0
    
    def can_move_sideways(self):
        if self.get_level_position() == len(self.get_comment_branch()) - 1:
            return False
        if self.get_level_position() == self.opts.pattern[self.current_level]:
            return False
        elif self.get_level_position() > self.opts.pattern[self.current_level]:
            print 'went too far sideways'
            return False
        return True
    
    def can_move_down(self):
        if self.current_level == len(self.opts.pattern) - 1:
            return False
        elif self.current_level > len(self.opts.pattern) - 1:
            print 'went too far down'
            return False
        if len(self.get_comment().replies) == 0:
            return False
        return True
    
    def move_all(self):
        """for testing; outputs comments and extra data while traversing whole tree"""
        while self.is_active:
            if self.direction <> 'U':
                comment = self.get_comment()
                self.get_comment_data()
                print comment.body, self.position, self.direction
                print self.child_counter
            self.move_one()
        print 'done with move_all()'

    def store_thread_data(self):
        """used if comment data is ignored"""
        writer.write_thread(self.data['thread'][self.thread.id], self.opts)

    def store_all_data(self):
        #get author data
        self.author_data = {}
        if not self.opts.nouser:
            for author in self.authors:
                author_id = author.id
                print 'processing author: %s' % author.name
                previous_time = self.opts.db.get_user_update_time(author_id)
                if previous_time is None:
                    self.author_data[author.id] = pod.get_user_data(author, self.opts)
                elif float((datetime.now() - previous_time).seconds)/(3600.*24) > opts.user_delay:
                    self.author_data[author.id] = pod.get_user_data(author, self.opts)
        #write thread data
        print 'writing thread data'
        self.data['thread'][self.thread.id].update({'comments_deleted':self.deleted_comments,
                                                    'comments_navigated':self.traversed_comments})
        writer.write_thread(self.data['thread'][self.thread.id], self.opts)
        #write thread comment data
        #print self.data['comments']
        for key, value in self.data['comments'].iteritems():
            writer.write_comment(value, self.opts)
        #write user data, as well as post/comment data for that
        for key, value in self.author_data.iteritems():
            userdata = value['userdata']
            print 'writing data for user: %s' % userdata['username']
            writer.write_user(userdata, self.opts)
            comment_data_dict = value['commentdata']
            thread_data_dict = value['threaddata']
            for ckey, cvalue in comment_data_dict.iteritems():
                writer.write_comment(cvalue, self.opts)
            for tkey, tvalue in thread_data_dict.iteritems():
                writer.write_thread(tvalue, self.opts)
                if self.opts.deepuser:
                    pass#self.opts.ids.append(tkey)#not original intention
        self.opts.db.commit()
        print 'STORED DATA FOR THREAD ID %s' % self.thread.id

    def get_thread_info(self):
        """assigns general data to self.data['thread']"""
        self.data['thread'].update(pod.get_thread_data(self.thread, self.opts,
                                                       mode='thread'))

    def get_comment_data(self):
        """adds data from a comment to self.data['comments']"""
        comment = self.get_comment()
        print self.position
        if isinstance(comment, MoreComments):
            self.direction = 'U'
            self.move_one()
            return
        current_author = comment.author
        comment_id = comment.id
        if current_author is not None:
            self.authors.add(current_author)
        else:
            self.deleted_comments += 1
        self.traversed_comments += 1
        data = pod.get_comment_data(comment, self.opts, 'thread')
        data[comment.id].update({'absolute_position':copy(self.position),
                     'thread_begin_timestamp':self.start_time})
        self.data['comments'].update(data)
        
def inject(target, source, position):
    if position == len(target):
        return target + source
    elif position == 0:
        return source + target
    else:
        return target[:position] + source + target[position:]
    
class testops(object):
    """this can be used for testing in order to properly run 
    the navigator class code"""
    pattern = [50,20,10,5,2,2,2,2,2,2,2]
    grabauthors = True
    nouser = False
    skip_comments = False
    user_limit = 100
    deepuser = False
