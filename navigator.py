import os
import datetime
import pytz
from praw.models import MoreComments
from praw.models.comment_forest import CommentForest
import sys
from prawcore.exceptions import NotFound

from copy import copy, deepcopy
import json

import writer
import praw_object_data as pod

def check_id(comment, parent_id):
    if comment is not None and parent_id is not None:
        return comment.parent_id in [parent_id, 't1_' + parent_id]
    else:
        return True

class Navigator(object):
    """the navigator class will parse a Reddit thread according to the options given"""
    @pod.retry_if_broken_connection
    def __init__(self, thread, opts):
        self.thread = thread
        self.opts = opts
        self.comment_id_set = set()
        self.authors = set()
        if opts.grabauthors:
            if self.thread.author is not None:
                self.authors.add(self.thread.author)
        #accumulators
        self.traversed_comments = 0
        self.deleted_comments = 0
        self.infinite_loop_counter = 0
        self.child_counter = [0 for _ in opts.pattern]
        only_deleted_comments = False
        #general
        if not opts.skip_comments:
            self.comments = thread.comments.list()
            #remove non-root elements of comments
            idx = 0
            while idx < len(self.comments) and idx < self.opts.pattern[0]:
                for i in range(3):
                    if isinstance(self.comments[idx], MoreComments):
                        new_comments = self.expand_if_forest(self.comments.pop(idx).comments(),
                                                             halt_early=True)
                        self.comments = inject(self.comments, new_comments, idx)
                    if idx == len(self.comments):
                        break
                if idx == len(self.comments):
                    break
                if isinstance(self.comments[idx], MoreComments):
                    print 'failed to expand MoreComments after 3 attempts... removing node'
                    self.comments.pop(idx)
                    continue
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
            only_deleted_comments = len(self.comment_tree[0])==0
            if only_deleted_comments:
                print 'this thread only has deleted comments'
                self.deleted_comments = self.thread.num_comments
                self.direction = 'E'
                self.is_active = False
        self.start_time = datetime.datetime.now(pytz.utc)
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
        if self.thread.num_comments==0 or only_deleted_comments or opts.skip_comments:
            self.direction='E'
            self.move_one()
            self.is_active=False
        else:
            self.direction = 'D'
        print 'initialized navigator'
        sys.stdout.flush()

    @pod.retry_if_broken_connection
    def navigate(self):
        """function that should be called once object is created"""
        #check if there are any comments
        #begin main sequence

        while self.is_active:
            if self.direction in ['D','S']:
                self.get_comment_data()
            if self.opts.verbose:
                sys.stdout.write('\r')
                sys.stdout.write('COMMENT TREE POSITION: ' + str( self.position))
            #print self.get_comment_branch()
            sys.stdout.flush()
            self.move_one()
        sys.stdout.write('\n')
        sys.stdout.flush()
        print 'Done navigating thread'
        self.store_all_data()

    def get_comment_branch(self, shift=0):
        return self.comment_tree[self.current_level + shift]

    def get_comment(self, pop=False, shift=0):
        if not pop:
            try:
                if self.current_level==0 and shift < 0:
                    print 'attempting to shift too far back'
                return self.get_comment_branch(shift=shift)[self.get_level_position(shift=shift)]
            except AssertionError:
                print sys.exc_info()
                raise AssertionError
        else:
            if self.current_level==0 and shift < 0:
                print 'attempting to shift too far back (with pop)'
            return self.get_comment_branch(shift=shift).pop(self.get_level_position(shift=shift))

    def get_level_position(self, shift=0):
        return self.position[self.current_level + shift]

    def assign_comment_branch(self, val, level_offset=0):
        self.comment_tree[self.current_level + level_offset] = val

    def expand_if_forest(self, replies, valid_parent_id=None, halt_early=False):
        if isinstance(replies, MoreComments):
            replies = replies.comments().list()
        if isinstance(replies, CommentForest):
            val = replies.list()
        else:
            val =  replies
        if halt_early:
            return val
        #expand morecomments
        idx = 0

        while idx < len(val) and valid_parent_id:
            if valid_parent_id:
                #print idx, val
                if not check_id(val[idx], valid_parent_id):
                    val.pop(idx)
                else:
                    if isinstance(val[idx], MoreComments):
                        morecomments = val.pop(idx).comments()
                        if isinstance(morecomments, CommentForest):
                            morecomments = morecomments.list()
                        val = inject(val,
                                     [v for v in morecomments\
                                      if check_id(v, valid_parent_id)],
                                      idx)
                    else:
                        idx+=1
            else:
                print 'stuck in loop...'
        if valid_parent_id:
            retval = [v for v in val if check_id(v, valid_parent_id)]
            return retval
        else:
            return val
    
    def move_one(self, force_up=False):
        """movement of a single space and direction-changing"""
        if self.direction in ['D','S']:
            if self.can_move_down() and not force_up:
                self.child_counter[self.current_level] += 1
                valid_id = self.get_comment().id
                self.comment_tree[self.current_level + 1]\
                    = self.expand_if_forest(self.get_comment().replies,
                                            valid_parent_id=valid_id)
                self.current_level += 1
                self.position[self.current_level] = 0
                self.direction = 'D'
            elif self.can_move_sideways() and not force_up:
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
            if self.can_move_sideways() and not force_up:
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
        if self.direction not in ['U','E'] and not force_up:
            self.check_morecomments()

    def assign_child_counter(self):
        #print self.position, self.child_counter[self.current_level]
        comment = self.get_comment()
        comment_id = comment.id
        if comment_id==u'_':
            #print self.position
            self.check_morecomments()
        else:
            self.data['comments'][comment_id].update({'nreplies':
                                                      self.child_counter[self.current_level]})
        #reset
        self.child_counter[self.current_level] = 0

    def check_morecomments(self):
        if isinstance(self.get_comment_branch(), CommentForest):
            self.assign_comment_branch(self.get_comment_branch().list())
        retval=False
        for i in range(3):
            if isinstance(self.get_comment(), MoreComments):
                comment = self.get_comment(pop=True)
                if self.current_level <> 0:
                    valid_id = self.get_comment(shift=-1).id
                else:
                    #should never happen
                    valid_id = None
                self.assign_comment_branch(inject(self.get_comment_branch(),
                                                  self.expand_if_forest(comment.comments(),
                                                                        valid_parent_id=valid_id),
                                                  self.get_level_position()))
                retval=True
        if isinstance(self.get_comment(), MoreComments):
            print 'still MoreComments after 3 attempts'
            self.direction = 'U'
            retval=True
        return retval
                
    def can_move_up(self):
        return self.current_level <> 0
    
    def can_move_sideways(self):
        if self.get_level_position() > len(self.get_comment_branch()) - 1:
            print 'already went off comment branch...'
            return False
        if self.get_level_position() == len(self.get_comment_branch()) - 1:
            return False
        if self.get_level_position() == self.opts.pattern[self.current_level]:
            return False
        elif self.get_level_position() > self.opts.pattern[self.current_level]:
            print 'went too far sideways'
            return False
        #check if next is MoreComments and check if it's blank
        next_comment = self.get_comment_branch()[self.get_level_position() + 1]
        if isinstance(next_comment, MoreComments):
            #print self.get_comment_branch()
            #do not actually modify comment tree since this is just a check, so deepcopy() is used
            new_comments = self.expand_if_forest(deepcopy(next_comment.comments()),
                                                 valid_parent_id=self.get_comment().parent_id)
            if isinstance(new_comments, CommentForest):
                #print '(side) replies is actually a forest...correcting'
                new_comments = new_comments.list()
            if len(new_comments) == 0:
                #print 'EMPTY MoreComments DETECTED going sideways!'
                if self.get_comment().id not in self.comment_id_set:
                    self.deleted_comments += 1
                return False
            else:
                pass
            #print 'new comments are not empty, apprently'
            #print new_comments   
            
        return True
    
    def can_move_down(self):
        if self.current_level == len(self.opts.pattern) - 1:
            return False
        elif self.current_level > len(self.opts.pattern) - 1:
            print 'went too far down'
            return False
        #check if lower reply is blank MoreComments
        valid_id = self.get_comment().id
        next_comment = self.expand_if_forest(self.get_comment().replies.list(),
                                             valid_parent_id = valid_id)
        if len(next_comment) == 0:
            return False
        if isinstance(next_comment[0], MoreComments):
            new_comments = next_comment[0].comments()
            if isinstance(new_comments, CommentForest):
                #print '(down) replies is actually a forest...correcting'
                new_comments = self.expand_if_forest(new_comments.list(),
                                                     valid_parent_id=valid_id)
            if len(new_comments) == 0:
                if self.get_comment().id not in self.comment_id_set:
                    self.deleted_comments += 1
                #print 'EMPTY MoreComments DETECTED going downward!'
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
        skip_counter = 0
        if not self.opts.nouser:
            n_authors = len(self.authors)
            print 'scraping %d authors' % n_authors
            skip_counter = 0
            for i, author in enumerate(self.authors):
                if i > 0 and i % 1 == 0:
                    sys.stdout.write( 'processed %d/%d authors...\r' % (i, n_authors))
                    sys.stdout.flush()
                try:
                    author_id = author.id
                    if not self.opts.db.check_user_update_time(author_id, self.opts):
                        skip_counter += 1
                        continue
                    #print 'processing author: %s' % author.name
                    previous_time = pod.localize(self.opts.db.get_user_update_time(author_id))
                    if previous_time is None:
                        self.author_data[author.id] = pod.get_user_data(author, self.opts)
                    elif float((datetime.now(pytz.utc) - previous_time).\
                               seconds)/(3600.*24) > self.opts.user_delay:
                        self.author_data[author.id] = pod.get_user_data(author, self.opts)
                except NotFound:
                    #will store data in proper format for shadowbanned users
                    self.author_data['%%' + str(author.name)] = pod.get_user_data(author, self.opts)
                except AttributeError:
                    #for suspended users, the Redditor object has no ID, though a user can be found
                    #print 'user %s has been suspended' %author.name
                    self.author_data['%%' + str(author.name)] = pod.get_user_data(author, self.opts)
            sys.stdout.write( 'processed %d/%d authors...\r' % (n_authors, n_authors))
            sys.stdout.flush()
        #write thread data
        print ''
        print 'skipped %s authors from delays' % skip_counter
        print 'writing thread data'
        self.data['thread'][self.thread.id].update({'comments_deleted':self.deleted_comments,
                                                    'comments_navigated':self.traversed_comments})
        writer.write_thread(self.data['thread'][self.thread.id], self.opts)
        #write thread comment data
        #print self.data['comments']
        for key, value in self.data['comments'].iteritems():
            writer.write_comment(value, self.opts)
        #write user data, as well as post/comment data for that
        i = 0
        for key, value in self.author_data.iteritems():
            userdata = value['userdata']
            #print 'writing data for user: %s' % userdata['username']
            writer.write_user(userdata, self.opts)
            comment_data_dict = value['commentdata']
            thread_data_dict = value['threaddata']
            for ckey, cvalue in comment_data_dict.iteritems():
                writer.write_comment(cvalue, self.opts)
            for tkey, tvalue in thread_data_dict.iteritems():
                writer.write_thread(tvalue, self.opts)
                if self.opts.deepuser:
                    pass#self.opts.ids.append(tkey)#not original intention
            i+=1
            sys.stdout.write('wrote %d/%d authors to database\r' % (i, n_authors))
            sys.stdout.flush()
        self.opts.db.commit()
        print 'STORED ALL DATA FOR THREAD ID %s' % self.thread.id

    def get_thread_info(self):
        """assigns general data to self.data['thread']"""
        self.data['thread'].update(pod.get_thread_data(self.thread, self.opts,
                                                       mode='thread'))

    def get_comment_data(self):
        """adds data from a comment to self.data['comments']"""
        comment = self.get_comment()
        #print self.position
        if isinstance(comment, MoreComments):
            self.direction = 'U'
            self.move_one()
            return
        current_author = comment.author
        comment_id = comment.id
        if current_author is not None:
            self.authors.add(current_author)
        elif comment_id not in self.comment_id_set:
            self.deleted_comments += 1
        if comment_id not in self.comment_id_set:
            self.traversed_comments += 1
            self.comment_id_set.add(comment_id)
            data = pod.get_comment_data(comment, self.opts, 'thread')
            data[comment.id].update({'absolute_position':copy(self.position),
                                     'thread_begin_timestamp':self.start_time})
            self.data['comments'].update(data)

        else:
            pass
            #print 'already got comment data for comment id %s' % comment_id
            #print 'position: %s' % str(self.position)
        
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
    user_delay = -1
    thread_delay = -1
