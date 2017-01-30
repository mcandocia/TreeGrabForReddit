import sys
import os
from argparse import ArgumentParser
import datetime
import re
from psycopg2.extensions import TransactionRollbackError

base_dir = os.path.abspath(
    os.path.join(
        os.path.join( 
            os.path.dirname( 
                os.path.abspath(__file__) 
            ),
            os.pardir
        ),
        os.pardir
    )
)

sys.path.insert(0, base_dir)

import db as pgdb
db = pgdb.Database('politics',{})

from multiprocessing import Pool

from nltk.corpus import stopwords

sw = stopwords.words('english')

#these words are to be used to 
stop_words = [w for w in sw]
stop_words.extend(['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t',
                   'u','v','w','x','y','z','1','2','3','4','5','6','7','8','9','0','',' ',
                   'http','https','com','reddit','np','org','net','gov','co','uk','imgur',
                   'img','youtube','www'])

swears = {
    'shit':'shit',
    'shits':'shit',
    'shitted':'shit',
    'shat':'shit',
    'shitting':'shit',
    'shittin':'shit',
    'shithead':'shit',
    'fuck':'fuck',
    'fucks':'fuck',
    'fucking':'fuck',
    'fucker':'fuck',
    'fucked':'fuck',
    'fuckin':'fuck',
    'fuckery':'fuck',
    'fuckwit':'fuck',
    'fuckwits':'fuck',
    'dumbfuck':'fuck',
    'dumbfucks':'fuck',
    'hell':'hell',
    'asshole':'asshole',
    'assholes':'asshole',
    'pussy':'pussy',
    'pussies':'pussy',
    'bitch':'bitch',
    'bitches':'bitch',
    'bitching':'bitch',
    'pissed':'piss',
    'piss':'piss',
    'pisses':'piss',
    'goddamn':'goddamn',
    'godamn':'goddamn',
    'cunt':'cunt',
    'cunts':'cunt',
    'motherfucker':'motherfucker',
    'retard':'retard',
    'retards':'retards',
    'retarded':'retarded',
    'whore':'whore',
    'whores':'whore',
    'whoring':'whore',
    'fag':'fag',
    'fags':'fag',
    'faggot':'fag',
    'faggots':'fag',
    'faggotry':'fag',
    'nigger':'nigger',
    'niggers':'nigger',
    'crap':'crap',
    'craps':'crap',
    'crapped':'crap',
    'crapping':'crap',
    'kike':'kike',
    'kikes':'kike',
    'kyke':'kike',
    'kykes':'kike',
    'spic':'spic',
    'spick':'spic',
    'spig':'spic',
    'spigotty':'spic',
    'chink':'chink',
    'chinks':'chink',
    'coon':'coon',
    'coons':'coon',
    'douchebag':'douchebag',
    'douchebags':'douchebag',
    'honky':'honky',
    'honkie':'honky',
    'honkey':'honky',
    'honkies':'honky',
    'injun':'injun',
    'injuns':'injun',
    'whitey':'whitey',
    'cocksucker':'cocksucker',
    'cocksuckers':'cocksucker',
    'twat':'twat',
    'twats':'twat',
}

#subreddits of interest

top_subreddits = [
    'TwoXChromosomes',
    'tifu',
    'gonewild',
    'UpliftingNews',
    'AskReddit',
    'The_Donald',
    'videos',
    'WritingPrompts',
    'nottheonion',
    'todayilearned',
    'worldnews',
    'creepy',
    'Jokes',
    'television',
    'movies',
    'sports',
    'news',
    'pics',
    'IAmA',
    'gaming',
    'gifs',
    'funny',
    'mildlyinteresting',
    'Showerthoughts',
    'Music',
    'OldSchoolCool',
    'aww',
    'Futurology',
    'space',
    'explainlikeimfive',
    'food',
    'announcements',
    'personalfinanace',
    'LifeProTips',
    'books',
    'Documentaries',
    'science',
    'nosleep',
    'history',
    'gadgets',
    'dataisbeautiful',
    'philosophy',
    'EarthPorn',
    'askscience',
    'GetMotivated',
    'listentothis',
    'DIY',
    'photoshopbattles',
    'Art',
    'InternetisBeautiful',
    'blog',
]

norm_subreddits = [
    'mildlyinteresting',
    'movies',
    'todayilearned',
    'videos',
    'pics',
    'gifs',
    'worldnews',
    'The_Donald',
    'aww',
    'Showerthoughts',
    'television',
    'gonewild',
    'Music',
    'tifu',
    'funny',
    'AskReddit',
    'Jokes',
    'gaming',
    'LifeProTips',
    'personalfinance',
    'IAmA',
    'news',
    'TwoXChromosomes',
    'Futurology',
    'explainlikeimfive',
    'nottheonion',
    'OldSchoolCool',
    'books',
    'food',
    'WritingPrompts',
    'sports',
    'space',
    'science',
    'dataisbeautiful',
    'Documentaries',
    'DIY',
    'creepy',
    'EarthPorn',
    'gadgets',
    'Art',
    'history',
    'UpliftingNews',
    'GetMotivated',
    'nosleep',
    'askscience',
    'photoshopbattles',
    'listentothis',
    'philosophy',
    'InternetisBeautiful',
    'announcements',
    'blog',
]

extended_bad_words = {
    "4r5e":1,
    "5h1t":1,
    "5hit":1,
    "a55":1,
    "anal":1,
    "anus":1,
    "ar5e":1,
    "arrse":1,
    "arse":1,
    "ass":1,
    "ass-fucker":1,
    "asses":1,
    "assfucker":1,
    "assfukka":1,
    "asshole":1,
    "assholes":1,
    "asswhole":1,
    "a_s_s":1,
    "b!tch":1,
    "b00bs":1,
    "b17ch":1,
    "b1tch":1,
    "ballbag":1,
    "balls":1,
    "ballsack":1,
    "bastard":1,
    "beastial":1,
    "beastiality":1,
    "bellend":1,
    "bestial":1,
    "bestiality":1,
    "bi+ch":1,
    "biatch":1,
    "bitch":1,
    "bitcher":1,
    "bitchers":1,
    "bitches":1,
    "bitchin":1,
    "bitching":1,
    "bloody":1,
    "blow job":1,
    "blowjob":1,
    "blowjobs":1,
    "boiolas":1,
    "bollock":1,
    "bollok":1,
    "boner":1,
    "boob":1,
    "boobs":1,
    "booobs":1,
    "boooobs":1,
    "booooobs":1,
    "booooooobs":1,
    "breasts":1,
    "buceta":1,
    "bugger":1,
    "bum":1,
    "bunny fucker":1,
    "butt":1,
    "butthole":1,
    "buttmuch":1,
    "buttplug":1,
    "c0ck":1,
    "c0cksucker":1,
    "carpet muncher":1,
    "cawk":1,
    "chink":1,
    "cipa":1,
    "cl1t":1,
    "clit":1,
    "clitoris":1,
    "clits":1,
    "cnut":1,
    "cock":1,
    "cock-sucker":1,
    "cockface":1,
    "cockhead":1,
    "cockmunch":1,
    "cockmuncher":1,
    "cocks":1,
    "cocksuck ":1,
    "cocksucked ":1,
    "cocksucker":1,
    "cocksucking":1,
    "cocksucks ":1,
    "cocksuka":1,
    "cocksukka":1,
    "cok":1,
    "cokmuncher":1,
    "coksucka":1,
    "coon":1,
    "cox":1,
    "crap":1,
    "cum":1,
    "cummer":1,
    "cumming":1,
    "cums":1,
    "cumshot":1,
    "cunilingus":1,
    "cunillingus":1,
    "cunnilingus":1,
    "cunt":1,
    "cuntlick ":1,
    "cuntlicker ":1,
    "cuntlicking ":1,
    "cunts":1,
    "cyalis":1,
    "cyberfuc":1,
    "cyberfuck ":1,
    "cyberfucked ":1,
    "cyberfucker":1,
    "cyberfuckers":1,
    "cyberfucking ":1,
    "d1ck":1,
    "damn":1,
    "dick":1,
    "dickhead":1,
    "dildo":1,
    "dildos":1,
    "dink":1,
    "dinks":1,
    "dirsa":1,
    "dlck":1,
    "dog-fucker":1,
    "doggin":1,
    "dogging":1,
    "donkeyribber":1,
    "doosh":1,
    "duche":1,
    "dyke":1,
    "ejaculate":1,
    "ejaculated":1,
    "ejaculates ":1,
    "ejaculating ":1,
    "ejaculatings":1,
    "ejaculation":1,
    "ejakulate":1,
    "f u c k":1,
    "f u c k e r":1,
    "f4nny":1,
    "fag":1,
    "fagging":1,
    "faggitt":1,
    "faggot":1,
    "faggs":1,
    "fagot":1,
    "fagots":1,
    "fags":1,
    "fanny":1,
    "fannyflaps":1,
    "fannyfucker":1,
    "fanyy":1,
    "fatass":1,
    "fcuk":1,
    "fcuker":1,
    "fcuking":1,
    "feck":1,
    "fecker":1,
    "felching":1,
    "fellate":1,
    "fellatio":1,
    "fingerfuck ":1,
    "fingerfucked ":1,
    "fingerfucker ":1,
    "fingerfuckers":1,
    "fingerfucking ":1,
    "fingerfucks ":1,
    "fistfuck":1,
    "fistfucked ":1,
    "fistfucker ":1,
    "fistfuckers ":1,
    "fistfucking ":1,
    "fistfuckings ":1,
    "fistfucks ":1,
    "flange":1,
    "fook":1,
    "fooker":1,
    "fuck":1,
    "fucka":1,
    "fucked":1,
    "fucker":1,
    "fuckers":1,
    "fuckhead":1,
    "fuckheads":1,
    "fuckin":1,
    "fucking":1,
    "fuckings":1,
    "fuckingshitmotherfucker":1,
    "fuckme ":1,
    "fucks":1,
    "fuckwhit":1,
    "fuckwit":1,
    "fudge packer":1,
    "fudgepacker":1,
    "fuk":1,
    "fuker":1,
    "fukker":1,
    "fukkin":1,
    "fuks":1,
    "fukwhit":1,
    "fukwit":1,
    "fux":1,
    "fux0r":1,
    "f_u_c_k":1,
    "gangbang":1,
    "gangbanged ":1,
    "gangbangs ":1,
    "gaylord":1,
    "gaysex":1,
    "goatse":1,
    "God":1,
    "god-dam":1,
    "god-damned":1,
    "goddamn":1,
    "goddamned":1,
    "hardcoresex ":1,
    "hell":1,
    "heshe":1,
    "hoar":1,
    "hoare":1,
    "hoer":1,
    "homo":1,
    "hore":1,
    "horniest":1,
    "horny":1,
    "hotsex":1,
    "jack-off ":1,
    "jackoff":1,
    "jap":1,
    "jerk-off ":1,
    "jism":1,
    "jiz ":1,
    "jizm ":1,
    "jizz":1,
    "kawk":1,
    "knob":1,
    "knobead":1,
    "knobed":1,
    "knobend":1,
    "knobhead":1,
    "knobjocky":1,
    "knobjokey":1,
    "kock":1,
    "kondum":1,
    "kondums":1,
    "kum":1,
    "kummer":1,
    "kumming":1,
    "kums":1,
    "kunilingus":1,
    "l3i+ch":1,
    "l3itch":1,
    "labia":1,
    "lmfao":1,
    "lust":1,
    "lusting":1,
    "m0f0":1,
    "m0fo":1,
    "m45terbate":1,
    "ma5terb8":1,
    "ma5terbate":1,
    "masochist":1,
    "master-bate":1,
    "masterb8":1,
    "masterbat*":1,
    "masterbat3":1,
    "masterbate":1,
    "masterbation":1,
    "masterbations":1,
    "masturbate":1,
    "mo-fo":1,
    "mof0":1,
    "mofo":1,
    "mothafuck":1,
    "mothafucka":1,
    "mothafuckas":1,
    "mothafuckaz":1,
    "mothafucked ":1,
    "mothafucker":1,
    "mothafuckers":1,
    "mothafuckin":1,
    "mothafucking ":1,
    "mothafuckings":1,
    "mothafucks":1,
    "mother fucker":1,
    "motherfuck":1,
    "motherfucked":1,
    "motherfucker":1,
    "motherfuckers":1,
    "motherfuckin":1,
    "motherfucking":1,
    "motherfuckings":1,
    "motherfuckka":1,
    "motherfucks":1,
    "muff":1,
    "mutha":1,
    "muthafecker":1,
    "muthafuckker":1,
    "muther":1,
    "mutherfucker":1,
    "n1gga":1,
    "n1gger":1,
    "nazi":1,
    "nigg3r":1,
    "nigg4h":1,
    "nigga":1,
    "niggah":1,
    "niggas":1,
    "niggaz":1,
    "nigger":1,
    "niggers ":1,
    "nob":1,
    "nob jokey":1,
    "nobhead":1,
    "nobjocky":1,
    "nobjokey":1,
    "numbnuts":1,
    "nutsack":1,
    "orgasim ":1,
    "orgasims ":1,
    "orgasm":1,
    "orgasms ":1,
    "p0rn":1,
    "pawn":1,
    "pecker":1,
    "penis":1,
    "penisfucker":1,
    "phonesex":1,
    "phuck":1,
    "phuk":1,
    "phuked":1,
    "phuking":1,
    "phukked":1,
    "phukking":1,
    "phuks":1,
    "phuq":1,
    "pigfucker":1,
    "pimpis":1,
    "piss":1,
    "pissed":1,
    "pisser":1,
    "pissers":1,
    "pisses ":1,
    "pissflaps":1,
    "pissin ":1,
    "pissing":1,
    "pissoff ":1,
    "poop":1,
    "porn":1,
    "porno":1,
    "pornography":1,
    "pornos":1,
    "prick":1,
    "pricks ":1,
    "pron":1,
    "pube":1,
    "pusse":1,
    "pussi":1,
    "pussies":1,
    "pussy":1,
    "pussys ":1,
    "rectum":1,
    "retard":1,
    "rimjaw":1,
    "rimming":1,
    "s hit":1,
    "s.o.b.":1,
    "sadist":1,
    "schlong":1,
    "screwing":1,
    "scroat":1,
    "scrote":1,
    "scrotum":1,
    "semen":1,
    "sex":1,
    "sh!+":1,
    "sh!t":1,
    "sh1t":1,
    "shag":1,
    "shagger":1,
    "shaggin":1,
    "shagging":1,
    "shemale":1,
    "shi+":1,
    "shit":1,
    "shitdick":1,
    "shite":1,
    "shited":1,
    "shitey":1,
    "shitfuck":1,
    "shitfull":1,
    "shithead":1,
    "shiting":1,
    "shitings":1,
    "shits":1,
    "shitted":1,
    "shitter":1,
    "shitters ":1,
    "shitting":1,
    "shittings":1,
    "shitty ":1,
    "skank":1,
    "slut":1,
    "sluts":1,
    "smegmap":1,
    "smut":1,
    "snatch":1,
    "son-of-a-bitch":1,
    "spac":1,
    "spunk":1,
    "s_h_i_t":1,
    "t1tt1e5":1,
    "t1tties":1,
    "teets":1,
    "teez":1,
    "testical":1,
    "testicle":1,
    "tit":1,
    "titfuck":1,
    "tits":1,
    "titt":1,
    "tittie5":1,
    "tittiefucker":1,
    "titties":1,
    "tittyfuck":1,
    "tittywank":1,
    "titwank":1,
    "tosser":1,
    "turd":1,
    "tw4t":1,
    "twat":1,
    "twathead":1,
    "twatty":1,
    "twunt":1,
    "twunter":1,
    "v14gra":1,
    "v1gra":1,
    "vagina":1,
    "viagra":1,
    "vulva":1,
    "w00se":1,
    "wang":1,
    "wank":1,
    "wanker":1,
    "wanky":1,
    "whoar":1,
    "whore":1,
    "willies":1,
    "willy":1,
    "xrated":1,
    "xxx":1
}

default_subreddits = [
    "announcements",
    "Art",
    "AskReddit",
    "askscience",
    "aww",
    "blog",
    "books",
    "creepy",
    "dataisbeautiful",
    "DIY",
    "Documentaries",
    "EarthPorn",
    "explainlikeimfive",
    "food",
    "funny",
    "Futurology",
    "gadgets",
    "gaming",
    "GetMotivated",
    "gifs",
    "history",
    "IAmA",
    "InternetIsBeautiful",
    "Jokes",
    "LifeProTips",
    "listentothis",
    "mildlyinteresting",
    "movies",
    "Music",
    "news",
    "nosleep",
    "nottheonion",
    "OldSchoolCool",
    "personalfinance",
    "philosophy",
    "photoshopbattles",
    "pics",
    "science",
    "Showerthoughts",
    "space",
    "sports",
    "television",
    "tifu",
    "todayilearned",
    "TwoXChromosomes",
    "UpliftingNews",
    "videos",
    "worldnews",
    "WritingPrompts",
]

def word_tabname(x):
    return 't_' + re.sub('[^0-9a-zA-Z]','',x)

suffix = '_defaults'
def main():
    mindate = datetime.datetime(2016,10,1)
    #create table to select data from
    db.execute("""CREATE TABLE IF NOT EXISTS swear_comment_source%s AS 
    (SELECT id, subreddit, text, 
    false AS has_swear,
    false AS has_specific_swear 
    FROM defaults.comments 
    WHERE created>=%%s
    AND author_name != 'AutoModerator'
    AND subreddit=ANY(%%s)
    );""" % suffix, (mindate, top_subreddits))
    print 'selected comments'
    db.execute("""CREATE TABLE IF NOT EXISTS swear_comment_words%s AS
    (SELECT id,
    subreddit,
    regexp_split_to_table(lower(text), '[. ''",.:\s?]+') AS word
    FROM swear_comment_source%s);""" % (suffix,suffix))
    print 'parsed words'
    db.execute("""DELETE FROM swear_comment_words%s
    WHERE word=ANY(%%s)""" % suffix, (stop_words,))
    print 'removed stopwords'
    #relative will be initially unfilled, later updated
    db.execute("""CREATE TABLE IF NOT EXISTS swears_general%s(
    subreddit VARCHAR(30),
    word VARCHAR(30),
    count BIGINT,
    relative FLOAT);""" % suffix)
    db.execute("""CREATE TABLE IF NOT EXISTS swears_specific%s(
    subreddit VARCHAR(30),
    word VARCHAR(30),
    count BIGINT,
    relative FLOAT);""" % suffix)
    db.commit()
    print 'intialized word-subreddit tables'
    #replace with multiprocessing
    print 'creating pool of processes'
    p = Pool(10)
    print 'doing general swear pool'
    for word in p.imap_unordered(general_swear, extended_bad_words.keys()):
        print word
    db.commit()
    
    print 'doing actual swear pool'
    for word in p.imap_unordered(specific_swear, swears.keys()):
        print word
    db.commit()

    #
    #now normalize
    db.execute("""UPDATE swears_general%s
    SET relative = count::float/t1.cnt::float FROM
    (SELECT subreddit, count(*) as cnt 
    FROM swear_comment_source%s
    GROUP BY subreddit) t1
    WHERE swears_general%s.subreddit = t1.subreddit;""" % (suffix, suffix, suffix))
    print 'added relative freq to general table'
    db.execute("""UPDATE swears_specific%s
    SET relative = count::float/t1.cnt::float FROM
    (SELECT subreddit, count(*) as cnt 
    FROM swear_comment_source%s
    GROUP BY subreddit) t1
    WHERE swears_specific%s.subreddit = t1.subreddit;""" % (suffix, suffix, suffix))
    print 'added relative freq to specific table'
    db.commit()
    #summaries
    #db
    db.execute("""CREATE TABLE IF NOT EXISTS swears_general_summary%s AS
    (SELECT subreddit, count(CASE WHEN has_swear THEN 1 ELSE NULL END) AS total_swears, 
    0.00 AS percentage_swears
    FROM swear_comment_source%s
    GROUP BY subreddit)""" % (suffix, suffix))
    db.execute("""UPDATE swears_general_summary%s
    SET percentage_swears=total_swears::float/t1.cnt FROM (
    SELECT subreddit, count(*) as cnt
    FROM swear_comment_source%s
    GROUP BY subreddit) t1
    WHERE swears_general_summary%s.subreddit = t1.subreddit""" % (suffix, suffix, suffix))

    db.execute("""CREATE TABLE IF NOT EXISTS swears_specific_summary%s AS
    (SELECT subreddit, count(CASE WHEN has_specific_swear THEN 1 ELSE NULL END) AS total_swears, 
    0.00 AS percentage_swears
    FROM swear_comment_source%s
    GROUP BY subreddit)""" % (suffix, suffix))
    db.execute("""UPDATE swears_specific_summary%s
    SET percentage_swears=total_swears::float/t1.cnt FROM (
    SELECT subreddit, count(*) as cnt
    FROM swear_comment_source%s
    GROUP BY subreddit) t1
    WHERE swears_specific_summary%s.subreddit = t1.subreddit""" % (suffix, suffix, suffix))
    db.commit()

def general_swear(swear):
    db = pgdb.Database('null_',{}, silence=True)
    while True:
        try:
            db.execute("""DROP TABLE IF EXISTS swear_%s;""" % word_tabname(swear))
            db.execute("""CREATE TABLE swear_%s AS (
            SELECT DISTINCT id, subreddit
            FROM swear_comment_words%s 
            WHERE word=%%s);""" % (word_tabname(swear),suffix), [swear])

            db.execute("""INSERT INTO swears_general%s
            SELECT subreddit, %%s AS swear_word, 
            count(*) as swear_count
            FROM swear_%s
            GROUP BY subreddit;""" % (suffix, word_tabname(swear)), [swear])
            db.execute("""UPDATE swear_comment_source%s
            SET has_swear=true
            WHERE not has_swear AND 
            id IN (SELECT id FROM swear_%s)""" % (suffix, word_tabname(swear)))
            db.execute("""DROP TABLE swear_%s;""" % word_tabname(swear))
            db.commit()
            break
        except TransactionRollbackError:
            print 'deadlock error with %s, retrying' % swear
            db.conn.rollback()
    return swear


def specific_swear(swear):
    db = pgdb.Database('null_',{}, silence=True)
    while True:
        try:
            db.execute("""DROP TABLE IF EXISTS swear_%s;""" % word_tabname(swear))            
            db.execute("""CREATE TABLE swear_%s AS (
            SELECT DISTINCT id, subreddit
            FROM swear_comment_words%s 
            WHERE word=%%s);""" % (word_tabname(swear), suffix), [swear])
            db.execute("""INSERT INTO swears_specific%s
            SELECT subreddit, %%s AS swear_word, 
            count(*) as swear_count
            FROM swear_%s
            GROUP BY subreddit;""" % (suffix, word_tabname(swear)), [swear])
            db.execute("""UPDATE swear_comment_source%s
            SET has_specific_swear=true
            WHERE not has_specific_swear AND 
            id IN (SELECT id FROM swear_%s)""" % (suffix, word_tabname(swear)))
            db.execute("""DROP TABLE swear_%s;""" % word_tabname(swear))
            db.commit()
            break
        except TransactionRollbackError:
            print 'deadlock issue with %s, retrying' % swear
            db.conn.rollback()
    return swear


if __name__=='__main__':
    main()
