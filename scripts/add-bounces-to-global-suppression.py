import redis
import sys

bounce_rules = {
    'bad-mailbox': 'hard',
    'invalid-mailbox': 'hard',
    'bad-domain': 'hard',
    'quota-issues': 'hard',
    'inactive-mailbox': 'hard',
    'bad-connection': 'hard',
    'spam-related': 'spam',
    'policy-related': 'spam',
    'message-expired': 'spam',
    'routing-errors': 'soft',
    'no-answer-from-host': 'soft',
    'content-related': 'spam',
    'protocol-errors': 'soft',
    'relaying-issues': 'soft'
}

r = redis.Redis(host='69.4.85.186',password='Gj5RVpE6ktJJNvE', db=1)
stats = dict(member=0,non_member=0)
setname = 'global_suppression'

total = 0
for line in sys.stdin:
    total += 1
    email, reason = line.rstrip().split("\t")
    
    if reason in bounce_rules and bounce_rules[reason] == 'hard':
        if r.sismember("global_suppression",email):
            stats['member'] += 1
        else:
            stats['non_member'] += 1
            print(f"Adding {email} to global suppression")
            r.sadd(setname, email);

    if total % 1000 == 0:
        print(stats)

print(stats)
