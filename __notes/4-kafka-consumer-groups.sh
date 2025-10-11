# list all consumer groups
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list --state

: <<'COMMENT'
┌──────────────────────────────────────────────shivp436@shivp346 …/apache-kafka-udemy/__notes on master!?
└─❯ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list --state
GROUP                       STATE
first-application           Empty
COMMENT

# describe all groups
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups --state

: <<'COMMENT'
┌──────────────────────────────────────────────shivp436@shivp346 …/apache-kafka-udemy/__notes on master!?
└─❯ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups --state

Consumer group 'first-application' has no active members.

GROUP             COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                #MEMBERS
first-application 127.0.0.1:9092  (1)       -                    Empty                0
COMMENT

# describe a group
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group first-application

: <<'COMMENT'
┌──────────────────────────────────────────────shivp436@shivp346 …/apache-kafka-udemy/__notes on master!?
└─❯ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group first-application

Consumer group 'first-application' has no active members.

GROUP             TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
first-application third-topic     0          3               3               0               -               -               -
first-application third-topic     2          3               3               0               -               -               -
first-application third-topic     1          3               3               0               -               -               -
first-application third-topic     4          4               4               0               -               -               -
first-application third-topic     3          3               3               0               -               -               -
COMMENT

# resetting offset
# options: --to-datetime, --by-period, --to-earliest, --to-latest, --shift-by, --from-file, --to-current
# can use: --dry-run to only visualize what will happen after execution
# --execute is needed to actually execute the action
# all consumers must be stopped to reset the group
# --topic third-topic:0,third-topic:2 => to reset only certain partitions of a topic

# resetting to earliest
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group first-application \
  --reset-offsets --to-earliest \
  --execute --topic third-topic

: <<'COMMENT'
┌──────────────────────────────────────────────shivp436@shivp346 …/apache-kafka-udemy/__notes on master!?
└─❯ kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group first-application \
  --reset-offsets --to-earliest \
  --execute --topic third-topic

GROUP                          TOPIC                          PARTITION  NEW-OFFSET
first-application              third-topic                    0          0
first-application              third-topic                    2          0
first-application              third-topic                    1          0
first-application              third-topic                    4          0
first-application              third-topic                    3          0
COMMENT
# all offsets are set to 0
# now new consumer from this group will read all messages from beginning

# consumer
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic third-topic \
  --from-beginning \
  --property print.partition=true \
  --property print.key=true \
  --property print.value=true \
  --property print.timestamp=true \
  --property print.offset=true \
  --property print.headers=true \
  --property key.separator=' | ' \
  --group first-application

: <<'COMMENT'
┌──────────────────────────────────────────────shivp436@shivp346 …/apache-kafka-udemy/__notes on master!?
└─❯ kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic third-topic \
  --from-beginning \
  --property print.partition=true \
  --property print.key=true \
  --property print.value=true \
  --property print.timestamp=true \
  --property print.offset=true \
  --property print.headers=true \
  --property key.separator=' | ' \
  --group first-application

CreateTime:1760207666586 | Partition:0 | Offset:0 | NO_HEADERS | 4 | AGAIN
CreateTime:1760207719899 | Partition:0 | Offset:1 | NO_HEADERS | 9 | HELLO
CreateTime:1760208661588 | Partition:0 | Offset:2 | NO_HEADERS | 35 | MAN
CreateTime:1760207688718 | Partition:2 | Offset:0 | NO_HEADERS | 1 | REPEAT
CreateTime:1760207751318 | Partition:2 | Offset:1 | NO_HEADERS | 10 | BOSS
CreateTime:1760208686397 | Partition:2 | Offset:2 | NO_HEADERS | 36 | SDd
CreateTime:1760207703497 | Partition:1 | Offset:0 | NO_HEADERS | 5 | WHYY
CreateTime:1760207768191 | Partition:1 | Offset:1 | NO_HEADERS | 11 | MAN
CreateTime:1760208690089 | Partition:1 | Offset:2 | NO_HEADERS | 37 |
CreateTime:1760207384991 | Partition:4 | Offset:0 | NO_HEADERS | 1 | HELLO
CreateTime:1760207709087 | Partition:4 | Offset:1 | NO_HEADERS | 6 | HELLO
CreateTime:1760207773268 | Partition:4 | Offset:2 | NO_HEADERS | 12 | WOMAN
CreateTime:1760208697920 | Partition:4 | Offset:3 | NO_HEADERS |  |
CreateTime:1760207548379 | Partition:3 | Offset:0 | NO_HEADERS | 2 | HI
CreateTime:1760207713775 | Partition:3 | Offset:1 | NO_HEADERS | 7 | WHYY
CreateTime:1760208650462 | Partition:3 | Offset:2 | NO_HEADERS | 34 | MANNNNN
COMMENT
# can see that it consumed all 16 messages from-beginning
# lag will be set to 0 again now

# Reset offset: shift-by (+ve to move forward, -ve to move backward)
# let's shift back by 2 messages
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group first-application \
  --reset-offsets --shift-by -2 \
  --execute --topic third-topic

: <<'COMMENT'
┌──────────────────────────────────────────────shivp436@shivp346 …/apache-kafka-udemy/__notes on master!?
└─❯ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group first-application --reset-offsets --shift-by -2 --execute --topic third-topic

GROUP                          TOPIC                          PARTITION  NEW-OFFSET
first-application              third-topic                    0          1
first-application              third-topic                    2          1
first-application              third-topic                    1          1
first-application              third-topic                    4          2
first-application              third-topic                    3          1
COMMENT
# offset it set back by 2 messages

: <<'COMMENT'
┌──────────────────────────────────────────────shivp436@shivp346 …/apache-kafka-udemy/__notes on master!?
└─❯ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group first-application

Consumer group 'first-application' has no active members.

GROUP             TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
first-application third-topic     0          1               3               2               -               -               -
first-application third-topic     2          1               3               2               -               -               -
first-application third-topic     1          1               3               2               -               -               -
first-application third-topic     4          2               4               2               -               -               -
first-application third-topic     3          1               3               2               -               -               -
COMMENT
# Lag is 2 for each partition now

# now if we consume, we should get only last 2 messages from each partition (2*5 = 10)
: <<'COMMENT'
┌──────────────────────────────────────────────shivp436@shivp346 …/apache-kafka-udemy/__notes on master!?
└─❯ kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic third-topic \
  --from-beginning \
  --property print.partition=true \
  --property print.key=true \
  --property print.value=true \
  --property print.timestamp=true \
  --property print.offset=true \
  --property print.headers=true \
  --property key.separator=' | ' \
  --group first-application

CreateTime:1760207719899 | Partition:0 | Offset:1 | NO_HEADERS | 9 | HELLO
CreateTime:1760208661588 | Partition:0 | Offset:2 | NO_HEADERS | 35 | MAN
CreateTime:1760207751318 | Partition:2 | Offset:1 | NO_HEADERS | 10 | BOSS
CreateTime:1760208686397 | Partition:2 | Offset:2 | NO_HEADERS | 36 | SDd
CreateTime:1760207768191 | Partition:1 | Offset:1 | NO_HEADERS | 11 | MAN
CreateTime:1760208690089 | Partition:1 | Offset:2 | NO_HEADERS | 37 |
CreateTime:1760207773268 | Partition:4 | Offset:2 | NO_HEADERS | 12 | WOMAN
CreateTime:1760208697920 | Partition:4 | Offset:3 | NO_HEADERS |  |
CreateTime:1760207713775 | Partition:3 | Offset:1 | NO_HEADERS | 7 | WHYY
CreateTime:1760208650462 | Partition:3 | Offset:2 | NO_HEADERS | 34 | MANNNNN
COMMENT
# got only last 2 messages from each partition

# resetting only partition 4 to earliest
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group first-application \
  --reset-offsets --to-earliest \
  --execute --topic third-topic:4

: <<'COMMENT'
┌──────────────────────────────────────────────shivp436@shivp346 …/apache-kafka-udemy/__notes on master!?
└─❯ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group first-application

Consumer group 'first-application' has no active members.

GROUP             TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
first-application third-topic     0          3               3               0               -               -               -
first-application third-topic     2          3               3               0               -               -               -
first-application third-topic     1          3               3               0               -               -               -
first-application third-topic     4          4               4               0               -               -               -
first-application third-topic     3          3               3               0               -               -               -
┌──────────────────────────────────────────────shivp436@shivp346 …/apache-kafka-udemy/__notes on master!?
└─❯ kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group first-application \
  --reset-offsets --to-earliest \
  --execute --topic third-topic:4

GROUP                          TOPIC                          PARTITION  NEW-OFFSET
first-application              third-topic                    4          0
┌──────────────────────────────────────────────shivp436@shivp346 …/apache-kafka-udemy/__notes on master!?
└─❯ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group first-application

Consumer group 'first-application' has no active members.

GROUP             TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
first-application third-topic     0          3               3               0               -               -               -
first-application third-topic     2          3               3               0               -               -               -
first-application third-topic     1          3               3               0               -               -               -
first-application third-topic     4          0               4               4               -               -               -
first-application third-topic     3          3               3               0               -               -               -
COMMENT
# can see that lag before was 0 for all, then we reset-offsets for partition 4 to earliest, now lag is 4 for that

# now if we read, we should only from partition 4
: <<'COMMENT'
┌──────────────────────────────────────────────shivp436@shivp346 …/apache-kafka-udemy/__notes on master!?
└─❯ kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic third-topic \
  --from-beginning \
  --property print.partition=true \
  --property print.key=true \
  --property print.value=true \
  --property print.timestamp=true \
  --property print.offset=true \
  --property print.headers=true \
  --property key.separator=' | ' \
  --group first-application
CreateTime:1760207384991 | Partition:4 | Offset:0 | NO_HEADERS | 1 | HELLO
CreateTime:1760207709087 | Partition:4 | Offset:1 | NO_HEADERS | 6 | HELLO
CreateTime:1760207773268 | Partition:4 | Offset:2 | NO_HEADERS | 12 | WOMAN
CreateTime:1760208697920 | Partition:4 | Offset:3 | NO_HEADERS |  |
COMMENT

## DELETING A GROUP OR TOPIC OFFSET

# delete offset for a topic from a consumer group
# this will delete the complete offset and remove all commits - different from setting it to earliest(0) explicitly
# next consumer, will read from the earliest or latest, as per config, if --reset-offsets to-earliest it will read from 0 for sure
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --delete-offsets \
  --group first-application \
  --topic third-topic

: <<'COMMENT'
┌──────────────────────────────────────────────shivp436@shivp346 …/apache-kafka-udemy/__notes on master!?
└─❯ kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --delete-offsets \
  --group first-application \
  --topic third-topic
Request succeed for deleting offsets with topic third-topic group first-application

TOPIC                          PARTITION       STATUS
third-topic                    0               Successful
third-topic                    1               Successful
third-topic                    2               Successful
third-topic                    3               Successful
third-topic                    4               Successful
COMMENT
# this will delete the offset for a topic from a consumer group
# if consumer group has only 1 topic, it will delete the whole consumer group
: <<'COMMENT'
┌──────────────────────────────────────────────shivp436@shivp346 …/apache-kafka-udemy/__notes on master!?
└─❯ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list --state
GROUP                     STATE
COMMENT
# deleted the entire consumer group

# DELETE A CONSUMER GROUP WITH MULTIPLE TOPICS ASSOCIATED
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group first-application

: <<'COMMENT'
┌──────────────────────────────────────────────shivp436@shivp346 …/apache-kafka-udemy/__notes on master!?
└─❯ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group first-application
Deletion of requested consumer groups ('first-application') was successful.
COMMENT
