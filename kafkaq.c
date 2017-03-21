
#define KXVER 3

#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/socket.h>
#include <librdkafka/rdkafka.h>  /* for Kafka driver */
#include "k.h"

static int run = 1;
static int consumerinit = 0;
static int producerinit = 0;
static rd_kafka_t *rkproducer, *rkconsumer;
static int fd[2];
const int MAX_MSG_SIZE = 1000000;

typedef struct subscription
{
  char topic[255];
  int partition;
} Subscription;

K initconsumer(K brokers, K options) {
  rd_kafka_conf_t *conf;
  char errstr[512];
  if(consumerinit) {
    krr("Already initialised - cleanup first");
    return (K)0;
  }
  if(brokers->t != -11) { krr("type"); return (K)0;}
  conf = rd_kafka_conf_new();
  if (!(rkconsumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr)))) {
   krr(errstr);
   return (K)0; 
  }
	if (rd_kafka_brokers_add(rkconsumer, brokers->s) == 0) {
    krr("No valid brokers specified");
    return (K)0;
  }
  consumerinit = 1;
  return (K)0;
}

K initproducer(K brokers, K options) {
  rd_kafka_conf_t *conf;
  char errstr[512];
  if(producerinit) {
    krr("Already initialised - cleanup first");
    return (K)0;
  }
  if(brokers->t != -11) { krr("type"); return (K)0;}
  conf = rd_kafka_conf_new();
  if (!(rkproducer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)))) {
   krr(errstr);
   return (K)0; 
  }
	if (rd_kafka_brokers_add(rkproducer, brokers->s) == 0) {
    krr("No valid brokers specified");
    return (K)0;
  }
  producerinit = 1;
  return (K)0;
 }

K cleanupconsumer() {
  if(!consumerinit) {
    krr("Not yet initialised - init first");
    return (K)0;
  }
  rd_kafka_destroy(rkconsumer);
  consumerinit = 0;
  run = 0;
  return (K)0;
 }

K cleanupproducer() {
  if(!producerinit) {
    krr("Not yet initialised - init first");
    return (K)0;
  }
  rd_kafka_destroy(rkproducer);
  producerinit = 0;
  return (K)0;
 }


static void msg_consume (rd_kafka_message_t *rkmessage,
			 void *opaque) {
	if (rkmessage->err) {
		if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
			return;
		}

		fprintf(stderr, "%% Consume error for topic \"%s\" [%"PRId32"] "
		       "otfset %"PRId64": %s\n",
		       rd_kafka_topic_name(rkmessage->rkt),
		       rkmessage->partition,
		       rkmessage->offset,
		       rd_kafka_message_errstr(rkmessage));

                if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
                    rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
                        run = 0;
		return;
	}
 
  if( (0>write(fd[0], &rkmessage->key_len, sizeof(size_t))) ||
      (0>write(fd[0], rkmessage->key, rkmessage->key_len)) ||
      (0>write(fd[0], &rkmessage->len, sizeof(size_t))) ||
      (0>write(fd[0], rkmessage->payload, rkmessage->len)) ) {
    fprintf(stderr, "Error writing message to socket");
    run = 0; 
  }
}


void* subscribe_thread (void *sub) {
	rd_kafka_topic_t *rkt;
	rd_kafka_topic_conf_t *topic_conf;
	int64_t start_offset = 0;
  Subscription *subinfo = (Subscription *)sub;
  int partition = subinfo->partition;

	topic_conf = rd_kafka_topic_conf_new();

  rkt = rd_kafka_topic_new(rkconsumer, subinfo->topic, topic_conf);
  topic_conf = NULL; /* Now owned by topic */

	if (rd_kafka_consume_start(rkt, partition, start_offset) == -1){
	  rd_kafka_resp_err_t err = rd_kafka_last_error();
    krr((S)rd_kafka_err2str(err));
    return (K)0;
  }
 
  free(subinfo);

  while(run) {
			rd_kafka_message_t *rkmessage;
      rd_kafka_poll(rkconsumer, 0);
			rkmessage = rd_kafka_consume(rkt, partition, 1000);
			if (!rkmessage) continue;
			msg_consume(rkmessage, NULL);
			rd_kafka_message_destroy(rkmessage);
		}

  rd_kafka_consume_stop(rkt, partition);
  while (rd_kafka_outq_len(rkconsumer) > 0) rd_kafka_poll(rkconsumer, 10);
  rd_kafka_topic_destroy(rkt);
  if (topic_conf) rd_kafka_topic_conf_destroy(topic_conf);
  return (K)0; 
 }

K callback(I d) {
  char keybuf[512]; 
  size_t keylength, length;
  K key, msg;
  key = ks("");
  msg = ktn(KG, 0);
  recv(d,&keylength,sizeof(size_t),0);
  if(keylength>0){
    recv(d,&keybuf,keylength,0);
    key = ks(sn(keybuf,keylength));
  }
  recv(d,&length,sizeof(size_t),0);
  if(length>0){
    msg = ktn(KG, length);
    recv(d,kG(msg),length,0);
  }
  K rtn = k(0, "kupd", key, msg);
  if(-128 == rtn->t) {};
  return (K)0;
 }

K subscribe(K topic, K partition) {
  pthread_t tid;
  if((topic->t != -11) || (partition->t != -7)) {krr("type"); return (K)0;};
  Subscription *sub = malloc(sizeof(Subscription));
  strcpy(sub->topic, topic->s);
  sub->partition = partition->j;
  run = 1;
  socketpair(AF_LOCAL,SOCK_STREAM,0,fd); 
  sd1(fd[1],callback);
  pthread_create(&tid, NULL, &subscribe_thread, (void*)sub);
  return (K)0;
 }

K unsubscribe() {
  run = 0;
  return (K)0;
 }

K publish(K topic, K partition, K key, K msg) {
	rd_kafka_topic_t *rkt;
	rd_kafka_topic_conf_t *topic_conf;
  if((topic->t != -11) || (partition->t != -7) || (key->t != -11) || (msg->t != 4)) {
    krr("type");
    return (K)0;
  }
	topic_conf = rd_kafka_topic_conf_new();
  rkt = rd_kafka_topic_new(rkproducer, topic->s, topic_conf);
  rd_kafka_produce(rkt, 0, RD_KAFKA_MSG_F_COPY, kG(msg), msg->n, NULL, 0, NULL);
  return (K)0;
 }


